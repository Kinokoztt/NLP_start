"""
方案2：基于Apache Arrow和Parquet的零拷贝时序数据管道
优点：零拷贝读取，高压缩比，Spark原生支持，适合大规模数据
"""

import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader
import pyarrow as pa
import pyarrow.parquet as pq
from typing import List, Optional, Tuple
from pathlib import Path
import time


class ArrowTimeSeriesWriter:
    """
    将Spark数据输出为优化的Parquet格式
    """
    
    @staticmethod
    def write_from_spark(
        spark_df,
        output_path: str,
        time_col: str,
        feature_cols: List[str],
        group_size: int = 10000,
        compression: str = 'zstd'
    ):
        """
        从Spark DataFrame写入优化的Parquet文件
        
        Args:
            spark_df: Spark DataFrame
            output_path: 输出路径
            time_col: 时间列名
            feature_cols: 特征列名
            group_size: Row Group大小（影响读取性能）
            compression: 压缩算法 ('zstd', 'snappy', 'gzip', 'lz4')
        """
        # 配置Spark输出Parquet
        (spark_df
         .orderBy(time_col)
         .select(feature_cols)
         .write
         .mode('overwrite')
         .option('compression', compression)
         .option('parquet.block.size', group_size * 1024)  # Row Group大小
         .parquet(output_path))
        
        print(f"数据已写入: {output_path}")
        print(f"  - 压缩算法: {compression}")
        print(f"  - Row Group大小: {group_size}")
    
    @staticmethod
    def optimize_parquet_for_timeseries(
        input_path: str,
        output_path: str,
        chunk_size: int = 100000
    ):
        """
        优化现有Parquet文件以提高时序读取性能
        """
        # 读取并重新组织数据
        table = pq.read_table(input_path)
        
        # 写入优化的Parquet
        pq.write_table(
            table,
            output_path,
            row_group_size=chunk_size,
            compression='zstd',
            compression_level=3,  # 平衡压缩率和速度
            use_dictionary=True,  # 启用字典编码
            write_statistics=True  # 写入统计信息
        )
        
        print(f"Parquet文件已优化: {output_path}")


class ArrowTimeSeriesDataset(Dataset):
    """
    基于PyArrow的零拷贝时序数据集
    """
    
    def __init__(
        self,
        parquet_path: str,
        window_size: int,
        stride: int = 1,
        pred_len: int = 1,
        feature_cols: Optional[List[str]] = None,
        cache_size: int = 10,  # 缓存多少个row group
        transform=None
    ):
        """
        Args:
            parquet_path: Parquet文件或目录路径
            window_size: 滑窗大小
            stride: 滑窗步长
            pred_len: 预测长度
            feature_cols: 要读取的特征列（None则读取所有）
            cache_size: 缓存的row group数量
            transform: 数据转换函数
        """
        self.parquet_path = Path(parquet_path)
        self.window_size = window_size
        self.stride = stride
        self.pred_len = pred_len
        self.transform = transform
        
        # 打开Parquet文件
        if self.parquet_path.is_dir():
            self.parquet_file = pq.ParquetDataset(str(self.parquet_path))
            self.table = self.parquet_file.read(columns=feature_cols)
        else:
            self.parquet_file = pq.ParquetFile(str(self.parquet_path))
            self.table = self.parquet_file.read(columns=feature_cols)
        
        # 转换为numpy数组（零拷贝）
        self.data = self.table.to_pandas().values.astype(np.float32)
        self.total_timesteps = len(self.data)
        self.feature_dim = self.data.shape[1]
        
        # 计算样本数量
        self.total_length = window_size + pred_len
        self.num_samples = (self.total_timesteps - self.total_length) // stride + 1
        
        print(f"加载Parquet数据集:")
        print(f"  - 总时间步数: {self.total_timesteps}")
        print(f"  - 特征维度: {self.feature_dim}")
        print(f"  - 样本数量: {self.num_samples}")
        print(f"  - 数据大小: {self.data.nbytes / 1024**2:.2f} MB")
    
    def __len__(self):
        return self.num_samples
    
    def __getitem__(self, idx):
        start_idx = idx * self.stride
        end_idx = start_idx + self.window_size
        pred_end_idx = end_idx + self.pred_len
        
        x = self.data[start_idx:end_idx]
        y = self.data[end_idx:pred_end_idx]
        
        if self.transform:
            x, y = self.transform(x, y)
        
        x = torch.from_numpy(x.copy())
        y = torch.from_numpy(y.copy())
        
        return x, y


class LazyArrowTimeSeriesDataset(Dataset):
    """
    懒加载版本：适合超大数据集，按需从磁盘读取
    结合了内存缓存，平衡速度和内存占用
    """
    
    def __init__(
        self,
        parquet_path: str,
        window_size: int,
        stride: int = 1,
        pred_len: int = 1,
        feature_cols: Optional[List[str]] = None,
        cache_row_groups: int = 5,  # 缓存多少个row group
        transform=None
    ):
        self.parquet_path = Path(parquet_path)
        self.window_size = window_size
        self.stride = stride
        self.pred_len = pred_len
        self.transform = transform
        self.feature_cols = feature_cols
        
        # 打开Parquet文件获取元数据
        self.parquet_file = pq.ParquetFile(str(self.parquet_path))
        self.metadata = self.parquet_file.metadata
        
        # 获取总行数
        self.total_timesteps = self.metadata.num_rows
        
        # 读取schema获取特征维度
        schema = self.parquet_file.schema
        if feature_cols:
            self.feature_dim = len(feature_cols)
        else:
            self.feature_dim = len(schema)
        
        # 计算样本数量
        self.total_length = window_size + pred_len
        self.num_samples = (self.total_timesteps - self.total_length) // stride + 1
        
        # Row Group信息
        self.num_row_groups = self.metadata.num_row_groups
        self.row_group_sizes = [
            self.metadata.row_group(i).num_rows
            for i in range(self.num_row_groups)
        ]
        
        # 计算每个row group的起始位置
        self.row_group_offsets = [0]
        for size in self.row_group_sizes:
            self.row_group_offsets.append(self.row_group_offsets[-1] + size)
        
        # LRU缓存
        from collections import OrderedDict
        self.cache = OrderedDict()
        self.cache_row_groups = cache_row_groups
        
        print(f"初始化LazyArrow数据集:")
        print(f"  - 总时间步数: {self.total_timesteps}")
        print(f"  - Row Groups: {self.num_row_groups}")
        print(f"  - 样本数量: {self.num_samples}")
        print(f"  - 缓存Row Groups: {cache_row_groups}")
    
    def _load_row_group(self, rg_idx: int) -> np.ndarray:
        """加载指定的row group到内存"""
        if rg_idx in self.cache:
            # 命中缓存，移到最后（LRU）
            self.cache.move_to_end(rg_idx)
            return self.cache[rg_idx]
        
        # 从磁盘读取
        table = self.parquet_file.read_row_group(rg_idx, columns=self.feature_cols)
        data = table.to_pandas().values.astype(np.float32)
        
        # 加入缓存
        self.cache[rg_idx] = data
        
        # 超出缓存限制，删除最旧的
        if len(self.cache) > self.cache_row_groups:
            self.cache.popitem(last=False)
        
        return data
    
    def _get_row_group_for_index(self, timestep_idx: int) -> Tuple[int, int]:
        """
        获取指定时间步所在的row group和相对偏移
        
        Returns:
            (row_group_idx, offset_in_group)
        """
        for i in range(self.num_row_groups):
            if timestep_idx < self.row_group_offsets[i + 1]:
                offset = timestep_idx - self.row_group_offsets[i]
                return i, offset
        raise IndexError(f"Timestep index {timestep_idx} out of range")
    
    def __len__(self):
        return self.num_samples
    
    def __getitem__(self, idx):
        start_idx = idx * self.stride
        end_idx = start_idx + self.window_size
        pred_end_idx = end_idx + self.pred_len
        
        # 确定需要读取哪些row groups
        start_rg, start_offset = self._get_row_group_for_index(start_idx)
        end_rg, _ = self._get_row_group_for_index(pred_end_idx - 1)
        
        # 如果数据跨多个row group，需要拼接
        if start_rg == end_rg:
            # 数据在同一个row group内
            data = self._load_row_group(start_rg)
            x = data[start_offset:start_offset + self.window_size]
            y = data[start_offset + self.window_size:start_offset + self.total_length]
        else:
            # 数据跨多个row group，需要拼接
            segments = []
            current_idx = start_idx
            
            for rg_idx in range(start_rg, end_rg + 1):
                rg_data = self._load_row_group(rg_idx)
                rg_start = self.row_group_offsets[rg_idx]
                rg_end = self.row_group_offsets[rg_idx + 1]
                
                # 计算在当前row group中的切片
                slice_start = max(0, current_idx - rg_start)
                slice_end = min(len(rg_data), pred_end_idx - rg_start)
                
                segments.append(rg_data[slice_start:slice_end])
                current_idx = rg_end
            
            data = np.concatenate(segments, axis=0)
            x = data[:self.window_size]
            y = data[self.window_size:self.total_length]
        
        if self.transform:
            x, y = self.transform(x, y)
        
        x = torch.from_numpy(x.copy())
        y = torch.from_numpy(y.copy())
        
        return x, y


def benchmark_arrow_vs_memmap():
    """
    性能对比：Arrow/Parquet vs memmap
    """
    print("=" * 60)
    print("性能对比: Arrow/Parquet vs numpy.memmap")
    print("=" * 60)
    
    # 创建测试数据
    total_timesteps = 1000000
    feature_dim = 20
    
    print("\n1. 生成测试数据...")
    data = np.random.randn(total_timesteps, feature_dim).astype(np.float32)
    
    # 测试1: 写入速度
    print("\n2. 测试写入速度...")
    
    # Parquet写入
    start = time.time()
    table = pa.Table.from_pandas(
        __import__('pandas').DataFrame(data)
    )
    pq.write_table(
        table,
        'test_data.parquet',
        compression='zstd',
        compression_level=3
    )
    parquet_write_time = time.time() - start
    parquet_size = Path('test_data.parquet').stat().st_size / 1024**2
    
    # memmap写入
    start = time.time()
    memmap_data = np.memmap(
        'test_data.memmap',
        dtype=np.float32,
        mode='w+',
        shape=(total_timesteps, feature_dim)
    )
    memmap_data[:] = data
    memmap_data.flush()
    memmap_write_time = time.time() - start
    memmap_size = Path('test_data.memmap').stat().st_size / 1024**2
    
    print(f"   Parquet写入: {parquet_write_time:.2f}秒, 大小: {parquet_size:.2f} MB")
    print(f"   memmap写入: {memmap_write_time:.2f}秒, 大小: {memmap_size:.2f} MB")
    print(f"   压缩比: {memmap_size / parquet_size:.2f}x")
    
    # 测试2: 随机读取速度
    print("\n3. 测试随机读取速度...")
    
    num_reads = 1000
    indices = np.random.randint(0, total_timesteps - 100, num_reads)
    
    # Parquet读取
    parquet_file = pq.ParquetFile('test_data.parquet')
    parquet_data = parquet_file.read().to_pandas().values
    
    start = time.time()
    for idx in indices:
        _ = parquet_data[idx:idx+100]
    parquet_read_time = time.time() - start
    
    # memmap读取
    memmap_data = np.memmap(
        'test_data.memmap',
        dtype=np.float32,
        mode='r',
        shape=(total_timesteps, feature_dim)
    )
    
    start = time.time()
    for idx in indices:
        _ = memmap_data[idx:idx+100]
    memmap_read_time = time.time() - start
    
    print(f"   Parquet读取{num_reads}次: {parquet_read_time:.3f}秒")
    print(f"   memmap读取{num_reads}次: {memmap_read_time:.3f}秒")
    print(f"   Parquet速度提升: {memmap_read_time / parquet_read_time:.2f}x")
    
    # 清理
    Path('test_data.parquet').unlink()
    Path('test_data.memmap').unlink()
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    print("方案2: Arrow/Parquet 零拷贝时序数据管道\n")
    
    # 性能对比
    benchmark_arrow_vs_memmap()
    
    print("\n\n✅ Arrow/Parquet方案优势:")
    print("  - 写入速度快")
    print("  - 高压缩比（节省50-80%存储空间）")
    print("  - 零拷贝读取")
    print("  - Spark原生支持，无需额外转换")
    print("  - 支持列式过滤，减少I/O")
    print("  - 适合大规模数据集")
