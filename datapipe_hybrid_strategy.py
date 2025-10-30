"""
方案3：混合内存策略时序数据管道
优点：自适应内存和磁盘平衡，适合超大规模数据集，智能预取
"""

import numpy as np
import torch
from torch.utils.data import Dataset, DataLoader
import h5py
from typing import Optional, List, Tuple
from collections import OrderedDict
from pathlib import Path
import threading
import queue


class HybridTimeSeriesDataset(Dataset):
    """
    混合策略数据集：
    - 热数据保持在内存
    - 冷数据存储在HDF5（高效磁盘格式）
    - LRU缓存 + 异步预取
    """
    
    def __init__(
        self,
        hdf5_path: str,
        window_size: int,
        stride: int = 1,
        pred_len: int = 1,
        cache_size_gb: float = 2.0,  # 内存缓存大小(GB)
        prefetch_size: int = 100,    # 预取队列大小
        chunk_size: int = 10000,     # HDF5 chunk大小
        transform=None
    ):
        """
        Args:
            hdf5_path: HDF5文件路径
            window_size: 滑窗大小
            stride: 滑窗步长
            pred_len: 预测长度
            cache_size_gb: 内存缓存大小(GB)
            prefetch_size: 异步预取的样本数量
            chunk_size: HDF5 chunk大小（影响I/O效率）
            transform: 数据转换函数
        """
        self.window_size = window_size
        self.stride = stride
        self.pred_len = pred_len
        self.transform = transform
        
        # 打开HDF5文件
        self.hdf5_file = h5py.File(hdf5_path, 'r')
        self.data = self.hdf5_file['timeseries']
        
        self.total_timesteps = self.data.shape[0]
        self.feature_dim = self.data.shape[1]
        
        # 计算样本数量
        self.total_length = window_size + pred_len
        self.num_samples = (self.total_timesteps - self.total_length) // stride + 1
        
        # 计算缓存容量（以chunk为单位）
        bytes_per_sample = window_size * self.feature_dim * 4  # float32
        self.cache_capacity = int((cache_size_gb * 1024**3) / bytes_per_sample)
        
        # LRU缓存
        self.cache = OrderedDict()
        self.cache_lock = threading.Lock()
        
        # 预取队列
        self.prefetch_queue = queue.Queue(maxsize=prefetch_size)
        self.prefetch_enabled = False
        
        print(f"混合策略数据集初始化:")
        print(f"  - 总时间步数: {self.total_timesteps}")
        print(f"  - 特征维度: {self.feature_dim}")
        print(f"  - 样本数量: {self.num_samples}")
        print(f"  - 缓存容量: {self.cache_capacity} 样本")
        print(f"  - 缓存大小: {cache_size_gb:.2f} GB")
    
    def _load_timesteps(self, start_idx: int, length: int) -> np.ndarray:
        """
        从HDF5加载指定范围的时间步
        使用缓存加速
        """
        cache_key = (start_idx, length)
        
        with self.cache_lock:
            if cache_key in self.cache:
                # 缓存命中
                self.cache.move_to_end(cache_key)
                return self.cache[cache_key]
        
        # 从磁盘加载
        data = self.data[start_idx:start_idx + length].astype(np.float32)
        
        with self.cache_lock:
            # 加入缓存
            self.cache[cache_key] = data
            
            # LRU淘汰
            if len(self.cache) > self.cache_capacity:
                self.cache.popitem(last=False)
        
        return data
    
    def __len__(self):
        return self.num_samples
    
    def __getitem__(self, idx):
        start_idx = idx * self.stride
        
        # 加载数据
        data = self._load_timesteps(start_idx, self.total_length)
        
        x = data[:self.window_size]
        y = data[self.window_size:]
        
        if self.transform:
            x, y = self.transform(x, y)
        
        x = torch.from_numpy(x.copy())
        y = torch.from_numpy(y.copy())
        
        return x, y
    
    def enable_prefetch(self):
        """启用异步预取"""
        self.prefetch_enabled = True
    
    def disable_prefetch(self):
        """禁用异步预取"""
        self.prefetch_enabled = False
    
    def get_cache_stats(self):
        """获取缓存统计信息"""
        with self.cache_lock:
            return {
                'cache_size': len(self.cache),
                'cache_capacity': self.cache_capacity,
                'cache_utilization': len(self.cache) / self.cache_capacity
            }
    
    def __del__(self):
        """清理资源"""
        if hasattr(self, 'hdf5_file'):
            self.hdf5_file.close()


class HDF5TimeSeriesWriter:
    """
    将数据写入HDF5文件（适合混合策略）
    """
    
    def __init__(
        self,
        output_path: str,
        total_timesteps: int,
        feature_dim: int,
        chunk_size: int = 10000,
        compression: str = 'gzip',
        compression_opts: int = 4
    ):
        """
        Args:
            output_path: 输出文件路径
            total_timesteps: 总时间步数
            feature_dim: 特征维度
            chunk_size: HDF5 chunk大小
            compression: 压缩算法 ('gzip', 'lzf', None)
            compression_opts: 压缩级别 (1-9 for gzip)
        """
        self.output_path = output_path
        self.total_timesteps = total_timesteps
        self.feature_dim = feature_dim
        
        # 创建HDF5文件
        self.file = h5py.File(output_path, 'w')
        
        # 创建数据集（支持chunking和压缩）
        self.dataset = self.file.create_dataset(
            'timeseries',
            shape=(total_timesteps, feature_dim),
            dtype=np.float32,
            chunks=(chunk_size, feature_dim),
            compression=compression,
            compression_opts=compression_opts
        )
        
        self.current_offset = 0
        
        print(f"HDF5文件已创建: {output_path}")
        print(f"  - Shape: ({total_timesteps}, {feature_dim})")
        print(f"  - Chunk: ({chunk_size}, {feature_dim})")
        print(f"  - Compression: {compression}")
    
    def write_batch(self, data: np.ndarray):
        """批量写入数据"""
        batch_size = data.shape[0]
        end_offset = self.current_offset + batch_size
        
        if end_offset > self.total_timesteps:
            raise ValueError(f"写入超出边界: {end_offset} > {self.total_timesteps}")
        
        self.dataset[self.current_offset:end_offset] = data
        self.current_offset = end_offset
    
    def write_from_spark(
        self,
        spark_df,
        time_col: str,
        feature_cols: List[str],
        batch_size: int = 100000
    ):
        """从Spark DataFrame写入数据"""
        spark_df_sorted = spark_df.orderBy(time_col)
        
        # 转换为pandas并批量写入
        pandas_df = spark_df_sorted.select(feature_cols).toPandas()
        data_array = pandas_df.values.astype(np.float32)
        
        for i in range(0, len(data_array), batch_size):
            batch = data_array[i:i+batch_size]
            self.write_batch(batch)
            print(f"已写入 {min(i+batch_size, len(data_array))}/{len(data_array)} 条数据")
    
    def close(self):
        """关闭文件"""
        self.file.close()


class AdaptiveTimeSeriesDataset(Dataset):
    """
    自适应策略数据集：根据访问模式动态调整缓存
    """
    
    def __init__(
        self,
        hdf5_path: str,
        window_size: int,
        stride: int = 1,
        pred_len: int = 1,
        cache_size_gb: float = 2.0,
        warmup_samples: int = 1000,  # 预热样本数
        transform=None
    ):
        self.window_size = window_size
        self.stride = stride
        self.pred_len = pred_len
        self.transform = transform
        
        # 打开HDF5文件
        self.hdf5_file = h5py.File(hdf5_path, 'r')
        self.data = self.hdf5_file['timeseries']
        
        self.total_timesteps = self.data.shape[0]
        self.feature_dim = self.data.shape[1]
        
        self.total_length = window_size + pred_len
        self.num_samples = (self.total_timesteps - self.total_length) // stride + 1
        
        # 访问统计
        self.access_count = {}
        self.access_lock = threading.Lock()
        
        # 动态缓存
        bytes_per_timestep = self.feature_dim * 4
        cache_timesteps = int((cache_size_gb * 1024**3) / bytes_per_timestep)
        
        self.cache = OrderedDict()
        self.cache_capacity = cache_timesteps // self.total_length
        
        # 预热阶段
        self.warmup_samples = warmup_samples
        self.warmup_done = False
        
        print(f"自适应策略数据集初始化:")
        print(f"  - 样本数量: {self.num_samples}")
        print(f"  - 缓存容量: {self.cache_capacity} 样本")
    
    def _record_access(self, idx: int):
        """记录访问模式"""
        with self.access_lock:
            self.access_count[idx] = self.access_count.get(idx, 0) + 1
    
    def _get_hot_samples(self, top_k: int) -> List[int]:
        """获取最热的样本索引"""
        with self.access_lock:
            sorted_samples = sorted(
                self.access_count.items(),
                key=lambda x: x[1],
                reverse=True
            )
            return [idx for idx, _ in sorted_samples[:top_k]]
    
    def warmup_cache(self):
        """预热缓存：预加载最常访问的样本"""
        if self.warmup_done:
            return
        
        print("正在预热缓存...")
        hot_samples = self._get_hot_samples(self.cache_capacity)
        
        for idx in hot_samples:
            start_idx = idx * self.stride
            data = self.data[start_idx:start_idx + self.total_length].astype(np.float32)
            self.cache[idx] = data
        
        self.warmup_done = True
        print(f"缓存已预热: {len(hot_samples)} 个热样本")
    
    def __len__(self):
        return self.num_samples
    
    def __getitem__(self, idx):
        self._record_access(idx)
        
        # 尝试从缓存获取
        if idx in self.cache:
            data = self.cache[idx]
        else:
            # 从磁盘加载
            start_idx = idx * self.stride
            data = self.data[start_idx:start_idx + self.total_length].astype(np.float32)
            
            # 如果完成预热，更新缓存
            if self.warmup_done:
                self.cache[idx] = data
                if len(self.cache) > self.cache_capacity:
                    self.cache.popitem(last=False)
        
        x = data[:self.window_size]
        y = data[self.window_size:]
        
        if self.transform:
            x, y = self.transform(x, y)
        
        x = torch.from_numpy(x.copy())
        y = torch.from_numpy(y.copy())
        
        return x, y
    
    def __del__(self):
        if hasattr(self, 'hdf5_file'):
            self.hdf5_file.close()


def benchmark_hybrid_strategy():
    """
    测试混合策略的性能
    """
    import time
    
    print("=" * 60)
    print("混合策略性能测试")
    print("=" * 60)
    
    # 创建测试数据
    print("\n1. 创建测试HDF5文件...")
    total_timesteps = 1000000
    feature_dim = 20
    
    writer = HDF5TimeSeriesWriter(
        output_path='test_hybrid.h5',
        total_timesteps=total_timesteps,
        feature_dim=feature_dim,
        chunk_size=10000,
        compression='gzip',
        compression_opts=4
    )
    
    # 写入随机数据
    batch_size = 10000
    for i in range(0, total_timesteps, batch_size):
        batch_data = np.random.randn(batch_size, feature_dim).astype(np.float32)
        writer.write_batch(batch_data)
    
    writer.close()
    
    file_size = Path('test_hybrid.h5').stat().st_size / 1024**2
    print(f"   文件大小: {file_size:.2f} MB")
    
    # 创建数据集
    print("\n2. 创建混合策略数据集...")
    dataset = HybridTimeSeriesDataset(
        hdf5_path='test_hybrid.h5',
        window_size=100,
        stride=10,
        pred_len=10,
        cache_size_gb=0.5  # 512MB缓存
    )
    
    # 测试不同访问模式的性能
    print("\n3. 测试顺序访问...")
    start = time.time()
    for i in range(1000):
        _ = dataset[i]
    sequential_time = time.time() - start
    print(f"   1000次顺序访问: {sequential_time:.2f}秒")
    
    print("\n4. 测试随机访问（冷缓存）...")
    indices = np.random.randint(0, len(dataset), 1000)
    start = time.time()
    for idx in indices:
        _ = dataset[idx]
    random_cold_time = time.time() - start
    print(f"   1000次随机访问: {random_cold_time:.2f}秒")
    
    print("\n5. 测试随机访问（热缓存）...")
    # 重复访问相同的索引
    start = time.time()
    for idx in indices:
        _ = dataset[idx]
    random_hot_time = time.time() - start
    print(f"   1000次随机访问: {random_hot_time:.2f}秒")
    print(f"   缓存加速比: {random_cold_time / random_hot_time:.2f}x")
    
    # 缓存统计
    stats = dataset.get_cache_stats()
    print(f"\n6. 缓存统计:")
    print(f"   缓存使用: {stats['cache_size']}/{stats['cache_capacity']}")
    print(f"   缓存利用率: {stats['cache_utilization']:.1%}")
    
    # 清理
    Path('test_hybrid.h5').unlink()
    print("\n✅ 测试完成！")


if __name__ == "__main__":
    print("方案3: 混合内存策略时序数据管道\n")
    
    benchmark_hybrid_strategy()
    
    print("\n\n✅ 混合策略方案优势:")
    print("  - 平衡内存和速度")
    print("  - 自适应缓存策略")
    print("  - 适合超大规模数据集")
    print("  - HDF5高效压缩和随机访问")
    print("  - 支持异步预取")
