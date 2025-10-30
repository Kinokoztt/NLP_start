"""
完整的性能对比测试：对比所有方案的性能
"""

import numpy as np
import time
from pathlib import Path
import matplotlib
matplotlib.use('Agg')  # 非GUI后端
import matplotlib.pyplot as plt
import pandas as pd


def create_test_data(total_timesteps: int = 1000000, feature_dim: int = 20):
    """创建测试数据"""
    return np.random.randn(total_timesteps, feature_dim).astype(np.float32)


def benchmark_memmap(data: np.ndarray, num_reads: int = 1000):
    """基准测试：numpy memmap（当前方案）"""
    print("\n" + "="*60)
    print("测试方案0: numpy.memmap（当前方案）")
    print("="*60)
    
    filepath = 'test_memmap.dat'
    
    # 写入性能
    print("1. 写入性能...")
    start = time.time()
    mm = np.memmap(filepath, dtype=np.float32, mode='w+', shape=data.shape)
    mm[:] = data
    mm.flush()
    write_time = time.time() - start
    del mm
    
    file_size = Path(filepath).stat().st_size / 1024**2
    write_speed = file_size / write_time
    
    print(f"   写入时间: {write_time:.2f}秒")
    print(f"   写入速度: {write_speed:.2f} MB/s")
    print(f"   文件大小: {file_size:.2f} MB")
    
    # 读取性能
    print("2. 读取性能...")
    mm = np.memmap(filepath, dtype=np.float32, mode='r', shape=data.shape)
    
    indices = np.random.randint(0, len(data) - 100, num_reads)
    
    start = time.time()
    for idx in indices:
        _ = mm[idx:idx+100].copy()
    read_time = time.time() - start
    
    read_speed = num_reads / read_time
    
    print(f"   {num_reads}次随机读取: {read_time:.3f}秒")
    print(f"   读取速度: {read_speed:.1f} 样本/秒")
    
    # 清理
    del mm
    Path(filepath).unlink()
    
    return {
        'name': 'memmap',
        'write_time': write_time,
        'write_speed': write_speed,
        'read_time': read_time,
        'read_speed': read_speed,
        'file_size': file_size
    }


def benchmark_shared_memory(data: np.ndarray, num_reads: int = 1000):
    """测试方案1: 共享内存"""
    print("\n" + "="*60)
    print("测试方案1: 共享内存")
    print("="*60)
    
    from multiprocessing import shared_memory
    
    # 写入性能
    print("1. 写入性能...")
    start = time.time()
    
    shm = shared_memory.SharedMemory(
        name='test_shm',
        create=True,
        size=data.nbytes
    )
    shared_array = np.ndarray(data.shape, dtype=data.dtype, buffer=shm.buf)
    shared_array[:] = data
    
    write_time = time.time() - start
    write_speed = (data.nbytes / 1024**2) / write_time
    
    print(f"   写入时间: {write_time:.2f}秒")
    print(f"   写入速度: {write_speed:.2f} MB/s")
    
    # 读取性能
    print("2. 读取性能...")
    indices = np.random.randint(0, len(data) - 100, num_reads)
    
    start = time.time()
    for idx in indices:
        _ = shared_array[idx:idx+100].copy()
    read_time = time.time() - start
    
    read_speed = num_reads / read_time
    
    print(f"   {num_reads}次随机读取: {read_time:.3f}秒")
    print(f"   读取速度: {read_speed:.1f} 样本/秒")
    
    # 清理
    shm.close()
    shm.unlink()
    
    return {
        'name': 'shared_memory',
        'write_time': write_time,
        'write_speed': write_speed,
        'read_time': read_time,
        'read_speed': read_speed,
        'file_size': data.nbytes / 1024**2
    }


def benchmark_parquet(data: np.ndarray, num_reads: int = 1000):
    """测试方案2: Parquet"""
    print("\n" + "="*60)
    print("测试方案2: Arrow/Parquet")
    print("="*60)
    
    import pyarrow as pa
    import pyarrow.parquet as pq
    
    filepath = 'test_parquet.parquet'
    
    # 写入性能
    print("1. 写入性能...")
    start = time.time()
    
    table = pa.Table.from_pandas(
        pd.DataFrame(data)
    )
    pq.write_table(
        table,
        filepath,
        compression='zstd',
        compression_level=3
    )
    
    write_time = time.time() - start
    file_size = Path(filepath).stat().st_size / 1024**2
    write_speed = file_size / write_time
    
    print(f"   写入时间: {write_time:.2f}秒")
    print(f"   写入速度: {write_speed:.2f} MB/s")
    print(f"   文件大小: {file_size:.2f} MB")
    print(f"   压缩比: {(data.nbytes / 1024**2) / file_size:.2f}x")
    
    # 读取性能
    print("2. 读取性能...")
    parquet_file = pq.ParquetFile(filepath)
    parquet_data = parquet_file.read().to_pandas().values
    
    indices = np.random.randint(0, len(data) - 100, num_reads)
    
    start = time.time()
    for idx in indices:
        _ = parquet_data[idx:idx+100].copy()
    read_time = time.time() - start
    
    read_speed = num_reads / read_time
    
    print(f"   {num_reads}次随机读取: {read_time:.3f}秒")
    print(f"   读取速度: {read_speed:.1f} 样本/秒")
    
    # 清理
    Path(filepath).unlink()
    
    return {
        'name': 'parquet',
        'write_time': write_time,
        'write_speed': write_speed,
        'read_time': read_time,
        'read_speed': read_speed,
        'file_size': file_size
    }


def benchmark_hdf5(data: np.ndarray, num_reads: int = 1000):
    """测试方案3: HDF5（混合策略）"""
    print("\n" + "="*60)
    print("测试方案3: HDF5（混合策略）")
    print("="*60)
    
    import h5py
    
    filepath = 'test_hdf5.h5'
    
    # 写入性能
    print("1. 写入性能...")
    start = time.time()
    
    with h5py.File(filepath, 'w') as f:
        f.create_dataset(
            'data',
            data=data,
            chunks=(10000, data.shape[1]),
            compression='gzip',
            compression_opts=4
        )
    
    write_time = time.time() - start
    file_size = Path(filepath).stat().st_size / 1024**2
    write_speed = file_size / write_time
    
    print(f"   写入时间: {write_time:.2f}秒")
    print(f"   写入速度: {write_speed:.2f} MB/s")
    print(f"   文件大小: {file_size:.2f} MB")
    print(f"   压缩比: {(data.nbytes / 1024**2) / file_size:.2f}x")
    
    # 读取性能
    print("2. 读取性能...")
    with h5py.File(filepath, 'r') as f:
        h5_data = f['data']
        
        indices = np.random.randint(0, len(data) - 100, num_reads)
        
        start = time.time()
        for idx in indices:
            _ = h5_data[idx:idx+100].copy()
        read_time = time.time() - start
    
    read_speed = num_reads / read_time
    
    print(f"   {num_reads}次随机读取: {read_time:.3f}秒")
    print(f"   读取速度: {read_speed:.1f} 样本/秒")
    
    # 清理
    Path(filepath).unlink()
    
    return {
        'name': 'hdf5',
        'write_time': write_time,
        'write_speed': write_speed,
        'read_time': read_time,
        'read_speed': read_speed,
        'file_size': file_size
    }


def plot_comparison(results: list):
    """绘制对比图表"""
    df = pd.DataFrame(results)
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('时序数据管道方案性能对比', fontsize=16, fontweight='bold')
    
    # 1. 写入速度对比
    ax1 = axes[0, 0]
    bars1 = ax1.bar(df['name'], df['write_speed'], color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A'])
    ax1.set_ylabel('写入速度 (MB/s)', fontsize=12)
    ax1.set_title('写入速度对比', fontsize=14, fontweight='bold')
    ax1.grid(axis='y', alpha=0.3)
    
    # 添加数值标签
    for bar in bars1:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}',
                ha='center', va='bottom', fontsize=10)
    
    # 2. 读取速度对比
    ax2 = axes[0, 1]
    bars2 = ax2.bar(df['name'], df['read_speed'], color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A'])
    ax2.set_ylabel('读取速度 (样本/秒)', fontsize=12)
    ax2.set_title('读取速度对比', fontsize=14, fontweight='bold')
    ax2.grid(axis='y', alpha=0.3)
    
    for bar in bars2:
        height = bar.get_height()
        ax2.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.0f}',
                ha='center', va='bottom', fontsize=10)
    
    # 3. 文件大小对比
    ax3 = axes[1, 0]
    bars3 = ax3.bar(df['name'], df['file_size'], color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A'])
    ax3.set_ylabel('文件大小 (MB)', fontsize=12)
    ax3.set_title('存储空间对比', fontsize=14, fontweight='bold')
    ax3.grid(axis='y', alpha=0.3)
    
    for bar in bars3:
        height = bar.get_height()
        ax3.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}',
                ha='center', va='bottom', fontsize=10)
    
    # 4. 综合评分（归一化）
    ax4 = axes[1, 1]
    
    # 计算综合得分（归一化并加权）
    df_norm = df.copy()
    df_norm['write_score'] = df['write_speed'] / df['write_speed'].max() * 100
    df_norm['read_score'] = df['read_speed'] / df['read_speed'].max() * 100
    df_norm['space_score'] = (1 - df['file_size'] / df['file_size'].max()) * 100
    df_norm['total_score'] = (
        df_norm['write_score'] * 0.2 +  # 写入权重20%
        df_norm['read_score'] * 0.5 +   # 读取权重50%
        df_norm['space_score'] * 0.3    # 存储权重30%
    )
    
    bars4 = ax4.bar(df['name'], df_norm['total_score'], color=['#FF6B6B', '#4ECDC4', '#45B7D1', '#FFA07A'])
    ax4.set_ylabel('综合得分', fontsize=12)
    ax4.set_title('综合性能评分 (0-100)', fontsize=14, fontweight='bold')
    ax4.set_ylim([0, 105])
    ax4.grid(axis='y', alpha=0.3)
    
    for bar in bars4:
        height = bar.get_height()
        ax4.text(bar.get_x() + bar.get_width()/2., height,
                f'{height:.1f}',
                ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    plt.tight_layout()
    plt.savefig('performance_comparison.png', dpi=300, bbox_inches='tight')
    print(f"\n图表已保存: performance_comparison.png")
    
    return df_norm


def print_summary_table(results: list):
    """打印汇总表格"""
    df = pd.DataFrame(results)
    
    print("\n" + "="*80)
    print("性能对比汇总表")
    print("="*80)
    
    # 计算加速比（相对于memmap）
    baseline = df[df['name'] == 'memmap'].iloc[0]
    
    print(f"\n{'方案':<20} {'写入速度':<15} {'读取速度':<15} {'文件大小':<15} {'读取加速':<10}")
    print("-" * 80)
    
    for _, row in df.iterrows():
        write_speed = f"{row['write_speed']:.1f} MB/s"
        read_speed = f"{row['read_speed']:.0f} 样本/s"
        file_size = f"{row['file_size']:.1f} MB"
        speedup = f"{row['read_speed'] / baseline['read_speed']:.1f}x"
        
        print(f"{row['name']:<20} {write_speed:<15} {read_speed:<15} {file_size:<15} {speedup:<10}")
    
    print("="*80)


def main():
    """主测试函数"""
    print("="*80)
    print("时序神经网络数据管道完整性能对比")
    print("="*80)
    print("\n测试配置:")
    print("  - 数据规模: 1,000,000 时间步 x 20 特征")
    print("  - 数据大小: ~76 MB")
    print("  - 随机读取: 1000次")
    print()
    
    # 创建测试数据
    print("创建测试数据...")
    data = create_test_data(total_timesteps=1000000, feature_dim=20)
    print(f"数据shape: {data.shape}")
    print(f"数据大小: {data.nbytes / 1024**2:.2f} MB\n")
    
    # 运行所有测试
    results = []
    
    try:
        results.append(benchmark_memmap(data))
    except Exception as e:
        print(f"memmap测试失败: {e}")
    
    try:
        results.append(benchmark_shared_memory(data))
    except Exception as e:
        print(f"共享内存测试失败: {e}")
    
    try:
        results.append(benchmark_parquet(data))
    except Exception as e:
        print(f"Parquet测试失败: {e}")
    
    try:
        results.append(benchmark_hdf5(data))
    except Exception as e:
        print(f"HDF5测试失败: {e}")
    
    # 打印汇总
    if results:
        print_summary_table(results)
        
        # 绘制对比图
        try:
            df_norm = plot_comparison(results)
            
            # 推荐方案
            best_idx = df_norm['total_score'].idxmax()
            best_method = results[best_idx]['name']
            
            print(f"\n🏆 推荐方案: {best_method}")
            print(f"   综合得分: {df_norm.iloc[best_idx]['total_score']:.1f}/100")
        except Exception as e:
            print(f"绘图失败: {e}")
    
    print("\n✅ 测试完成！")


if __name__ == "__main__":
    main()
