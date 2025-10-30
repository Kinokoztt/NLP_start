"""
端到端完整示例：从数据准备到模型训练

演示如何使用三种方案进行完整的训练流程
"""

import numpy as np
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
import time
from pathlib import Path


class SimpleLSTM(nn.Module):
    """简单的LSTM时序预测模型"""
    
    def __init__(self, input_dim, hidden_dim=64, num_layers=2, pred_len=10):
        super().__init__()
        self.hidden_dim = hidden_dim
        self.num_layers = num_layers
        self.pred_len = pred_len
        
        self.lstm = nn.LSTM(
            input_dim,
            hidden_dim,
            num_layers,
            batch_first=True,
            dropout=0.1
        )
        
        self.fc = nn.Linear(hidden_dim, input_dim * pred_len)
    
    def forward(self, x):
        # x: (batch, seq_len, features)
        batch_size = x.size(0)
        
        lstm_out, _ = self.lstm(x)
        # 使用最后一个时间步的输出
        last_hidden = lstm_out[:, -1, :]
        
        # 预测未来pred_len个时间步
        output = self.fc(last_hidden)
        output = output.view(batch_size, self.pred_len, -1)
        
        return output


def train_model(model, dataloader, num_epochs=3, device='cpu'):
    """训练模型"""
    model = model.to(device)
    criterion = nn.MSELoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=0.001)
    
    print(f"\n开始训练 (设备: {device})...")
    epoch_times = []
    
    for epoch in range(num_epochs):
        model.train()
        total_loss = 0
        start_time = time.time()
        
        for batch_idx, (x, y) in enumerate(dataloader):
            x, y = x.to(device), y.to(device)
            
            optimizer.zero_grad()
            output = model(x)
            loss = criterion(output, y)
            loss.backward()
            optimizer.step()
            
            total_loss += loss.item()
            
            if batch_idx % 50 == 0:
                print(f"  Epoch {epoch+1}/{num_epochs}, Batch {batch_idx}/{len(dataloader)}, "
                      f"Loss: {loss.item():.4f}")
        
        epoch_time = time.time() - start_time
        epoch_times.append(epoch_time)
        avg_loss = total_loss / len(dataloader)
        
        print(f"✓ Epoch {epoch+1} 完成: Loss={avg_loss:.4f}, 用时={epoch_time:.2f}秒")
    
    avg_epoch_time = np.mean(epoch_times)
    print(f"\n训练完成! 平均每个epoch用时: {avg_epoch_time:.2f}秒")
    
    return avg_epoch_time


def example_shared_memory():
    """示例1：使用共享内存方案"""
    print("\n" + "="*70)
    print("示例1: 共享内存方案 (最快)")
    print("="*70)
    
    from datapipe_shared_memory import (
        SharedMemoryTimeSeriesWriter,
        SharedMemoryTimeSeriesDataset,
        cleanup_shared_memory
    )
    
    # 参数
    total_timesteps = 100000
    feature_dim = 20
    window_size = 100
    pred_len = 10
    batch_size = 128
    
    # 步骤1: 创建模拟数据并写入共享内存
    print("\n步骤1: 写入数据到共享内存...")
    start = time.time()
    
    writer = SharedMemoryTimeSeriesWriter(
        name="demo_timeseries",
        total_timesteps=total_timesteps,
        feature_dim=feature_dim
    )
    
    # 模拟从Spark批量写入
    batch_size_write = 10000
    for i in range(0, total_timesteps, batch_size_write):
        batch_data = np.random.randn(batch_size_write, feature_dim).astype(np.float32)
        writer.write_batch(batch_data)
    
    writer.save_metadata("demo_metadata.pkl")
    writer.close()
    
    write_time = time.time() - start
    print(f"  ✓ 写入完成，用时: {write_time:.2f}秒")
    
    # 步骤2: 创建数据集和DataLoader
    print("\n步骤2: 创建数据集...")
    dataset = SharedMemoryTimeSeriesDataset(
        metadata_path="demo_metadata.pkl",
        window_size=window_size,
        stride=10,
        pred_len=pred_len
    )
    
    dataloader = DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=True,
        num_workers=2,
        persistent_workers=True,
        pin_memory=True
    )
    
    print(f"  ✓ 数据集大小: {len(dataset)} 样本")
    print(f"  ✓ Batch数量: {len(dataloader)}")
    
    # 步骤3: 训练模型
    print("\n步骤3: 训练模型...")
    model = SimpleLSTM(input_dim=feature_dim, pred_len=pred_len)
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    
    train_time = train_model(model, dataloader, num_epochs=3, device=device)
    
    # 步骤4: 清理
    print("\n步骤4: 清理共享内存...")
    cleanup_shared_memory("demo_metadata.pkl")
    Path("demo_metadata.pkl").unlink()
    print("  ✓ 清理完成")
    
    return {
        'method': '共享内存',
        'write_time': write_time,
        'train_time': train_time,
        'total_time': write_time + train_time
    }


def example_parquet():
    """示例2：使用Parquet方案"""
    print("\n" + "="*70)
    print("示例2: Arrow/Parquet方案 (高压缩)")
    print("="*70)
    
    import pyarrow as pa
    import pyarrow.parquet as pq
    import pandas as pd
    from datapipe_arrow_parquet import ArrowTimeSeriesDataset
    
    # 参数
    total_timesteps = 100000
    feature_dim = 20
    window_size = 100
    pred_len = 10
    batch_size = 128
    
    # 步骤1: 创建Parquet文件
    print("\n步骤1: 写入数据到Parquet...")
    start = time.time()
    
    data = np.random.randn(total_timesteps, feature_dim).astype(np.float32)
    table = pa.Table.from_pandas(pd.DataFrame(data))
    
    pq.write_table(
        table,
        'demo_data.parquet',
        compression='zstd',
        compression_level=3
    )
    
    write_time = time.time() - start
    file_size = Path('demo_data.parquet').stat().st_size / 1024**2
    print(f"  ✓ 写入完成，用时: {write_time:.2f}秒")
    print(f"  ✓ 文件大小: {file_size:.2f} MB")
    
    # 步骤2: 创建数据集和DataLoader
    print("\n步骤2: 创建数据集...")
    dataset = ArrowTimeSeriesDataset(
        parquet_path='demo_data.parquet',
        window_size=window_size,
        stride=10,
        pred_len=pred_len
    )
    
    dataloader = DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=True,
        num_workers=2
    )
    
    print(f"  ✓ 数据集大小: {len(dataset)} 样本")
    
    # 步骤3: 训练模型
    print("\n步骤3: 训练模型...")
    model = SimpleLSTM(input_dim=feature_dim, pred_len=pred_len)
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    
    train_time = train_model(model, dataloader, num_epochs=3, device=device)
    
    # 步骤4: 清理
    print("\n步骤4: 清理文件...")
    Path('demo_data.parquet').unlink()
    print("  ✓ 清理完成")
    
    return {
        'method': 'Parquet',
        'write_time': write_time,
        'train_time': train_time,
        'total_time': write_time + train_time
    }


def example_hdf5():
    """示例3：使用HDF5混合策略方案"""
    print("\n" + "="*70)
    print("示例3: HDF5混合策略 (大规模)")
    print("="*70)
    
    from datapipe_hybrid_strategy import (
        HDF5TimeSeriesWriter,
        HybridTimeSeriesDataset
    )
    
    # 参数
    total_timesteps = 100000
    feature_dim = 20
    window_size = 100
    pred_len = 10
    batch_size = 128
    
    # 步骤1: 创建HDF5文件
    print("\n步骤1: 写入数据到HDF5...")
    start = time.time()
    
    writer = HDF5TimeSeriesWriter(
        output_path='demo_data.h5',
        total_timesteps=total_timesteps,
        feature_dim=feature_dim,
        chunk_size=10000,
        compression='gzip',
        compression_opts=4
    )
    
    # 批量写入
    batch_size_write = 10000
    for i in range(0, total_timesteps, batch_size_write):
        batch_data = np.random.randn(batch_size_write, feature_dim).astype(np.float32)
        writer.write_batch(batch_data)
    
    writer.close()
    
    write_time = time.time() - start
    file_size = Path('demo_data.h5').stat().st_size / 1024**2
    print(f"  ✓ 写入完成，用时: {write_time:.2f}秒")
    print(f"  ✓ 文件大小: {file_size:.2f} MB")
    
    # 步骤2: 创建数据集和DataLoader
    print("\n步骤2: 创建数据集...")
    dataset = HybridTimeSeriesDataset(
        hdf5_path='demo_data.h5',
        window_size=window_size,
        stride=10,
        pred_len=pred_len,
        cache_size_gb=0.5  # 512MB缓存
    )
    
    dataloader = DataLoader(
        dataset,
        batch_size=batch_size,
        shuffle=True,
        num_workers=2
    )
    
    print(f"  ✓ 数据集大小: {len(dataset)} 样本")
    
    # 步骤3: 训练模型
    print("\n步骤3: 训练模型...")
    model = SimpleLSTM(input_dim=feature_dim, pred_len=pred_len)
    device = 'cuda' if torch.cuda.is_available() else 'cpu'
    
    train_time = train_model(model, dataloader, num_epochs=3, device=device)
    
    # 步骤4: 清理
    print("\n步骤4: 清理文件...")
    Path('demo_data.h5').unlink()
    print("  ✓ 清理完成")
    
    return {
        'method': 'HDF5',
        'write_time': write_time,
        'train_time': train_time,
        'total_time': write_time + train_time
    }


def compare_all_methods():
    """对比所有三种方案"""
    print("\n" + "="*70)
    print("端到端完整示例：三种方案对比")
    print("="*70)
    print("\n测试配置:")
    print("  - 数据规模: 100,000 时间步 x 20 特征")
    print("  - 滑窗大小: 100")
    print("  - 预测长度: 10")
    print("  - Batch大小: 128")
    print("  - 训练轮数: 3")
    print()
    
    results = []
    
    # 运行三个示例
    try:
        result1 = example_shared_memory()
        results.append(result1)
    except Exception as e:
        print(f"\n❌ 共享内存方案失败: {e}")
    
    try:
        result2 = example_parquet()
        results.append(result2)
    except Exception as e:
        print(f"\n❌ Parquet方案失败: {e}")
    
    try:
        result3 = example_hdf5()
        results.append(result3)
    except Exception as e:
        print(f"\n❌ HDF5方案失败: {e}")
    
    # 打印对比结果
    if results:
        print("\n" + "="*70)
        print("对比总结")
        print("="*70)
        print(f"\n{'方案':<15} {'准备时间':<12} {'训练时间':<12} {'总时间':<12}")
        print("-" * 70)
        
        for r in results:
            print(f"{r['method']:<15} {r['write_time']:>10.2f}秒  {r['train_time']:>10.2f}秒  {r['total_time']:>10.2f}秒")
        
        # 找出最快的方案
        fastest = min(results, key=lambda x: x['total_time'])
        print(f"\n🏆 最快方案: {fastest['method']} (总用时 {fastest['total_time']:.2f}秒)")
    
    print("\n" + "="*70)
    print("✅ 所有示例运行完成！")
    print("="*70)


if __name__ == "__main__":
    # 运行完整对比
    compare_all_methods()
    
    print("\n💡 提示:")
    print("  - 如果数据能放入内存，优先使用共享内存方案")
    print("  - 如果需要最小存储空间，使用Parquet方案")
    print("  - 如果数据超大，使用HDF5混合策略方案")
    print("\n📖 详细文档请查看 QUICK_START.md")
