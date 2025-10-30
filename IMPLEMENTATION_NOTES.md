# 实现说明和技术细节

## 核心设计理念

### 1. 零拷贝 (Zero-Copy)

所有三个方案都尽可能采用零拷贝技术：

**共享内存方案**：
```python
# 使用numpy的buffer view，避免数据复制
self.data = np.ndarray(shape, dtype=dtype, buffer=shm.buf)
x = self.data[start:end]  # 返回view而非copy
```

**Parquet方案**：
```python
# PyArrow的零拷贝转换
table = self.parquet_file.read()
self.data = table.to_pandas().values  # 零拷贝
```

**HDF5方案**：
```python
# HDF5的切片直接从磁盘读取到内存
data = self.data[start:end]  # 智能I/O
```

### 2. 时间步分组存储

保持了原方案的优势设计：

```python
# 以连续的时间步组存储，而非单个样本
# 这样滑窗生成的多个样本可以共享相同的数据块
#
# 原始存储（时间步）:
# [t0, t1, t2, t3, t4, t5, t6, t7, t8, t9, ...]
#
# 生成样本（window_size=3, stride=1）:
# sample_0: [t0, t1, t2] -> predict [t3]
# sample_1: [t1, t2, t3] -> predict [t4]
# sample_2: [t2, t3, t4] -> predict [t5]
#
# 注意sample_0和sample_1共享了[t1, t2]，这就是存储优化的关键
```

### 3. 多进程友好

**共享内存**：
- 使用`multiprocessing.shared_memory`而非`threading`
- 支持PyTorch的`DataLoader`多进程
- 每个worker进程独立连接到共享内存

**其他方案**：
- 使用持久化文件，天然支持多进程
- 配合`persistent_workers=True`避免重复初始化

## 性能优化技巧

### 1. DataLoader配置

```python
# 最佳配置模板
dataloader = DataLoader(
    dataset,
    batch_size=256,              # 根据GPU内存调整
    shuffle=True,
    num_workers=4,               # 通常设为CPU核心数的一半
    persistent_workers=True,      # ⚠️ 关键：避免worker重复初始化
    pin_memory=True,             # ⚠️ 关键：加速CPU到GPU传输
    prefetch_factor=2,           # 每个worker预取2个batch
    drop_last=True               # 训练时丢弃不完整的batch
)
```

**为什么这些参数重要？**

- `persistent_workers=True`: 
  - 避免每个epoch重新创建worker进程
  - 共享内存方案中特别重要（避免重复连接）
  - 可节省30-50%的epoch开始时间

- `pin_memory=True`:
  - 数据固定在页锁定内存中
  - GPU可以通过DMA直接访问
  - 加速CPU->GPU传输约2-3倍

- `prefetch_factor=2`:
  - 每个worker异步预取2个batch
  - CPU处理和GPU计算并行
  - 避免GPU空转等待数据

### 2. 内存使用优化

**共享内存方案**：
```python
# 确保系统有足够的共享内存空间
# Linux: 检查 /dev/shm 大小
# df -h /dev/shm

# 如果不够，临时增加：
# sudo mount -o remount,size=10G /dev/shm
```

**Parquet方案**：
```python
# Row Group大小影响性能
# 小Row Group (1000-5000): 适合随机访问
# 大Row Group (50000-100000): 适合顺序访问

pq.write_table(
    table,
    path,
    row_group_size=10000,  # 根据访问模式调整
    compression='zstd',     # 速度和压缩比平衡
    compression_level=3     # 级别越高压缩越好但越慢
)
```

**HDF5方案**：
```python
# Chunk大小影响I/O效率
# 经验法则: 10KB - 1MB per chunk
# 时序数据: chunk_size约等于typical_window_size

f.create_dataset(
    'data',
    chunks=(10000, feature_dim),  # 10000时间步为一个chunk
    compression='gzip',
    compression_opts=4  # 级别4是速度和压缩的平衡点
)
```

### 3. 缓存策略

**LRU缓存（HDF5方案）**：
```python
# 缓存大小设置建议
# 训练: 2-4GB (足够容纳多个epoch的热数据)
# 推理: 0.5-1GB (访问模式更随机)

cache_size_gb = 2.0  # 根据可用内存调整

# 缓存利用率监控
stats = dataset.get_cache_stats()
print(f"缓存利用率: {stats['cache_utilization']:.1%}")
```

## 常见陷阱和解决方案

### 陷阱1：共享内存未清理

**问题**：
```python
# 训练中断后，共享内存没有释放
# /dev/shm 被占满
```

**解决方案**：
```python
# 方法1：使用上下文管理器
from contextlib import contextmanager

@contextmanager
def shared_memory_dataset(metadata_path):
    dataset = SharedMemoryTimeSeriesDataset(metadata_path, ...)
    try:
        yield dataset
    finally:
        cleanup_shared_memory(metadata_path)

# 使用
with shared_memory_dataset("meta.pkl") as dataset:
    # 训练代码
    pass

# 方法2：手动清理
# Linux: rm /dev/shm/psm_*
```

### 陷阱2：Parquet读取慢

**问题**：
```python
# 读取Parquet时发现比预期慢
```

**原因和解决**：
```python
# 原因1：Row Group太小，元数据开销大
# 解决：增加row_group_size

# 原因2：没有利用列式存储的优势
# 解决：只读取需要的列
dataset = ArrowTimeSeriesDataset(
    parquet_path,
    feature_cols=['f1', 'f2', 'f3']  # 而非所有列
)

# 原因3：文件在HDFS上，网络I/O慢
# 解决：使用缓存或本地化
spark.read.parquet("hdfs://...").write.parquet("file:///local/path")
```

### 陷阱3：HDF5多进程访问

**问题**：
```python
# DataLoader num_workers > 0 时出错
# HDF5: Can't read data (file read failed)
```

**解决方案**：
```python
# HDF5不支持多进程写入，但支持多进程读取
# 确保：
# 1. 文件以只读模式打开
self.hdf5_file = h5py.File(path, 'r')  # 'r' not 'r+'

# 2. 每个worker进程独立打开文件
# 在__init__中不要共享file handle
```

### 陷阱4：GPU内存不足

**问题**：
```python
# CUDA out of memory
```

**解决方案**：
```python
# 方法1：减小batch_size
dataloader = DataLoader(dataset, batch_size=128)  # 从256降到128

# 方法2：梯度累积
accumulation_steps = 4
for i, (x, y) in enumerate(dataloader):
    loss = model(x, y) / accumulation_steps
    loss.backward()
    
    if (i + 1) % accumulation_steps == 0:
        optimizer.step()
        optimizer.zero_grad()

# 方法3：混合精度训练
from torch.cuda.amp import autocast, GradScaler

scaler = GradScaler()
with autocast():
    output = model(x)
    loss = criterion(output, y)

scaler.scale(loss).backward()
scaler.step(optimizer)
scaler.update()
```

## 扩展和定制

### 1. 添加数据增强

```python
def augment_timeseries(x, y):
    """时序数据增强"""
    # 添加噪声
    x = x + torch.randn_like(x) * 0.01
    
    # 缩放
    scale = torch.randn(1) * 0.1 + 1.0
    x = x * scale
    
    # 时间偏移（需要重新从数据集读取）
    # ...
    
    return x, y

dataset = SharedMemoryTimeSeriesDataset(
    ...,
    transform=augment_timeseries
)
```

### 2. 支持多变量预测

```python
class MultiVariateDataset(SharedMemoryTimeSeriesDataset):
    """支持不同特征有不同预测长度"""
    
    def __init__(self, ..., target_features, pred_len_dict):
        super().__init__(...)
        self.target_features = target_features
        self.pred_len_dict = pred_len_dict
    
    def __getitem__(self, idx):
        x, y = super().__getitem__(idx)
        
        # 只返回目标特征
        y_target = y[:, self.target_features]
        
        return x, y_target
```

### 3. 支持不等长序列

```python
from torch.nn.utils.rnn import pad_sequence

def collate_variable_length(batch):
    """处理不等长序列"""
    x_list, y_list = zip(*batch)
    
    # Padding
    x_padded = pad_sequence(x_list, batch_first=True)
    y_padded = pad_sequence(y_list, batch_first=True)
    
    # 创建mask
    lengths = torch.tensor([len(x) for x in x_list])
    
    return x_padded, y_padded, lengths

dataloader = DataLoader(
    dataset,
    collate_fn=collate_variable_length
)
```

## 性能调优清单

### 数据准备阶段

- [ ] 选择合适的数据格式（共享内存 vs Parquet vs HDF5）
- [ ] 优化Spark输出（分区数、文件大小）
- [ ] 设置合适的压缩算法和级别
- [ ] 按时间排序数据
- [ ] 过滤无效数据

### 训练阶段

- [ ] 设置合适的batch_size（通常128-512）
- [ ] 启用persistent_workers
- [ ] 启用pin_memory（GPU训练）
- [ ] 设置合适的num_workers（CPU核心数的1/2到2/3）
- [ ] 使用prefetch_factor进行异步预取
- [ ] 监控GPU利用率（应>90%）
- [ ] 监控CPU利用率（应<80%）
- [ ] 监控内存使用

### 调试阶段

- [ ] 使用小数据集验证流程
- [ ] 测试单个样本加载时间
- [ ] 测试batch加载时间
- [ ] 分析瓶颈（I/O vs 计算）
- [ ] 检查数据质量（NaN, Inf）
- [ ] 验证数据形状和类型

## 生产环境部署建议

### 1. 数据管理

```python
# 推荐目录结构
project/
├── data/
│   ├── raw/              # 原始Parquet/HDF5文件
│   ├── processed/        # 处理后的数据
│   └── metadata/         # 元数据文件
├── models/               # 保存的模型
├── logs/                 # 训练日志
└── configs/              # 配置文件
```

### 2. 配置管理

```python
# config.yaml
data:
  format: "shared_memory"  # or "parquet", "hdf5"
  path: "/path/to/data"
  window_size: 100
  pred_len: 10
  stride: 10

training:
  batch_size: 256
  num_workers: 4
  epochs: 50
  lr: 0.001

# 使用
import yaml
with open('config.yaml') as f:
    config = yaml.safe_load(f)
```

### 3. 日志和监控

```python
# 使用TensorBoard
from torch.utils.tensorboard import SummaryWriter

writer = SummaryWriter('runs/experiment_1')

for epoch in range(num_epochs):
    for i, (x, y) in enumerate(dataloader):
        # 训练
        loss = ...
        
        # 记录
        writer.add_scalar('Loss/train', loss, epoch * len(dataloader) + i)
        
        # 监控数据加载速度
        if i % 100 == 0:
            writer.add_scalar('Speed/samples_per_sec', 
                             batch_size / batch_time, 
                             global_step)

writer.close()
```

### 4. 容错和恢复

```python
# 保存checkpoint
torch.save({
    'epoch': epoch,
    'model_state_dict': model.state_dict(),
    'optimizer_state_dict': optimizer.state_dict(),
    'loss': loss,
}, f'checkpoint_epoch_{epoch}.pt')

# 恢复训练
checkpoint = torch.load('checkpoint_epoch_10.pt')
model.load_state_dict(checkpoint['model_state_dict'])
optimizer.load_state_dict(checkpoint['optimizer_state_dict'])
start_epoch = checkpoint['epoch'] + 1
```

## 总结

这三个方案各有优势：

1. **共享内存**：最快，但受内存限制
2. **Parquet**：最节省空间，Spark友好
3. **HDF5**：最灵活，适合超大数据

选择哪个方案取决于：
- 数据规模
- 可用内存
- 访问模式
- 存储要求

建议先用小数据集测试所有方案，选择最适合你场景的。
