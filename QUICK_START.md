# 快速开始指南

## 安装依赖

```bash
pip install -r requirements.txt
```

## 方案选择决策树

```
开始
 │
 ├─ 数据能否全部加载到内存？
 │   ├─ 是 → 使用方案1（共享内存）⭐ 推荐
 │   └─ 否 → 继续
 │
 ├─ 是否需要频繁切换数据集？
 │   ├─ 是 → 使用方案2（Arrow/Parquet）
 │   └─ 否 → 使用方案3（混合策略）
```

## 方案1：共享内存（最快，适合中等规模）

### 步骤1：从Spark写入共享内存

```python
from datapipe_shared_memory import SharedMemoryTimeSeriesWriter

# 初始化写入器
writer = SharedMemoryTimeSeriesWriter(
    name="my_timeseries",
    total_timesteps=1000000,
    feature_dim=10,
    dtype=np.float32
)

# 从Spark DataFrame批量写入
writer.write_from_spark(
    spark_df=your_spark_dataframe,
    time_col="timestamp",
    feature_cols=["feature1", "feature2", "feature3", ...]
)

# 保存元数据
writer.save_metadata("timeseries_metadata.pkl")
writer.close()
```

### 步骤2：训练时使用

```python
from datapipe_shared_memory import SharedMemoryTimeSeriesDataset
from torch.utils.data import DataLoader

# 创建数据集
dataset = SharedMemoryTimeSeriesDataset(
    metadata_path="timeseries_metadata.pkl",
    window_size=100,     # 输入序列长度
    stride=1,            # 滑窗步长
    pred_len=10          # 预测长度
)

# 创建DataLoader
dataloader = DataLoader(
    dataset,
    batch_size=256,
    shuffle=True,
    num_workers=4,              # 多进程加载
    persistent_workers=True,    # 保持worker进程
    pin_memory=True,           # GPU加速
    prefetch_factor=2          # 预取因子
)

# 训练
for epoch in range(num_epochs):
    for x, y in dataloader:
        # x: (batch_size, window_size, feature_dim)
        # y: (batch_size, pred_len, feature_dim)
        
        output = model(x)
        loss = criterion(output, y)
        # ...
```

### 步骤3：清理（训练完成后）

```python
from datapipe_shared_memory import cleanup_shared_memory

cleanup_shared_memory("timeseries_metadata.pkl")
```

---

## 方案2：Arrow/Parquet（高压缩，适合大规模）

### 步骤1：Spark直接输出Parquet

```python
from datapipe_arrow_parquet import ArrowTimeSeriesWriter

# 直接从Spark输出Parquet
ArrowTimeSeriesWriter.write_from_spark(
    spark_df=your_spark_dataframe,
    output_path="hdfs://path/to/output.parquet",
    time_col="timestamp",
    feature_cols=["feature1", "feature2", ...],
    group_size=10000,       # Row Group大小
    compression='zstd'      # 压缩算法
)
```

或者在Spark中直接写入：

```python
(spark_df
 .orderBy("timestamp")
 .select(feature_cols)
 .write
 .mode('overwrite')
 .option('compression', 'zstd')
 .parquet("output_path"))
```

### 步骤2：训练时使用

```python
from datapipe_arrow_parquet import ArrowTimeSeriesDataset
from torch.utils.data import DataLoader

# 如果数据能全部加载到内存
dataset = ArrowTimeSeriesDataset(
    parquet_path="output_path",
    window_size=100,
    stride=1,
    pred_len=10,
    feature_cols=None  # None表示读取所有列
)

# 如果数据太大，使用懒加载版本
from datapipe_arrow_parquet import LazyArrowTimeSeriesDataset

dataset = LazyArrowTimeSeriesDataset(
    parquet_path="output_path",
    window_size=100,
    stride=1,
    pred_len=10,
    cache_row_groups=5  # 缓存5个row group
)

# 创建DataLoader
dataloader = DataLoader(
    dataset,
    batch_size=256,
    shuffle=True,
    num_workers=4,
    persistent_workers=True
)

# 训练循环同方案1
```

---

## 方案3：混合策略（自适应，适合超大规模）

### 步骤1：写入HDF5

```python
from datapipe_hybrid_strategy import HDF5TimeSeriesWriter

writer = HDF5TimeSeriesWriter(
    output_path="timeseries.h5",
    total_timesteps=10000000,
    feature_dim=20,
    chunk_size=10000,
    compression='gzip',
    compression_opts=4
)

# 从Spark写入
writer.write_from_spark(
    spark_df=your_spark_dataframe,
    time_col="timestamp",
    feature_cols=["feature1", "feature2", ...],
    batch_size=100000
)

writer.close()
```

### 步骤2：训练时使用

```python
from datapipe_hybrid_strategy import HybridTimeSeriesDataset
from torch.utils.data import DataLoader

# 创建混合策略数据集
dataset = HybridTimeSeriesDataset(
    hdf5_path="timeseries.h5",
    window_size=100,
    stride=1,
    pred_len=10,
    cache_size_gb=2.0,  # 2GB内存缓存
    prefetch_size=100
)

# 或使用自适应版本（推荐）
from datapipe_hybrid_strategy import AdaptiveTimeSeriesDataset

dataset = AdaptiveTimeSeriesDataset(
    hdf5_path="timeseries.h5",
    window_size=100,
    stride=1,
    pred_len=10,
    cache_size_gb=2.0,
    warmup_samples=1000  # 预热样本数
)

dataloader = DataLoader(
    dataset,
    batch_size=256,
    shuffle=True,
    num_workers=4
)

# 第一个epoch后预热缓存
for epoch in range(num_epochs):
    if epoch == 1:
        dataset.warmup_cache()  # 预热最常访问的样本
    
    for x, y in dataloader:
        # 训练代码
        pass
```

---

## 性能对比测试

运行完整的性能对比：

```bash
python benchmark_comparison.py
```

这将生成：
- 控制台输出：详细的性能指标
- `performance_comparison.png`：可视化对比图表

---

## 完整示例：端到端流程

### 示例1：小规模数据集（共享内存方案）

```python
# ====== 步骤1: 数据准备 ======
from pyspark.sql import SparkSession
from datapipe_shared_memory import SharedMemoryTimeSeriesWriter

spark = SparkSession.builder.appName("TimeSeries").getOrCreate()

# 从HDFS读取
df = spark.read.parquet("hdfs://path/to/your/data")
total_timesteps = df.count()

# 写入共享内存
writer = SharedMemoryTimeSeriesWriter(
    name="training_data",
    total_timesteps=total_timesteps,
    feature_dim=10
)

# 批量写入
pandas_df = df.orderBy("timestamp").select(feature_cols).toPandas()
writer.write_batch(pandas_df.values.astype(np.float32))
writer.save_metadata("metadata.pkl")
writer.close()

# ====== 步骤2: 训练 ======
import torch
import torch.nn as nn
from torch.utils.data import DataLoader
from datapipe_shared_memory import SharedMemoryTimeSeriesDataset

# 创建数据集
train_dataset = SharedMemoryTimeSeriesDataset(
    metadata_path="metadata.pkl",
    window_size=100,
    stride=1,
    pred_len=10
)

train_loader = DataLoader(
    train_dataset,
    batch_size=256,
    shuffle=True,
    num_workers=4,
    persistent_workers=True,
    pin_memory=True
)

# 定义模型（示例）
class TimeSeriesModel(nn.Module):
    def __init__(self, input_dim, hidden_dim, output_dim):
        super().__init__()
        self.lstm = nn.LSTM(input_dim, hidden_dim, batch_first=True)
        self.fc = nn.Linear(hidden_dim, output_dim)
    
    def forward(self, x):
        # x: (batch, seq_len, features)
        lstm_out, _ = self.lstm(x)
        output = self.fc(lstm_out[:, -1, :])
        return output

model = TimeSeriesModel(input_dim=10, hidden_dim=64, output_dim=10)
model = model.cuda()

criterion = nn.MSELoss()
optimizer = torch.optim.Adam(model.parameters(), lr=0.001)

# 训练循环
num_epochs = 10
for epoch in range(num_epochs):
    model.train()
    total_loss = 0
    
    for batch_idx, (x, y) in enumerate(train_loader):
        x, y = x.cuda(), y.cuda()
        
        optimizer.zero_grad()
        output = model(x)
        loss = criterion(output, y[:, -1, :])
        loss.backward()
        optimizer.step()
        
        total_loss += loss.item()
        
        if batch_idx % 100 == 0:
            print(f"Epoch {epoch}, Batch {batch_idx}, Loss: {loss.item():.4f}")
    
    avg_loss = total_loss / len(train_loader)
    print(f"Epoch {epoch} completed, Avg Loss: {avg_loss:.4f}")

# ====== 步骤3: 清理 ======
from datapipe_shared_memory import cleanup_shared_memory

cleanup_shared_memory("metadata.pkl")

print("训练完成！")
```

---

## 性能优化建议

### 1. DataLoader优化

```python
# 推荐配置
dataloader = DataLoader(
    dataset,
    batch_size=256,              # 根据GPU内存调整
    shuffle=True,
    num_workers=4,               # CPU核心数的一半
    persistent_workers=True,      # ⚠️ 重要：保持worker进程
    pin_memory=True,             # ⚠️ 重要：加速GPU传输
    prefetch_factor=2,           # 每个worker预取2个batch
    drop_last=True               # 丢弃不完整的batch
)
```

### 2. 共享内存方案优化

- **内存大小**：确保可用RAM > 数据集大小 * 1.2
- **多实验共享**：多个训练任务可以共享同一块内存
- **清理时机**：所有训练任务完成后再清理

### 3. Parquet方案优化

- **Row Group大小**：
  - 较小（1000-5000）：适合随机访问
  - 较大（50000-100000）：适合顺序访问
- **压缩算法**：
  - `zstd`：平衡速度和压缩比（推荐）
  - `snappy`：最快但压缩比低
  - `gzip`：压缩比高但慢

### 4. HDF5方案优化

- **Chunk大小**：通常设为10000-50000
- **缓存大小**：根据可用内存设置，通常1-4GB
- **压缩级别**：gzip level 4是速度和压缩的平衡点

---

## 常见问题

### Q1: 如何选择stride（滑窗步长）？

- **stride=1**：生成最多样本，但相邻样本高度重叠
- **stride=window_size**：无重叠，样本最少
- **推荐**：stride = window_size // 4 到 window_size // 2

### Q2: 共享内存使用后如何确认已清理？

```bash
# Linux/Mac
ls -lh /dev/shm/

# 如果看到psm_* 文件，说明未清理
# 手动删除：
rm /dev/shm/psm_*
```

### Q3: 如何处理多个特征列类型不一致？

在Spark中统一转换为float32：

```python
from pyspark.sql import functions as F

feature_cols = ["f1", "f2", "f3"]
for col in feature_cols:
    df = df.withColumn(col, F.col(col).cast("float"))
```

### Q4: 训练时内存占用过高怎么办？

1. 减少`num_workers`
2. 减少`batch_size`
3. 减少`prefetch_factor`
4. 如果使用共享内存，考虑切换到Parquet/HDF5方案

### Q5: 如何加速数据从Spark到本地的传输？

```python
# 使用repartition控制并行度
df = df.repartition(100)  # 100个分区

# 使用coalesce减少小文件
df = df.coalesce(10)

# 使用cache避免重复计算
df = df.cache()
```

---

## 预期性能提升

相比当前的memmap方案：

| 指标 | 方案1(共享内存) | 方案2(Parquet) | 方案3(混合) |
|------|----------------|----------------|-------------|
| 读取速度 | **10-50x** ⚡ | 5-20x | 5-30x |
| 准备时间 | **-80%** ⚡ | **-90%** ⚡ | -60% |
| 存储空间 | 节省50% | **节省50-80%** ⚡ | 节省40-60% |
| 适用规模 | <100GB | 任意 | 任意 |

---

## 技术支持

如有问题，请参考：
- `DATAPIPE_OPTIMIZATION.md` - 详细的技术方案文档
- 各个实现文件中的docstring和注释
- `benchmark_comparison.py` - 运行性能测试

祝训练顺利！🚀
