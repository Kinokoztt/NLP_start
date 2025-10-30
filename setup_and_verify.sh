#!/bin/bash
# 安装和验证脚本

set -e

echo "=========================================="
echo "时序数据管道优化方案 - 安装和验证"
echo "=========================================="

# 检查Python版本
echo ""
echo "1. 检查Python版本..."
python3 --version || python --version

# 安装依赖
echo ""
echo "2. 安装依赖包..."
pip3 install -r requirements.txt || pip install -r requirements.txt

# 验证导入
echo ""
echo "3. 验证模块导入..."

echo "   检查NumPy..."
python3 -c "import numpy; print(f'   ✓ NumPy {numpy.__version__}')"

echo "   检查PyTorch..."
python3 -c "import torch; print(f'   ✓ PyTorch {torch.__version__}')"

echo "   检查PyArrow..."
python3 -c "import pyarrow; print(f'   ✓ PyArrow {pyarrow.__version__}')"

echo "   检查h5py..."
python3 -c "import h5py; print(f'   ✓ h5py {h5py.__version__}')"

echo "   检查Pandas..."
python3 -c "import pandas; print(f'   ✓ Pandas {pandas.__version__}')"

# 运行快速测试
echo ""
echo "4. 运行快速功能测试..."

echo "   测试共享内存模块..."
python3 -c "
from datapipe_shared_memory import SharedMemoryTimeSeriesWriter
import numpy as np

# 创建小规模测试
writer = SharedMemoryTimeSeriesWriter('test_shm', 1000, 5)
writer.write_batch(np.random.randn(1000, 5).astype(np.float32))
writer.save_metadata('test_meta.pkl')
writer.close()

from datapipe_shared_memory import SharedMemoryTimeSeriesDataset, cleanup_shared_memory
dataset = SharedMemoryTimeSeriesDataset('test_meta.pkl', window_size=10, pred_len=1)
print(f'   ✓ 共享内存测试通过 (数据集大小: {len(dataset)})')

cleanup_shared_memory('test_meta.pkl')
import os
os.remove('test_meta.pkl')
"

echo "   测试Parquet模块..."
python3 -c "
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datapipe_arrow_parquet import ArrowTimeSeriesDataset

# 创建测试Parquet文件
data = np.random.randn(1000, 5).astype(np.float32)
table = pa.Table.from_pandas(pd.DataFrame(data))
pq.write_table(table, 'test.parquet', compression='zstd')

# 测试数据集
dataset = ArrowTimeSeriesDataset('test.parquet', window_size=10, pred_len=1)
print(f'   ✓ Parquet测试通过 (数据集大小: {len(dataset)})')

import os
os.remove('test.parquet')
"

echo "   测试HDF5模块..."
python3 -c "
import numpy as np
from datapipe_hybrid_strategy import HDF5TimeSeriesWriter, HybridTimeSeriesDataset

# 创建测试HDF5文件
writer = HDF5TimeSeriesWriter('test.h5', 1000, 5, chunk_size=100)
writer.write_batch(np.random.randn(1000, 5).astype(np.float32))
writer.close()

# 测试数据集
dataset = HybridTimeSeriesDataset('test.h5', window_size=10, pred_len=1, cache_size_gb=0.1)
print(f'   ✓ HDF5测试通过 (数据集大小: {len(dataset)})')

import os
os.remove('test.h5')
"

echo ""
echo "=========================================="
echo "✅ 安装和验证完成！"
echo "=========================================="
echo ""
echo "下一步："
echo "  1. 查看 QUICK_START.md 了解使用方法"
echo "  2. 运行 python3 example_end_to_end.py 查看完整示例"
echo "  3. 运行 python3 benchmark_comparison.py 进行性能对比"
echo ""
