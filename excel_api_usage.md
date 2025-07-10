# Excel处理API使用说明

## 概述

这个API可以将Excel文件转换为文本，保持表格结构和位置信息，并可以调用您现有的PDF解析API。

## 功能特点

1. **表格结构保持**: 识别并保持Excel中的表格结构
2. **图片位置信息**: 提取并记录图片的位置信息
3. **合并单元格支持**: 处理合并单元格信息
4. **多Sheet支持**: 处理包含多个Sheet的Excel文件
5. **PDF API集成**: 可选择性地调用现有的PDF解析API
6. **批量处理**: 支持批量处理多个Excel文件

## 安装和启动

### 方法1: 使用启动脚本
```bash
chmod +x start_excel_api.sh
./start_excel_api.sh
```

### 方法2: 手动安装
```bash
pip install -r requirements.txt
python excel_to_text_api.py
```

服务将在 `http://localhost:8001` 启动

## API端点

### 1. 处理单个Excel文件
**POST** `/process-excel`

**参数:**
- `file`: Excel文件 (.xlsx 或 .xls)
- `pdf_api_url` (可选): 您现有的PDF解析API URL

**示例:**
```bash
curl -X POST "http://localhost:8001/process-excel" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@your_excel_file.xlsx" \
  -F "pdf_api_url=http://your-pdf-api-url/parse"
```

**响应格式:**
```json
{
  "filename": "your_excel_file.xlsx",
  "sheets": [
    {
      "name": "Sheet1",
      "tables": [
        {
          "start_row": 0,
          "end_row": 10,
          "rows": [...],
          "structure": "table"
        }
      ],
      "images": [
        {
          "position": {"row": 5, "col": 3},
          "size": {"width": 200, "height": 150},
          "description": "图片位置: 行5, 列3"
        }
      ],
      "merged_cells": [
        {
          "range": "A1:B2",
          "start_row": 1,
          "end_row": 2,
          "start_col": 1,
          "end_col": 2
        }
      ]
    }
  ],
  "text_content": "完整的文本内容，包含表格数据和位置信息",
  "structure_preserved": true
}
```

### 2. 批量处理Excel文件
**POST** `/batch-process-excel`

**参数:**
- `files`: 多个Excel文件
- `pdf_api_url` (可选): PDF解析API URL

**示例:**
```bash
curl -X POST "http://localhost:8001/batch-process-excel" \
  -H "accept: application/json" \
  -H "Content-Type: multipart/form-data" \
  -F "files=@file1.xlsx" \
  -F "files=@file2.xlsx" \
  -F "pdf_api_url=http://your-pdf-api-url/parse"
```

### 3. 健康检查
**GET** `/health`

### 4. API信息
**GET** `/`

## 如何与现有PDF API集成

如果您有现有的PDF解析API，可以通过以下方式集成：

1. **确保您的PDF API接受POST请求**，格式如下：
   ```
   POST /your-pdf-endpoint
   Content-Type: multipart/form-data
   file: [PDF文件]
   ```

2. **API应该返回JSON格式**，包含文本内容：
   ```json
   {
     "text": "解析的文本内容",
     // 或者
     "content": "解析的文本内容"
   }
   ```

3. **在调用时提供pdf_api_url参数**：
   ```bash
   -F "pdf_api_url=http://your-domain:port/your-pdf-endpoint"
   ```

## 处理流程

1. **Excel解析**: 使用openpyxl读取Excel文件
2. **结构识别**: 识别表格区域、图片位置、合并单元格
3. **PDF转换**: 将Excel内容转换为PDF格式
4. **API调用** (可选): 如果提供了pdf_api_url，调用您的PDF解析API
5. **文本提取**: 如果没有提供PDF API，使用内置的文本提取
6. **结果整合**: 将所有信息整合为结构化输出

## 输出说明

### 表格结构保持
- 识别连续的非空行作为表格
- 保持行列关系
- 记录表格的起始和结束位置

### 图片位置信息
- 记录图片在Sheet中的精确位置（行、列）
- 记录图片尺寸信息
- 在文本中标记图片位置

### 补充文本处理
- 表格外的单独文本会被识别并保留
- 保持相对位置信息
- 合并单元格信息被完整记录

## 使用Python客户端示例

```python
import requests

# 处理单个Excel文件
with open('your_file.xlsx', 'rb') as f:
    files = {'file': f}
    data = {'pdf_api_url': 'http://your-pdf-api-url/parse'}  # 可选
    response = requests.post(
        'http://localhost:8001/process-excel', 
        files=files, 
        data=data
    )
    result = response.json()
    print(result['text_content'])

# 批量处理
files = [
    ('files', open('file1.xlsx', 'rb')),
    ('files', open('file2.xlsx', 'rb'))
]
data = {'pdf_api_url': 'http://your-pdf-api-url/parse'}
response = requests.post(
    'http://localhost:8001/batch-process-excel',
    files=files,
    data=data
)
results = response.json()
```

## 注意事项

1. **文件大小限制**: 建议单个文件不超过50MB
2. **支持格式**: .xlsx 和 .xls 格式
3. **图片处理**: 当前版本记录位置信息，不提取图片内容
4. **临时文件**: 处理过程中的临时文件会自动清理
5. **错误处理**: 提供详细的错误信息和状态码

## 故障排除

### 常见问题

1. **导入错误**: 确保所有依赖包都已安装
2. **PDF API调用失败**: 检查URL是否正确，API是否可访问
3. **Excel文件无法打开**: 确保文件没有密码保护，格式正确
4. **内存不足**: 对于大文件，可能需要增加系统内存

### 日志查看
API会在控制台输出详细的处理日志，包括错误信息。

## 扩展功能

您可以根据需要扩展以下功能：
- 图片内容OCR识别
- 更复杂的表格结构识别
- 自定义文本格式输出
- 数据库存储支持