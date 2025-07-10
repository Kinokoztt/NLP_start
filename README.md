# Excel处理API项目

这是一个完整的Excel文件处理解决方案，可以将Excel文件转换为结构化的文本内容，保持表格结构、图片位置信息和补充文本，并可以与您现有的PDF解析API集成。

## 🎯 项目特点

✅ **完整的Excel解析** - 支持.xlsx和.xls格式  
✅ **表格结构保持** - 智能识别并保留表格的行列结构  
✅ **图片位置记录** - 精确记录Excel中图片的位置和尺寸信息  
✅ **文本区域识别** - 识别表格外的独立文本内容  
✅ **合并单元格处理** - 完整处理合并单元格信息  
✅ **PDF API集成** - 可调用现有的PDF解析API  
✅ **批量处理支持** - 支持同时处理多个Excel文件  
✅ **RESTful API** - 标准的REST API接口  

## 🚀 快速开始

### 1. 启动服务

```bash
# 方法1: 使用启动脚本（推荐）
chmod +x start_excel_api.sh
./start_excel_api.sh

# 方法2: 手动启动
source excel_api_env/bin/activate
python excel_to_text_api_simple.py
```

服务将在 `http://localhost:8001` 启动

### 2. 测试服务

```bash
# 健康检查
curl http://localhost:8001/health

# 查看API信息
curl http://localhost:8001/
```

## 📋 API端点

### 处理单个Excel文件
**POST** `/process-excel`

```bash
curl -X POST "http://localhost:8001/process-excel" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@your_excel_file.xlsx" \
  -F "pdf_api_url=http://your-pdf-api-url/parse"  # 可选
```

### 批量处理Excel文件
**POST** `/batch-process-excel`

```bash
curl -X POST "http://localhost:8001/batch-process-excel" \
  -H "Content-Type: multipart/form-data" \
  -F "files=@file1.xlsx" \
  -F "files=@file2.xlsx" \
  -F "pdf_api_url=http://your-pdf-api-url/parse"  # 可选
```

### 其他端点
- **GET** `/health` - 健康检查
- **GET** `/` - API信息

## 📊 响应格式

```json
{
  "filename": "example.xlsx",
  "processed_at": "2024-01-10T12:00:00",
  "sheets": [
    {
      "name": "Sheet1",
      "total_rows": 20,
      "total_cols": 6,
      "tables": [
        {
          "start_row": 1,
          "end_row": 10,
          "column_count": 6,
          "structure": "table",
          "rows": [...]
        }
      ],
      "text_regions": [
        {
          "start_row": 15,
          "end_row": 16,
          "type": "text_block",
          "content": [...]
        }
      ],
      "images": [
        {
          "position": {"row": 5, "col": 3},
          "size": {"width": 200, "height": 150},
          "description": "图片位置: 行6, 列4"
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
  "text_content": "完整的文本内容，保持结构...",
  "structure_preserved": true
}
```

## 🔧 与现有PDF API集成

如果您有现有的PDF解析API，只需在请求中提供`pdf_api_url`参数：

### PDF API要求
1. **接受POST请求**，格式：
   ```
   POST /your-pdf-endpoint
   Content-Type: multipart/form-data
   file: [PDF文件]
   ```

2. **返回JSON格式**：
   ```json
   {
     "text": "解析的文本内容"
     // 或者
     "content": "解析的文本内容"
   }
   ```

### 集成示例
```bash
curl -X POST "http://localhost:8001/process-excel" \
  -F "file=@data.xlsx" \
  -F "pdf_api_url=http://your-domain:port/pdf-parse"
```

## 🛠️ 处理流程

```
Excel文件 → 结构解析 → PDF转换 → PDF API(可选) → 文本输出
    ↓           ↓           ↓           ↓            ↓
  上传文件   提取表格     生成PDF    调用您的API   返回结构化文本
            图片位置                              
            文本区域                              
            合并单元格                            
```

## 📁 项目文件

```
workspace/
├── excel_to_text_api_simple.py    # 主API服务文件
├── start_excel_api.sh             # 启动脚本
├── requirements.txt               # Python依赖
├── test_excel_api.py              # 测试脚本
├── excel_api_usage.md             # 详细使用说明
├── excel_api_env/                 # Python虚拟环境
└── README.md                      # 项目说明（本文件）
```

## 🧪 测试

运行测试脚本验证API功能：

```bash
source excel_api_env/bin/activate
python test_excel_api.py
```

测试脚本会：
1. 创建示例Excel文件
2. 测试API健康状况
3. 测试单文件处理
4. 测试批量处理
5. 显示处理结果

## 📋 依赖包

- **fastapi** - Web框架
- **uvicorn** - ASGI服务器
- **openpyxl** - Excel文件处理
- **reportlab** - PDF生成
- **httpx** - HTTP客户端（调用PDF API）
- **pillow** - 图像处理
- **python-multipart** - 文件上传支持
- **aiofiles** - 异步文件操作

## ⚡ 性能特点

- **异步处理** - 支持并发请求
- **内存优化** - 自动清理临时文件
- **大文件支持** - 智能分页处理大型Excel
- **错误处理** - 完善的错误提示和恢复机制

## 🔍 功能详解

### 表格结构识别
- 自动识别连续的数据行作为表格
- 保持原始的行列结构
- 记录表格的起始和结束位置

### 图片位置信息
- 精确记录图片在Sheet中的位置（行、列）
- 提取图片尺寸信息
- 在文本输出中标记图片位置

### 文本区域处理
- 识别表格外的独立文本
- 保持文本的相对位置信息
- 区分不同类型的内容区域

### 合并单元格支持
- 完整记录合并单元格的范围
- 保持合并单元格的内容
- 在文本输出中标注合并信息

## 📝 使用场景

1. **文档数字化** - 将Excel表格转换为可搜索的文本
2. **数据提取** - 从复杂Excel中提取结构化信息
3. **内容分析** - 分析Excel中的文本和图片内容
4. **API集成** - 与现有的文档处理流程集成
5. **批量处理** - 处理大量Excel文件

## 🚨 注意事项

1. **文件大小限制**: 建议单个文件不超过50MB
2. **支持格式**: .xlsx 和 .xls 格式
3. **图片处理**: 当前版本记录位置信息，不提取图片内容
4. **临时文件**: 处理过程中的临时文件会自动清理
5. **错误处理**: 提供详细的错误信息和状态码

## 🐛 故障排除

### 常见问题

1. **依赖安装失败**
   ```bash
   # 确保Python版本兼容
   python --version  # 应该是3.8+
   
   # 重新创建虚拟环境
   rm -rf excel_api_env
   python3 -m venv excel_api_env
   source excel_api_env/bin/activate
   pip install fastapi uvicorn openpyxl reportlab httpx python-multipart aiofiles
   ```

2. **API无法启动**
   ```bash
   # 检查端口是否被占用
   netstat -tlnp | grep 8001
   
   # 更改端口
   python excel_to_text_api_simple.py  # 修改代码中的端口号
   ```

3. **Excel文件无法处理**
   - 确保文件没有密码保护
   - 检查文件格式是否正确
   - 验证文件没有损坏

## 📞 技术支持

如果您在使用过程中遇到问题：

1. 查看API日志输出
2. 检查Excel文件格式
3. 验证PDF API接口规范
4. 测试网络连接

## 📈 扩展功能

您可以根据需要扩展以下功能：

- **图片OCR** - 识别图片中的文字内容
- **表格智能识别** - 更复杂的表格结构分析
- **多格式支持** - 支持CSV、XML等格式
- **数据库存储** - 将结果存储到数据库
- **实时处理** - WebSocket支持实时处理状态

---

🎉 **Excel处理API现已就绪！** 

您现在可以轻松地将Excel文件转换为结构化文本，同时保持表格结构、图片位置和补充文本信息，并可选择性地集成您现有的PDF解析API。 
