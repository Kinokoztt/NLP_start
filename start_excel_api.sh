#!/bin/bash

echo "正在安装Excel处理API的依赖..."

# 安装Python依赖
pip install fastapi==0.104.1
pip install uvicorn[standard]==0.24.0
pip install openpyxl==3.1.2
pip install pandas==2.1.3
pip install xlsxwriter==3.1.9
pip install reportlab==4.0.7
pip install Pillow==10.1.0
pip install python-multipart==0.0.6
pip install aiofiles==23.2.1
pip install httpx==0.25.2

echo "依赖安装完成。"
echo "启动Excel处理API服务..."

# 启动API服务
python excel_to_text_api.py