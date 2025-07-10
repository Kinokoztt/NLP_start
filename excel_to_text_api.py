import asyncio
import io
import os
import tempfile
from typing import Dict, List, Optional, Any
import uuid
import json
from pathlib import Path

import httpx
import pandas as pd
from fastapi import FastAPI, File, UploadFile, HTTPException, Form
from fastapi.responses import JSONResponse
import openpyxl
from openpyxl.drawing.image import Image as OpenpyxlImage
from reportlab.lib.pagesizes import letter, A4
from reportlab.pdfgen import canvas
from reportlab.lib.units import inch
from reportlab.lib.styles import getSampleStyleSheet
from reportlab.platypus import SimpleDocTemplate, Paragraph, Table, TableStyle, Spacer, Image
from reportlab.lib import colors
from reportlab.lib.utils import ImageReader
from PIL import Image as PILImage

app = FastAPI(title="Excel to Text API", description="Convert Excel files to text using PDF parsing")

class ExcelProcessor:
    def __init__(self):
        self.temp_dir = tempfile.mkdtemp()
    
    async def process_excel_file(
        self, 
        file_content: bytes, 
        filename: str,
        pdf_api_url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        处理Excel文件，提取所有内容包括表格、图片和文本
        """
        result = {
            "filename": filename,
            "sheets": [],
            "images": [],
            "text_content": "",
            "structure_preserved": True
        }
        
        try:
            # 保存临时文件
            temp_excel_path = os.path.join(self.temp_dir, f"{uuid.uuid4()}.xlsx")
            with open(temp_excel_path, 'wb') as f:
                f.write(file_content)
            
            # 打开Excel文件
            workbook = openpyxl.load_workbook(temp_excel_path, data_only=False)
            
            # 处理每个sheet
            for sheet_name in workbook.sheetnames:
                sheet_data = await self._process_sheet(workbook[sheet_name], sheet_name)
                result["sheets"].append(sheet_data)
            
            # 将Excel转换为PDF
            pdf_path = await self._excel_to_pdf(temp_excel_path, workbook)
            
            # 如果提供了PDF API URL，调用现有的PDF解析API
            if pdf_api_url:
                pdf_text = await self._call_pdf_api(pdf_path, pdf_api_url)
                result["text_content"] = pdf_text
            else:
                # 如果没有提供API URL，使用自己的文本提取
                result["text_content"] = await self._extract_text_from_sheets(result["sheets"])
            
            # 清理临时文件
            os.unlink(temp_excel_path)
            if os.path.exists(pdf_path):
                os.unlink(pdf_path)
                
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"处理Excel文件时出错: {str(e)}")
        
        return result
    
    async def _process_sheet(self, sheet, sheet_name: str) -> Dict[str, Any]:
        """处理单个sheet"""
        sheet_data = {
            "name": sheet_name,
            "tables": [],
            "images": [],
            "text_regions": [],
            "merged_cells": []
        }
        
        # 获取所有数据
        data = []
        max_row = sheet.max_row
        max_col = sheet.max_column
        
        # 提取表格数据
        for row in range(1, max_row + 1):
            row_data = []
            for col in range(1, max_col + 1):
                cell = sheet.cell(row=row, column=col)
                cell_value = cell.value if cell.value is not None else ""
                row_data.append(str(cell_value))
            data.append(row_data)
        
        # 识别表格结构
        table_regions = self._identify_table_regions(data)
        sheet_data["tables"] = table_regions
        
        # 提取合并单元格信息
        for merged_range in sheet.merged_cells.ranges:
            sheet_data["merged_cells"].append({
                "range": str(merged_range),
                "start_row": merged_range.min_row,
                "end_row": merged_range.max_row,
                "start_col": merged_range.min_col,
                "end_col": merged_range.max_col
            })
        
        # 提取图片
        if hasattr(sheet, '_images'):
            for img in sheet._images:
                image_info = {
                    "position": {
                        "row": img.anchor._from.row if hasattr(img.anchor, '_from') else 0,
                        "col": img.anchor._from.col if hasattr(img.anchor, '_from') else 0
                    },
                    "size": {
                        "width": img.width if hasattr(img, 'width') else 0,
                        "height": img.height if hasattr(img, 'height') else 0
                    },
                    "description": f"图片位置: 行{img.anchor._from.row}, 列{img.anchor._from.col}" if hasattr(img.anchor, '_from') else "图片"
                }
                sheet_data["images"].append(image_info)
        
        return sheet_data
    
    def _identify_table_regions(self, data: List[List[str]]) -> List[Dict[str, Any]]:
        """识别表格区域"""
        tables = []
        
        # 简单的表格识别逻辑 - 寻找连续的非空行作为表格
        current_table = []
        table_start_row = 0
        
        for i, row in enumerate(data):
            non_empty_cells = [cell for cell in row if cell.strip()]
            
            if len(non_empty_cells) > 1:  # 认为是表格行
                if not current_table:
                    table_start_row = i
                current_table.append({
                    "row_index": i,
                    "data": row,
                    "non_empty_count": len(non_empty_cells)
                })
            else:
                if current_table and len(current_table) > 2:  # 至少3行才算表格
                    tables.append({
                        "start_row": table_start_row,
                        "end_row": i - 1,
                        "rows": current_table,
                        "structure": "table"
                    })
                current_table = []
        
        # 处理最后一个表格
        if current_table and len(current_table) > 2:
            tables.append({
                "start_row": table_start_row,
                "end_row": len(data) - 1,
                "rows": current_table,
                "structure": "table"
            })
        
        return tables
    
    async def _excel_to_pdf(self, excel_path: str, workbook) -> str:
        """将Excel转换为PDF"""
        pdf_path = os.path.join(self.temp_dir, f"{uuid.uuid4()}.pdf")
        
        try:
            # 创建PDF文档
            doc = SimpleDocTemplate(pdf_path, pagesize=A4)
            story = []
            styles = getSampleStyleSheet()
            
            # 处理每个sheet
            for sheet_name in workbook.sheetnames:
                sheet = workbook[sheet_name]
                
                # 添加Sheet标题
                title = Paragraph(f"<b>Sheet: {sheet_name}</b>", styles['Heading1'])
                story.append(title)
                story.append(Spacer(1, 12))
                
                # 提取数据并创建表格
                data = []
                max_row = min(sheet.max_row, 100)  # 限制行数避免PDF过大
                max_col = min(sheet.max_column, 20)  # 限制列数
                
                for row in range(1, max_row + 1):
                    row_data = []
                    for col in range(1, max_col + 1):
                        cell = sheet.cell(row=row, column=col)
                        cell_value = cell.value if cell.value is not None else ""
                        row_data.append(str(cell_value)[:50])  # 限制单元格内容长度
                    
                    # 只添加非空行
                    if any(cell.strip() for cell in row_data):
                        data.append(row_data)
                
                if data:
                    # 创建表格
                    table = Table(data)
                    table.setStyle(TableStyle([
                        ('BACKGROUND', (0, 0), (-1, 0), colors.grey),
                        ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                        ('ALIGN', (0, 0), (-1, -1), 'CENTER'),
                        ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                        ('FONTSIZE', (0, 0), (-1, 0), 10),
                        ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                        ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
                        ('GRID', (0, 0), (-1, -1), 1, colors.black)
                    ]))
                    story.append(table)
                
                story.append(Spacer(1, 20))
            
            # 构建PDF
            doc.build(story)
            
        except Exception as e:
            # 如果PDF创建失败，创建一个简单的文本PDF
            c = canvas.Canvas(pdf_path, pagesize=letter)
            c.drawString(100, 750, f"Excel文件处理: {os.path.basename(excel_path)}")
            c.drawString(100, 730, f"转换时间: {pd.Timestamp.now()}")
            c.drawString(100, 710, "包含表格数据和图片信息")
            c.save()
        
        return pdf_path
    
    async def _call_pdf_api(self, pdf_path: str, api_url: str) -> str:
        """调用现有的PDF解析API"""
        try:
            async with httpx.AsyncClient() as client:
                with open(pdf_path, 'rb') as f:
                    files = {'file': (os.path.basename(pdf_path), f, 'application/pdf')}
                    response = await client.post(api_url, files=files, timeout=60.0)
                    
                if response.status_code == 200:
                    result = response.json()
                    return result.get('text', result.get('content', str(result)))
                else:
                    return f"PDF API调用失败: {response.status_code}"
                    
        except Exception as e:
            return f"调用PDF API时出错: {str(e)}"
    
    async def _extract_text_from_sheets(self, sheets: List[Dict[str, Any]]) -> str:
        """从sheet数据中提取文本"""
        text_parts = []
        
        for sheet in sheets:
            text_parts.append(f"\n=== Sheet: {sheet['name']} ===\n")
            
            # 添加表格内容
            for table in sheet['tables']:
                text_parts.append(f"\n--- 表格 (行 {table['start_row']}-{table['end_row']}) ---\n")
                for row_info in table['rows']:
                    row_text = " | ".join([cell for cell in row_info['data'] if cell.strip()])
                    if row_text.strip():
                        text_parts.append(row_text)
                text_parts.append("\n")
            
            # 添加图片信息
            if sheet['images']:
                text_parts.append("\n--- 图片信息 ---\n")
                for img in sheet['images']:
                    text_parts.append(f"图片位置: 行{img['position']['row']}, 列{img['position']['col']}")
                    text_parts.append(f"尺寸: {img['size']['width']}x{img['size']['height']}")
                text_parts.append("\n")
            
            # 添加合并单元格信息
            if sheet['merged_cells']:
                text_parts.append("\n--- 合并单元格 ---\n")
                for merged in sheet['merged_cells']:
                    text_parts.append(f"合并区域: {merged['range']}")
        
        return "\n".join(text_parts)

# 创建处理器实例
processor = ExcelProcessor()

@app.post("/process-excel")
async def process_excel(
    file: UploadFile = File(...),
    pdf_api_url: Optional[str] = Form(None)
):
    """
    处理Excel文件，提取所有内容并可选择性地调用PDF解析API
    
    - file: Excel文件
    - pdf_api_url: 可选的PDF解析API URL
    """
    if not file.filename.endswith(('.xlsx', '.xls')):
        raise HTTPException(status_code=400, detail="只支持Excel文件 (.xlsx, .xls)")
    
    try:
        content = await file.read()
        result = await processor.process_excel_file(content, file.filename, pdf_api_url)
        return JSONResponse(content=result)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"处理文件时出错: {str(e)}")

@app.post("/batch-process-excel")
async def batch_process_excel(
    files: List[UploadFile] = File(...),
    pdf_api_url: Optional[str] = Form(None)
):
    """
    批量处理多个Excel文件
    """
    results = []
    
    for file in files:
        if not file.filename.endswith(('.xlsx', '.xls')):
            results.append({
                "filename": file.filename,
                "error": "不支持的文件格式"
            })
            continue
        
        try:
            content = await file.read()
            result = await processor.process_excel_file(content, file.filename, pdf_api_url)
            results.append(result)
        except Exception as e:
            results.append({
                "filename": file.filename,
                "error": str(e)
            })
    
    return JSONResponse(content={"results": results})

@app.get("/health")
async def health_check():
    """健康检查"""
    return {"status": "healthy", "service": "Excel to Text API"}

@app.get("/")
async def root():
    """API信息"""
    return {
        "service": "Excel to Text API",
        "description": "将Excel文件转换为文本，保持表格结构和位置信息",
        "endpoints": {
            "/process-excel": "处理单个Excel文件",
            "/batch-process-excel": "批量处理Excel文件",
            "/health": "健康检查"
        }
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)