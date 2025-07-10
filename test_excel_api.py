#!/usr/bin/env python3
"""
Excel处理API测试脚本
"""

import asyncio
import os
import requests
import json
from pathlib import Path
import openpyxl
from openpyxl.styles import Font, PatternFill
from openpyxl.drawing.image import Image
from PIL import Image as PILImage
import tempfile

def create_test_excel():
    """创建一个测试用的Excel文件"""
    wb = openpyxl.Workbook()
    
    # Sheet 1 - 主数据表
    ws1 = wb.active
    ws1.title = "主数据表"
    
    # 添加表头
    headers = ['序号', '姓名', '年龄', '部门', '工资', '备注']
    for col, header in enumerate(headers, 1):
        cell = ws1.cell(row=1, column=col, value=header)
        cell.font = Font(bold=True)
        cell.fill = PatternFill(start_color="CCCCCC", end_color="CCCCCC", fill_type="solid")
    
    # 添加数据
    data = [
        [1, '张三', 28, '技术部', 8000, '高级工程师'],
        [2, '李四', 32, '市场部', 7500, '市场经理'],
        [3, '王五', 25, '技术部', 6500, '初级工程师'],
        [4, '赵六', 30, '人事部', 7000, '人事专员'],
        [5, '钱七', 35, '财务部', 8500, '财务经理']
    ]
    
    for row_idx, row_data in enumerate(data, 2):
        for col_idx, value in enumerate(row_data, 1):
            ws1.cell(row=row_idx, column=col_idx, value=value)
    
    # 合并一些单元格作为标题
    ws1.merge_cells('A8:F8')
    ws1['A8'] = '部门统计信息'
    ws1['A8'].font = Font(bold=True, size=14)
    
    # 添加部门统计表
    dept_headers = ['部门', '人数', '平均工资']
    for col, header in enumerate(dept_headers, 1):
        ws1.cell(row=9, column=col, value=header)
    
    dept_data = [
        ['技术部', 2, 7250],
        ['市场部', 1, 7500],
        ['人事部', 1, 7000],
        ['财务部', 1, 8500]
    ]
    
    for row_idx, row_data in enumerate(dept_data, 10):
        for col_idx, value in enumerate(row_data, 1):
            ws1.cell(row=row_idx, column=col_idx, value=value)
    
    # 添加说明文本
    ws1['A15'] = '备注: 此表格包含员工基本信息和部门统计'
    ws1['A16'] = '数据更新时间: 2024年1月'
    
    # Sheet 2 - 图表数据
    ws2 = wb.create_sheet("图表数据")
    
    # 月度销售数据
    ws2['A1'] = '月度销售数据'
    ws2['A1'].font = Font(bold=True, size=16)
    
    months = ['1月', '2月', '3月', '4月', '5月', '6月']
    sales = [120, 135, 128, 142, 158, 163]
    
    ws2['A3'] = '月份'
    ws2['B3'] = '销售额(万元)'
    
    for i, (month, sale) in enumerate(zip(months, sales), 4):
        ws2[f'A{i}'] = month
        ws2[f'B{i}'] = sale
    
    # 添加总计
    ws2['A10'] = '总计'
    ws2['B10'] = f'=SUM(B4:B9)'
    ws2['B10'].font = Font(bold=True)
    
    # 保存文件
    temp_dir = tempfile.gettempdir()
    excel_path = os.path.join(temp_dir, 'test_excel_file.xlsx')
    wb.save(excel_path)
    
    return excel_path

def test_api_health(base_url):
    """测试API健康状况"""
    try:
        response = requests.get(f"{base_url}/health", timeout=10)
        if response.status_code == 200:
            print("✅ API健康检查通过")
            return True
        else:
            print(f"❌ API健康检查失败: {response.status_code}")
            return False
    except Exception as e:
        print(f"❌ 无法连接到API: {e}")
        return False

def test_process_excel(base_url, excel_path, pdf_api_url=None):
    """测试Excel文件处理"""
    print("\n📊 测试Excel文件处理...")
    
    try:
        with open(excel_path, 'rb') as f:
            files = {'file': (os.path.basename(excel_path), f, 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')}
            data = {}
            if pdf_api_url:
                data['pdf_api_url'] = pdf_api_url
            
            response = requests.post(
                f"{base_url}/process-excel",
                files=files,
                data=data,
                timeout=30
            )
        
        if response.status_code == 200:
            result = response.json()
            print("✅ Excel文件处理成功")
            
            # 显示处理结果摘要
            print(f"📁 文件名: {result['filename']}")
            print(f"📄 Sheet数量: {len(result['sheets'])}")
            
            for i, sheet in enumerate(result['sheets']):
                print(f"  Sheet {i+1}: {sheet['name']}")
                print(f"    📋 表格数量: {len(sheet['tables'])}")
                print(f"    🖼️  图片数量: {len(sheet['images'])}")
                print(f"    🔗 合并单元格: {len(sheet['merged_cells'])}")
            
            # 显示部分文本内容
            text_content = result['text_content']
            if len(text_content) > 500:
                print(f"📝 文本内容预览:\n{text_content[:500]}...")
            else:
                print(f"📝 文本内容:\n{text_content}")
            
            return True
            
        else:
            print(f"❌ Excel文件处理失败: {response.status_code}")
            print(f"错误信息: {response.text}")
            return False
            
    except Exception as e:
        print(f"❌ 处理Excel文件时出错: {e}")
        return False

def test_batch_process(base_url, excel_paths, pdf_api_url=None):
    """测试批量处理"""
    print("\n📚 测试批量处理...")
    
    try:
        files = []
        for path in excel_paths:
            files.append(('files', (os.path.basename(path), open(path, 'rb'), 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')))
        
        data = {}
        if pdf_api_url:
            data['pdf_api_url'] = pdf_api_url
        
        response = requests.post(
            f"{base_url}/batch-process-excel",
            files=files,
            data=data,
            timeout=60
        )
        
        # 关闭文件
        for _, file_tuple in files:
            file_tuple[1].close()
        
        if response.status_code == 200:
            results = response.json()
            print("✅ 批量处理成功")
            print(f"📁 处理文件数量: {len(results['results'])}")
            
            for i, result in enumerate(results['results']):
                if 'error' in result:
                    print(f"  文件 {i+1}: ❌ {result['filename']} - {result['error']}")
                else:
                    print(f"  文件 {i+1}: ✅ {result['filename']} - {len(result['sheets'])} sheets")
            
            return True
        else:
            print(f"❌ 批量处理失败: {response.status_code}")
            return False
            
    except Exception as e:
        print(f"❌ 批量处理时出错: {e}")
        return False

def main():
    """主测试函数"""
    print("🚀 开始测试Excel处理API")
    print("=" * 50)
    
    # API配置
    base_url = "http://localhost:8001"
    pdf_api_url = None  # 如果有PDF API，在这里设置URL
    
    # 创建测试文件
    print("📄 创建测试Excel文件...")
    excel_path = create_test_excel()
    print(f"✅ 测试文件创建完成: {excel_path}")
    
    # 测试API健康状况
    if not test_api_health(base_url):
        print("\n❌ API服务不可用，请先启动服务")
        print("运行命令: python excel_to_text_api.py")
        return
    
    # 测试单文件处理
    success1 = test_process_excel(base_url, excel_path, pdf_api_url)
    
    # 测试批量处理（使用同一个文件多次）
    success2 = test_batch_process(base_url, [excel_path], pdf_api_url)
    
    # 清理测试文件
    try:
        os.unlink(excel_path)
        print("\n🧹 清理测试文件完成")
    except:
        pass
    
    # 测试结果
    print("\n" + "=" * 50)
    print("📊 测试结果汇总:")
    print(f"  单文件处理: {'✅ 通过' if success1 else '❌ 失败'}")
    print(f"  批量处理: {'✅ 通过' if success2 else '❌ 失败'}")
    
    if success1 and success2:
        print("\n🎉 所有测试通过！API工作正常")
    else:
        print("\n⚠️ 部分测试失败，请检查API服务")

if __name__ == "__main__":
    main()