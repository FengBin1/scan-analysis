import streamlit as st
import pandas as pd
import numpy as np
import os
import re
import io
import warnings
from openpyxl.styles import Font, Alignment, PatternFill, Border, Side, NamedStyle
from concurrent.futures import ThreadPoolExecutor, as_completed

# 过滤无关警告
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas")

# -------------------------- 页面基础设置 --------------------------
st.set_page_config(page_title="多科目扫描状态分析工具", page_icon="📊", layout="wide")

# -------------------------- 全局样式定义 --------------------------
header_style = NamedStyle(name="header_style")
header_style.font = Font(name='微软雅黑', size=11, bold=True, color='FFFFFF')
header_style.fill = PatternFill(start_color='4472C4', end_color='4472C4', fill_type='solid')
header_style.alignment = Alignment(horizontal='center', vertical='center')
header_style.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))

data_style = NamedStyle(name="data_style")
data_style.font = Font(name='微软雅黑', size=10)
data_style.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
data_style.alignment = Alignment(horizontal='left', vertical='center')

center_data_style = NamedStyle(name="center_data_style")
center_data_style.font = Font(name='微软雅黑', size=10)
center_data_style.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
center_data_style.alignment = Alignment(horizontal='center', vertical='center')

# -------------------------- 核心工具函数 --------------------------
def extract_subject_from_filename(filename):
    pattern = r'\((.*?)\)'
    match = re.search(pattern, filename)
    if match:
        subject = match.group(1).strip()
        non_subject = ['名单', '成绩', '数据', '统计', '考试', '期末', '期中']
        if not any(kw in subject for kw in non_subject):
            return subject
    base_name = os.path.splitext(filename)[0]
    return f"未知科目_{base_name[-4:]}" if len(base_name) >=4 else "未知科目"

def load_uploaded_file(uploaded_file):
    """适配 Streamlit 的内存文件读取"""
    try:
        target_cols = ['考号', '姓名', '学校', '班级', '学籍号', '扫描否', '扫描状态']
        df = pd.read_excel(
            uploaded_file, 
            sheet_name=0, 
            header=1, 
            usecols=lambda x: x in target_cols,
            engine='openpyxl'
        )
        
        subject = extract_subject_from_filename(uploaded_file.name)
        
        core_fields_map = {
            '考号': '考号', '姓名': '姓名', '学校': '学校', '班级': '班级',
            '学籍号': '学号', '扫描否': '扫描状态', '扫描状态': '扫描状态'
        }
        df = df.rename(columns={k: v for k, v in core_fields_map.items() if k in df.columns})
        
        required_cols = ['考号', '姓名', '学校', '班级', '学号', '扫描状态']
        for col in required_cols:
            if col not in df.columns:
                df[col] = '未记录'
        
        scan_status_map = {
            'True': '已扫', 'False': '未扫', '1': '已扫', '0': '未扫',
            '是': '已扫', '否': '未扫', '已扫': '已扫', '未扫': '未扫',
            '': '未扫', '未记录': '未记录'
        }
        df['扫描状态'] = df['扫描状态'].astype(str).str.strip().replace(scan_status_map).fillna('未记录')
        
        result_df = df[required_cols].assign(科目=subject)
        result_df = result_df.drop_duplicates(subset=['考号', '科目'], keep='first')
        
        for col in ['学校', '姓名', '班级', '学号', '扫描状态', '科目']:
            if result_df[col].nunique() / len(result_df) < 0.5:
                result_df[col] = result_df[col].astype('category')
        
        return result_df, subject
    except Exception as e:
        st.error(f"❌ 加载文件 {uploaded_file.name} 失败：{str(e)}")
        return None, None

def generate_student_list_data(all_merged_data):
    student_list = all_merged_data.groupby('考号').agg(
        姓名=('姓名', 'first'), 学校=('学校', 'first'), 班级=('班级', 'first'),
        科目=('科目', lambda x: '; '.join(x.unique()) if not x.empty else '无'),
        涉及科目数=('科目', 'nunique')
    ).reset_index()
    
    student_list = student_list[['考号', '姓名', '学校', '班级', '科目', '涉及科目数']]
    return student_list

def classify_scan_status(all_merged_data):
    status_summary = all_merged_data.groupby('考号').agg(
        学校=('学校', 'first'), 姓名=('姓名', 'first'), 学号=('学号', 'first'),
        班级=('班级', 'first'), 科目=('科目', lambda x: ', '.join(x.unique()) if not x.empty else '无')
    ).reset_index()
    
    is_scanned = all_merged_data['扫描状态'] == '已扫'
    is_unscanned = all_merged_data['扫描状态'] == '未扫'
    is_unrecorded = all_merged_data['扫描状态'] == '未记录'

    scanned_subj = all_merged_data[is_scanned].groupby('考号')['科目'].apply(lambda x: '; '.join(x)).rename('已扫科目')
    unscanned_subj = all_merged_data[is_unscanned].groupby('考号')['科目'].apply(lambda x: '; '.join(x)).rename('未扫_纯')
    unrecorded_subj = all_merged_data[is_unrecorded].groupby('考号')['科目'].apply(lambda x: '; '.join([f"{s}(未记录)" for s in x])).rename('未扫_记录')

    df_subj = pd.DataFrame(index=status_summary['考号'])
    df_subj = df_subj.join(scanned_subj).join(unscanned_subj).join(unrecorded_subj)
    df_subj['已扫科目'] = df_subj['已扫科目'].fillna('无')
    
    s1 = df_subj['未扫_纯'].fillna('')
    s2 = df_subj['未扫_记录'].fillna('')
    sep = np.where((s1 != '') & (s2 != ''), '; ', '')
    df_subj['未扫科目'] = (s1 + sep + s2).replace('', '无')

    status_summary = status_summary.merge(df_subj[['已扫科目', '未扫科目']], on='考号', how='left')

    counts = all_merged_data.pivot_table(index='考号', columns='扫描状态', values='科目', aggfunc='count', fill_value=0, observed=False)
    for col in ['已扫', '未扫', '未记录']:
        if col not in counts.columns: counts[col] = 0
            
    total_valid = counts['已扫'] + counts['未扫']
    conditions = [
        total_valid == 0,
        (counts['已扫'] > 0) & (counts['未扫'] == 0),
        (counts['未扫'] > 0) & (counts['已扫'] == 0)
    ]
    counts['扫描状态分类'] = np.select(conditions, ['状态未记录', '全已扫', '全未扫'], default='状态差异')
    status_summary = status_summary.merge(counts[['扫描状态分类']], on='考号', how='left')
    
    status_list = all_merged_data.groupby('考号')['扫描状态'].apply(lambda x: list(x.unique())).rename('扫描状态')
    status_summary = status_summary.merge(status_list, on='考号', how='left')
    status_summary['涉及科目数'] = status_summary['科目'].str.count(',') + 1
    
    final_cols = ['学校', '姓名', '学号', '班级', '科目', '涉及科目数', '已扫科目', '未扫科目', '扫描状态', '扫描状态分类']
    return {
        '全已扫': status_summary[status_summary['扫描状态分类'] == '全已扫'][final_cols],
        '全未扫': status_summary[status_summary['扫描状态分类'] == '全未扫'][final_cols],
        '状态差异': status_summary[status_summary['扫描状态分类'] == '状态差异'][final_cols],
        '状态未记录': status_summary[status_summary['扫描状态分类'] == '状态未记录'][final_cols]
    }

def create_scan_pivot_table(all_merged_data):
    pivot_data = all_merged_data.copy()
    pivot_data['是否已扫'] = pivot_data['扫描状态'] == '已扫'
    
    total_pivot = pd.pivot_table(pivot_data, index='学校', columns='科目', values='考号', aggfunc='count', fill_value=0, observed=False)
    scanned_pivot = pd.pivot_table(pivot_data[pivot_data['是否已扫']], index='学校', columns='科目', values='考号', aggfunc='count', fill_value=0, observed=False)
    
    all_schools = total_pivot.index.union(scanned_pivot.index)
    all_subjects = total_pivot.columns.union(scanned_pivot.columns)
    total_pivot = total_pivot.reindex(index=all_schools, columns=all_subjects, fill_value=0)
    scanned_pivot = scanned_pivot.reindex(index=all_schools, columns=all_subjects, fill_value=0)
    
    result_pivot = scanned_pivot.astype(str) + '/' + total_pivot.astype(str)
    pct_matrix = np.where(total_pivot > 0, (scanned_pivot / total_pivot) * 100, 0.0)
    percentage_pivot = pd.DataFrame(pct_matrix, index=all_schools, columns=all_subjects)
    map_func = getattr(percentage_pivot, 'map', percentage_pivot.applymap)
    percentage_pivot = map_func(lambda x: f"{x:.1f}%")
    
    row_scanned = scanned_pivot.sum(axis=1)
    row_total = total_pivot.sum(axis=1)
    result_pivot['总计'] = row_scanned.astype(str) + '/' + row_total.astype(str)
    row_pct = np.where(row_total > 0, (row_scanned / row_total) * 100, 0.0)
    percentage_pivot['总计'] = [f"{x:.1f}%" for x in row_pct]
    
    col_scanned = scanned_pivot.sum(axis=0)
    col_total = total_pivot.sum(axis=0)
    col_scanned['总计'] = col_scanned.sum()
    col_total['总计'] = col_total.sum()
    
    result_pivot.loc['总计'] = col_scanned.astype(str) + '/' + col_total.astype(str)
    col_pct = np.where(col_total > 0, (col_scanned / col_total) * 100, 0.0)
    percentage_pivot.loc['总计'] = [f"{x:.1f}%" for x in col_pct]
    
    result_pivot.index.name = '学校'
    percentage_pivot.index.name = '学校'
    return {'数量透视表': result_pivot, '百分比透视表': percentage_pivot}

def set_excel_cell_style_optimized(ws):
    for cell in ws[1]: cell.style = header_style
    max_row = ws.max_row
    max_col = ws.max_column
    headers = [cell.value for cell in ws[1]]
    
    center_cols_idx = {i for i, h in enumerate(headers) if h in ['涉及科目数', '扫描状态分类']}
    if '学校' in headers and max_row >= 2: center_cols_idx = set(range(max_col))
    
    for row in ws.iter_rows(min_row=2, max_row=max_row, min_col=1, max_col=max_col):
        for idx, cell in enumerate(row):
            cell.style = center_data_style if idx in center_cols_idx else data_style
    
    column_widths = {}
    for col in ws.columns:
        col_letter = col[0].column_letter
        header_val = str(col[0].value) if col[0].value else ""
        max_len = max([len(str(cell.value)) for cell in col[:100] if cell.value] or [0])
        
        if col_letter in ['E'] and '科目' in headers and headers.index('科目') == 4:
            column_widths[col_letter] = min(max_len + 4, 40)
        elif '总计' in header_val or '学校' in header_val:
            column_widths[col_letter] = min(max_len + 3, 20)
        else:
            column_widths[col_letter] = min(max_len + 3, 15)
            
    for col_letter, width in column_widths.items():
        ws.column_dimensions[col_letter].width = width

# -------------------------- Streamlit 网页前端逻辑 --------------------------
st.title("🚀 多科目考生扫描状态对比分析工具")
st.markdown("上传包含各科目扫描数据的 Excel 文件，一键生成合并分析报告。**（数据仅在内存中处理，不保存，绝对安全）**")

uploaded_files = st.file_uploader("请选择所有需要对比的科目 Excel 文件（至少2个）", type=['xlsx', 'xls'], accept_multiple_files=True)

if st.button("开始极速分析", type="primary"):
    if len(uploaded_files) < 2:
        st.warning("⚠️ 至少需要上传 2 个 Excel 文件才能进行对比！")
    else:
        with st.spinner('🚀 正在多线程读取数据...'):
            merged_data = []
            all_subjects = []
            
            with ThreadPoolExecutor(max_workers=min(len(uploaded_files), 8)) as executor:
                futures = {executor.submit(load_uploaded_file, f): f for f in uploaded_files}
                for future in as_completed(futures):
                    file_data, subject = future.result()
                    if file_data is not None and not file_data.empty:
                        merged_data.append(file_data)
                        all_subjects.append(subject)

            if not merged_data:
                st.error("❌ 未读取到有效数据，请检查文件格式。")
                st.stop()

            all_merged = pd.concat(merged_data, ignore_index=True)
            total_students = all_merged['考号'].nunique()
            
        st.success(f"✅ 数据合并完成！共涵盖 **{len(all_subjects)}** 个科目，**{total_students}** 名考生。")

        with st.spinner('📊 正在进行矩阵透视计算与分类...'):
            student_list_data = generate_student_list_data(all_merged)
            pivot_tables = create_scan_pivot_table(all_merged)
            classification_result = classify_scan_status(all_merged)

        with st.spinner('💾 正在生成精美的 Excel 报告...'):
            # 使用内存缓冲区 BytesIO 代替本地文件
            output = io.BytesIO()
            with pd.ExcelWriter(output, engine='openpyxl') as writer:
                student_list_data.to_excel(writer, sheet_name='名单数据', index=False)
                set_excel_cell_style_optimized(writer.sheets['名单数据'])
                
                pivot_tables['数量透视表'].to_excel(writer, sheet_name='扫描数量透视表')
                set_excel_cell_style_optimized(writer.sheets['扫描数量透视表'])
                
                pivot_tables['百分比透视表'].to_excel(writer, sheet_name='扫描百分比透视表')
                set_excel_cell_style_optimized(writer.sheets['扫描百分比透视表'])
                
                for sheet_name, df in classification_result.items():
                    if not df.empty:
                        df.to_excel(writer, sheet_name=sheet_name, index=False)
                        set_excel_cell_style_optimized(writer.sheets[sheet_name])
                        
            processed_data = output.getvalue()

        st.balloons()
        st.subheader("🎉 分析完成！核心结果预览：")
        col1, col2, col3 = st.columns(3)
        col1.metric("✅ 全已扫人数", f"{len(classification_result['全已扫'])} 人")
        col2.metric("❌ 全未扫人数", f"{len(classification_result['全未扫'])} 人")
        col3.metric("⚠️ 状态差异人数", f"{len(classification_result['状态差异'])} 人")

        st.download_button(
            label="📥 点击下载完整 Excel 分析报告",
            data=processed_data,
            file_name="多科目扫描状态对比分析.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            type="primary",
            use_container_width=True

        )
