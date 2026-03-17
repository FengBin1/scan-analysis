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

# -------------------------- 全局常量 & 样式定义 --------------------------
SUBJECT_CODE_MAP = {
    '01': '语文', '02': '数学', '03': '英语', '04': '物理', '05': '化学',
    '06': '生物', '07': '地理', '08': '政治', '09': '历史', '10': '理综',
    '11': '文综', '12': '美术', '13': '音乐', '14': '思想品德', '15': '信息技术',
    '16': '通用技术', '17': '技术', '18': '社会', '19': '科学', '20': '道德与法治',
    '21': '生物地理', '22': '综合', '23': '物理化学', '24': '计算机',
    '25': '历史与社会', '26': '体育'
}

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

# -------------------------- 页面基础设置 & 状态初始化 --------------------------
st.set_page_config(page_title="多科目异常追踪系统", page_icon="🎯", layout="wide")

if 'analysis_completed' not in st.session_state:
    st.session_state.analysis_completed = False
    st.session_state.excel_bytes = None
    st.session_state.report_sheets = {}
    st.session_state.diff_df = pd.DataFrame()
    st.session_state.img_mapping = {}
    st.session_state.enable_viewer = False

# -------------------------- 核心工具函数 --------------------------
def extract_subject_from_filename(filename):
    pattern = r'\((.*?)\)'
    match = re.search(pattern, filename)
    if match:
        subject = match.group(1).strip()
        non_subject = ['名单', '成绩', '数据', '统计', '考试', '期末', '期中']
        if not any(kw in subject for kw in non_subject): return subject
    base_name = os.path.splitext(filename)[0]
    return f"未知科目_{base_name[-4:]}" if len(base_name) >=4 else "未知科目"

def load_uploaded_file(uploaded_file):
    try:
        target_cols = ['考号', '姓名', '学校', '班级', '学籍号', '扫描否', '扫描状态']
        df = pd.read_excel(uploaded_file, sheet_name=0, header=1, usecols=lambda x: x in target_cols, engine='openpyxl')
        subject = extract_subject_from_filename(uploaded_file.name)
        
        core_fields_map = {'考号': '考号', '姓名': '姓名', '学校': '学校', '班级': '班级', '学籍号': '学号', '扫描否': '扫描状态', '扫描状态': '扫描状态'}
        df = df.rename(columns={k: v for k, v in core_fields_map.items() if k in df.columns})
        
        required_cols = ['考号', '姓名', '学校', '班级', '学号', '扫描状态']
        for col in required_cols:
            if col not in df.columns: df[col] = '未记录'
        
        scan_status_map = {'True': '已扫', 'False': '未扫', '1': '已扫', '0': '未扫', '是': '已扫', '否': '未扫', '已扫': '已扫', '未扫': '未扫', '': '未扫', '未记录': '未记录'}
        df['扫描状态'] = df['扫描状态'].astype(str).str.strip().replace(scan_status_map).fillna('未记录')
        
        result_df = df[required_cols].assign(科目=subject).drop_duplicates(subset=['考号', '科目'], keep='first')
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
    return student_list[['考号', '姓名', '学校', '班级', '科目', '涉及科目数']]

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

    df_subj = pd.DataFrame(index=status_summary['考号']).join(scanned_subj).join(unscanned_subj).join(unrecorded_subj)
    df_subj['已扫科目'] = df_subj['已扫科目'].fillna('无')
    s1, s2 = df_subj['未扫_纯'].fillna(''), df_subj['未扫_记录'].fillna('')
    sep = np.where((s1 != '') & (s2 != ''), '; ', '')
    df_subj['未扫科目'] = (s1 + sep + s2).replace('', '无')

    status_summary = status_summary.merge(df_subj[['已扫科目', '未扫科目']], on='考号', how='left')

    counts = all_merged_data.pivot_table(index='考号', columns='扫描状态', values='科目', aggfunc='count', fill_value=0, observed=False)
    for col in ['已扫', '未扫', '未记录']:
        if col not in counts.columns: counts[col] = 0
            
    total_valid = counts['已扫'] + counts['未扫']
    conditions = [total_valid == 0, (counts['已扫'] > 0) & (counts['未扫'] == 0), (counts['未扫'] > 0) & (counts['已扫'] == 0)]
    counts['扫描状态分类'] = np.select(conditions, ['状态未记录', '全已扫', '全未扫'], default='状态差异')
    status_summary = status_summary.merge(counts[['扫描状态分类']], on='考号', how='left')
    status_summary = status_summary.merge(all_merged_data.groupby('考号')['扫描状态'].apply(lambda x: list(x.unique())).rename('扫描状态'), on='考号', how='left')
    status_summary['涉及科目数'] = status_summary['科目'].str.count(',') + 1
    
    final_cols = ['考号', '学校', '姓名', '学号', '班级', '科目', '涉及科目数', '已扫科目', '未扫科目', '扫描状态', '扫描状态分类']
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
    
    all_schools, all_subjects = total_pivot.index.union(scanned_pivot.index), total_pivot.columns.union(scanned_pivot.columns)
    total_pivot, scanned_pivot = total_pivot.reindex(index=all_schools, columns=all_subjects, fill_value=0), scanned_pivot.reindex(index=all_schools, columns=all_subjects, fill_value=0)
    
    result_pivot = scanned_pivot.astype(str) + '/' + total_pivot.astype(str)
    percentage_pivot = pd.DataFrame(np.where(total_pivot > 0, (scanned_pivot / total_pivot) * 100, 0.0), index=all_schools, columns=all_subjects).applymap(lambda x: f"{x:.1f}%")
    
    row_scanned, row_total = scanned_pivot.sum(axis=1), total_pivot.sum(axis=1)
    result_pivot['总计'] = row_scanned.astype(str) + '/' + row_total.astype(str)
    percentage_pivot['总计'] = [f"{x:.1f}%" for x in np.where(row_total > 0, (row_scanned / row_total) * 100, 0.0)]
    
    col_scanned, col_total = scanned_pivot.sum(axis=0), total_pivot.sum(axis=0)
    col_scanned['总计'], col_total['总计'] = col_scanned.sum(), col_total.sum()
    
    result_pivot.loc['总计'] = col_scanned.astype(str) + '/' + col_total.astype(str)
    percentage_pivot.loc['总计'] = [f"{x:.1f}%" for x in np.where(col_total > 0, (col_scanned / col_total) * 100, 0.0)]
    
    result_pivot = result_pivot.rename_axis('学校').reset_index()
    percentage_pivot = percentage_pivot.rename_axis('学校').reset_index()
    
    return {'数量透视表': result_pivot, '百分比透视表': percentage_pivot}

def parse_txt_mappings(txt_files):
    mapping = {}
    for f in txt_files:
        content = f.getvalue().decode('utf-8', errors='ignore').splitlines()
        file_subject_code = None
        for line in content:
            if '\t' in line:
                local_path = line.split('\t')[1].strip().replace('/', '\\')
                parts = local_path.split('\\')
                if len(parts) >= 3:
                    file_subject_code = parts[-3][-2:] 
                    break
        if not file_subject_code: continue
        subject_name = SUBJECT_CODE_MAP.get(file_subject_code, f"未知科目_{file_subject_code}")
        
        for line in content:
            if '\t' not in line: continue
            url, local_path = line.split('\t', 1)
            url = url.strip().strip('"').strip("'")
            local_path = local_path.strip().replace('/', '\\')
            parts = local_path.split('\\')
            if len(parts) >= 3 and '(1)' in parts[-1]: 
                student_dir = parts[-2]
                student_id = student_dir.split('(')[0] 
                mapping[(student_id, subject_name)] = url
    return mapping

# -------------------------- 统一 Excel 生成器 --------------------------
def set_excel_cell_style_optimized(ws):
    for cell in ws[1]: cell.style = header_style
    max_row, max_col, headers = ws.max_row, ws.max_column, [cell.value for cell in ws[1]]
    center_cols_idx = {i for i, h in enumerate(headers) if h in ['涉及科目数', '扫描状态分类', '考号', '学号', '处理进度']}
    if '学校' in headers and max_row >= 2: center_cols_idx = set(range(max_col))
    
    for row in ws.iter_rows(min_row=2, max_row=max_row, min_col=1, max_col=max_col):
        for idx, cell in enumerate(row):
            if headers[idx] == '处理进度' and cell.value == '已标记':
                cell.font = Font(name='微软雅黑', size=10, color='008000', bold=True)
                cell.alignment = Alignment(horizontal='center', vertical='center')
                cell.border = Border(left=Side(style='thin'), right=Side(style='thin'), top=Side(style='thin'), bottom=Side(style='thin'))
            else:
                cell.style = center_data_style if idx in center_cols_idx else data_style
    
    for col in ws.columns:
        col_letter, header_val = col[0].column_letter, str(col[0].value) if col[0].value else ""
        max_len = max([len(str(cell.value)) for cell in col[:100] if cell.value] or [0])
        width = min(max_len + 4, 40) if '科目' in header_val and '数' not in header_val else min(max_len + 3, 20) if '总计' in header_val or '学校' in header_val else min(max_len + 3, 15)
        ws.column_dimensions[col_letter].width = width

def generate_latest_excel():
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in st.session_state.report_sheets.items():
            if sheet_name == '状态差异': df = st.session_state.diff_df
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            set_excel_cell_style_optimized(writer.sheets[sheet_name])
    st.session_state.excel_bytes = output.getvalue()

# -------------------------- Streamlit 网页前端逻辑 --------------------------
st.title("🎯 多科目异常试卷追踪与标记系统")
st.markdown("不仅能找出扫描差异，更能**永久记忆您的处理标记**！每次上传最新数据，系统都会自动继承历史备注，未考/免考人员不再重复核对。")

col_a, col_b, col_c = st.columns(3)
with col_a:
    raw_files = st.file_uploader("1️⃣ 必填：上传最新各科 Excel 数据", type=['xlsx', 'xls'], accept_multiple_files=True, help="系统永远以这里的最新数据为准进行比对。")
with col_b:
    history_file = st.file_uploader("2️⃣ 可选：上传上次导出的本系统标记报告", type=['xlsx', 'xls'], accept_multiple_files=False, help="若上传，系统会自动读取上次标记的备注和进度，精准继承给这次的新数据。")
with col_c:
    txt_files = st.file_uploader("3️⃣ 可选：上传图片映射 TXT", type=['txt'], accept_multiple_files=True, help="不传则只进行纯文字标记；上传后可在面板中在线查阅试卷原图。")

if st.button("🚀 开始极速分析与继承标记", type="primary", use_container_width=True):
    if not raw_files or len(raw_files) < 2:
        st.warning("⚠️ 必须至少上传 2 个最新科目的 Excel 文件才能进行对比！")
        st.stop()

    with st.spinner('🚀 正在合并最新数据并继承历史记忆...'):
        st.session_state.report_sheets = {}
        
        # 1. 解析最新原始数据
        merged_data = []
        with ThreadPoolExecutor(max_workers=min(len(raw_files), 8)) as executor:
            futures = {executor.submit(load_uploaded_file, f): f for f in raw_files}
            for future in as_completed(futures):
                file_data, subject = future.result()
                if file_data is not None and not file_data.empty: merged_data.append(file_data)

        all_merged = pd.concat(merged_data, ignore_index=True)
        student_list_data = generate_student_list_data(all_merged)
        pivot_tables = create_scan_pivot_table(all_merged)
        classification_result = classify_scan_status(all_merged)
        
        st.session_state.report_sheets['名单数据'] = student_list_data
        st.session_state.report_sheets['扫描数量透视表'] = pivot_tables['数量透视表']
        st.session_state.report_sheets['扫描百分比透视表'] = pivot_tables['百分比透视表']
        
        # 2. 提取最新状态差异，并预埋处理字段
        new_diff_df = classification_result['状态差异'].copy()
        if not new_diff_df.empty:
            new_diff_df.insert(0, '处理进度', '未处理')
            new_diff_df.insert(1, '处理备注', '')
        
        # 3. 核心：如果上传了历史表格，自动提取并覆盖继承
        inherited_count = 0
        if history_file and not new_diff_df.empty:
            try:
                hist_df = pd.read_excel(history_file, sheet_name='状态差异')
                if '考号' in hist_df.columns and '处理进度' in hist_df.columns:
                    # 构建记忆字典 { "198060001": "已标记" }
                    hist_status = hist_df.set_index(hist_df['考号'].astype(str))['处理进度'].to_dict()
                    hist_remark = hist_df.set_index(hist_df['考号'].astype(str))['处理备注'].to_dict() if '处理备注' in hist_df.columns else {}
                    
                    def apply_status(row):
                        sid = str(row['考号'])
                        return hist_status.get(sid, '未处理')
                        
                    def apply_remark(row):
                        sid = str(row['考号'])
                        # 只有在存在并且不是 nan 时才继承
                        rem = hist_remark.get(sid, '')
                        return rem if pd.notna(rem) else ''
                        
                    new_diff_df['处理进度'] = new_diff_df.apply(apply_status, axis=1)
                    new_diff_df['处理备注'] = new_diff_df.apply(apply_remark, axis=1)
                    inherited_count = len(new_diff_df[new_diff_df['处理进度'] == '已标记'])
            except Exception as e:
                st.warning(f"⚠️ 提取历史继承数据时出错: {e}")

        for sheet_name, df in classification_result.items():
            if sheet_name == '状态差异':
                st.session_state.report_sheets[sheet_name] = new_diff_df
            elif not df.empty:
                st.session_state.report_sheets[sheet_name] = df
                
        st.session_state.diff_df = new_diff_df
        
        # 4. 解析图片（可选）
        if txt_files:
            st.session_state.img_mapping = parse_txt_mappings(txt_files)
            st.session_state.enable_viewer = True
        else:
            st.session_state.enable_viewer = False
            st.session_state.img_mapping = {}

        st.session_state.analysis_completed = True
        generate_latest_excel()
        
        if inherited_count > 0:
            st.toast(f"🎉 成功从历史文件中继承了 {inherited_count} 名考生的标记记录！")

# -------------------------- 沉浸式处理工作台 --------------------------
if st.session_state.analysis_completed:
    st.divider()
    diff_df = st.session_state.diff_df
    
    if diff_df.empty:
        st.success("🎉 太棒了！本次最新数据中没有任何状态差异的考生。")
        st.stop()
        
    total_diff = len(diff_df)
    processed_count = len(diff_df[diff_df['处理进度'] == '已标记'])
    pending_count = total_diff - processed_count
    
    st.header("💻 异常名单在线标记台")
    col1, col2, col3 = st.columns(3)
    col1.metric("⚠️ 差异大名单人数", f"{total_diff} 人")
    col2.metric("✅ 已知情 / 已标记", f"{processed_count} 人")
    col3.metric("⏳ 尚未查明待处理", f"{pending_count} 人")
    
    st.download_button(
        label="💾 下载最新进度 Excel 报告 (处理完随时可点，作为下次的继承凭证)",
        data=st.session_state.excel_bytes,
        file_name="多科目状态追踪与处理报告.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True
    )
    st.markdown("---")

    # 构造下拉选项
    options = []
    for _, row in diff_df.iterrows():
        status_icon = "🟢 [已标记]" if str(row.get('处理进度')) == '已标记' else "🔴 [未处理]"
        options.append(f"{status_icon} {row['考号']} | {row['姓名']} | {row['学校']} {row['班级']}")
    
    selected_option = st.selectbox("🔍 搜索或选择考生进行标记（下拉可见历史继承状态）：", options, index=0)
    
    if selected_option:
        student_id = selected_option.split(" | ")[0].split("] ")[1].strip()
        student_name = selected_option.split(" | ")[1].strip()
        
        matched_rows = diff_df[diff_df['考号'].astype(str) == student_id]
        
        if not matched_rows.empty:
            row_data = matched_rows.iloc[0]
            current_status = str(row_data.get('处理进度', '未处理'))
            current_remark = str(row_data.get('处理备注', ''))
            scanned_subjs = str(row_data['已扫科目']).split('; ')
            unscanned_subjs = str(row_data['未扫科目'])
            
            view_col, action_col = st.columns([6, 4])
            
            with view_col:
                st.markdown(f"**当前查验：** `{student_name} ({student_id})` 　|　 ❌ **差异科目：** `{unscanned_subjs}`")
                
                # 如果没上传 TXT，就不渲染图片模块
                if not st.session_state.enable_viewer:
                    st.info("ℹ️ 未上传图片映射 TXT，无法预览试卷。如已知晓原因（如缺考），请直接在右侧填写备注并标记。")
                else:
                    valid_images = []
                    for subj in scanned_subjs:
                        url = st.session_state.img_mapping.get((student_id, subj))
                        if url: valid_images.append((subj, url))
                    
                    if valid_images:
                        img_cols = st.columns(len(valid_images) if len(valid_images) <= 2 else 2)
                        for idx, (subj, url) in enumerate(valid_images):
                            with img_cols[idx % 2]:
                                st.markdown(f"📄 **{subj}**")
                                html_img = f'''
                                <a href="{url}" target="_blank">
                                    <img src="{url}" style="width:100%; border-radius:4px; border:1px solid #ccc;"/>
                                </a>
                                '''
                                st.markdown(html_img, unsafe_allow_html=True)
                    else:
                        st.warning("⚠️ 暂无匹配的试卷第一页图片。")
            
            with action_col:
                with st.form(key=f"form_{student_id}"):
                    st.subheader("📝 添加状态与备注")
                    new_status = st.radio("当前状态：", ["未处理", "已标记"], index=0 if current_status == '未处理' else 1)
                    new_remark = st.text_input("差异原因 (必填/选填)：", value=current_remark if current_remark != 'nan' else '', placeholder="如：语文缺考、试卷污染无法扫描等...")
                    
                    if st.form_submit_button("✅ 确认保存", type="primary", use_container_width=True):
                        idx_to_update = st.session_state.diff_df.index[st.session_state.diff_df['考号'].astype(str) == student_id].tolist()
                        if idx_to_update:
                            st.session_state.diff_df.at[idx_to_update[0], '处理进度'] = new_status
                            st.session_state.diff_df.at[idx_to_update[0], '处理备注'] = new_remark
                            generate_latest_excel()
                            st.toast(f"✅ {student_name} 的标记结果已永久保存！")
                            st.rerun()
