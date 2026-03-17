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
st.set_page_config(page_title="多科目异常试卷追踪系统", page_icon="🎯", layout="wide")

if 'analysis_completed' not in st.session_state:
    st.session_state.analysis_completed = False
    st.session_state.excel_bytes = None
    st.session_state.report_sheets = {}  # 统一存放所有 Sheet 数据
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
    
    # 强制将 index (学校) 转为普通列，方便统一无损读写
    result_pivot = result_pivot.rename_axis('学校').reset_index()
    percentage_pivot = percentage_pivot.rename_axis('学校').reset_index()
    
    return {'数量透视表': result_pivot, '百分比透视表': percentage_pivot}

def parse_txt_mappings(txt_files):
    mapping = {}
    processed_subjects = set()
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
        if file_subject_code in processed_subjects: continue
        processed_subjects.add(file_subject_code)
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
            # 高亮处理状态
            if headers[idx] == '处理进度' and cell.value == '已核对':
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
    """根据最新的 st.session_state.report_sheets 和 diff_df 动态生成 Excel"""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        for sheet_name, df in st.session_state.report_sheets.items():
            # 始终使用最新鲜的差异表
            if sheet_name == '状态差异':
                df = st.session_state.diff_df
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            set_excel_cell_style_optimized(writer.sheets[sheet_name])
    st.session_state.excel_bytes = output.getvalue()

# -------------------------- Streamlit 网页前端逻辑 --------------------------
st.title("🎯 多科目异常试卷追踪系统")
st.markdown("不仅能一键合并扫描数据，更能**实时标记异常原因、随时导出进度、支持断点续传处理**！")

# 【核心架构拓展】：工作模式选择
work_mode = st.radio("⚙️ 请选择当前工作模式：", 
                     ["1. 🆕 全新分析（上传原始多科目Excel进行比对）", 
                      "2. 🔄 继续处理（上传已导出的本系统Excel报告继续标记）"], 
                     horizontal=True)

with st.container(border=True):
    if work_mode.startswith("1"):
        uploaded_files = st.file_uploader("📂 [步骤 1] 请上传所有需要对比的科目 Excel 文件（至少2个）", type=['xlsx', 'xls'], accept_multiple_files=True)
    else:
        uploaded_files = st.file_uploader("📂 [步骤 1] 请上传上次从本系统下载的带处理进度的 Excel 报告", type=['xlsx', 'xls'], accept_multiple_files=False)
    
    st.divider()
    st.info("💡 [步骤 2] (必选) 请上传阿里云 OSS 图片映射的 TXT 文件，用于在线核对图片。")
    txt_files = st.file_uploader("📂 请上传科目图片映射 TXT 文件", type=['txt'], accept_multiple_files=True)

# 动作按钮
if st.button("🚀 加载数据并开启工作台", type="primary", use_container_width=True):
    if work_mode.startswith("1") and (not uploaded_files or len(uploaded_files) < 2):
        st.warning("⚠️ 全新分析模式下，至少需要上传 2 个 Excel 文件！")
        st.stop()
    elif work_mode.startswith("2") and not uploaded_files:
        st.warning("⚠️ 继续处理模式下，请上传一个历史生成的 Excel 报告！")
        st.stop()
        
    if not txt_files:
        st.warning("⚠️ 必须上传 TXT 文件，才能开启在线核对看板！")
        st.stop()

    with st.spinner('🚀 正在解析数据并构建工作流...'):
        st.session_state.report_sheets = {}
        
        # ================= 模式一：全新分析 =================
        if work_mode.startswith("1"):
            merged_data = []
            with ThreadPoolExecutor(max_workers=min(len(uploaded_files), 8)) as executor:
                futures = {executor.submit(load_uploaded_file, f): f for f in uploaded_files}
                for future in as_completed(futures):
                    file_data, subject = future.result()
                    if file_data is not None and not file_data.empty: merged_data.append(file_data)

            if not merged_data:
                st.error("❌ 未读取到有效数据，请检查文件格式。")
                st.stop()

            all_merged = pd.concat(merged_data, ignore_index=True)
            student_list_data = generate_student_list_data(all_merged)
            pivot_tables = create_scan_pivot_table(all_merged)
            classification_result = classify_scan_status(all_merged)
            
            # 存储基础表
            st.session_state.report_sheets['名单数据'] = student_list_data
            st.session_state.report_sheets['扫描数量透视表'] = pivot_tables['数量透视表']
            st.session_state.report_sheets['扫描百分比透视表'] = pivot_tables['百分比透视表']
            
            for sheet_name, df in classification_result.items():
                if not df.empty:
                    # 【关键】为差异表增加处理标记列
                    if sheet_name == '状态差异':
                        df.insert(0, '处理进度', '未处理')
                        df.insert(1, '处理备注', '')
                    st.session_state.report_sheets[sheet_name] = df
                    
            st.session_state.diff_df = st.session_state.report_sheets.get('状态差异', pd.DataFrame())
            
        # ================= 模式二：继续处理 =================
        else:
            xls = pd.ExcelFile(uploaded_files)
            for sheet_name in xls.sheet_names:
                df = pd.read_excel(xls, sheet_name=sheet_name)
                # 兼容性修复：确保旧表格也有这两个字段
                if sheet_name == '状态差异':
                    if '处理进度' not in df.columns: df.insert(0, '处理进度', '未处理')
                    if '处理备注' not in df.columns: df.insert(1, '处理备注', '')
                st.session_state.report_sheets[sheet_name] = df
            
            st.session_state.diff_df = st.session_state.report_sheets.get('状态差异', pd.DataFrame())

        # 统一解析图片并保存
        st.session_state.img_mapping = parse_txt_mappings(txt_files)
        st.session_state.enable_viewer = True
        st.session_state.analysis_completed = True
        
        # 初始生成一次 Excel
        generate_latest_excel()

# -------------------------- 沉浸式处理工作台 --------------------------
if st.session_state.analysis_completed:
    st.divider()
    
    # 顶部状态统计
    diff_df = st.session_state.diff_df
    if diff_df.empty:
        st.success("🎉 太棒了！本次数据没有任何状态差异的考生。")
        st.stop()
        
    total_diff = len(diff_df)
    processed_count = len(diff_df[diff_df['处理进度'] == '已核对'])
    pending_count = total_diff - processed_count
    
    st.header("💻 异常试卷在线处理台")
    col1, col2, col3 = st.columns(3)
    col1.metric("⚠️ 总需核对人数", f"{total_diff} 人")
    col2.metric("✅ 已核对完毕", f"{processed_count} 人")
    col3.metric("⏳ 待处理", f"{pending_count} 人")
    
    # 动态下载按钮 (无论何时点击，下载的都是带有最新标记的 Excel)
    st.download_button(
        label="💾 保存并导出最新进度 Excel 报告 (处理完随时可点)",
        data=st.session_state.excel_bytes,
        file_name="多科目状态追踪与处理报告.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        type="primary",
        use_container_width=True
    )
    
    st.markdown("---")

    # ==================== 动态标记互动区 ====================
    options = []
    # 格式: [未处理] 198060001 | 王若涵
    for _, row in diff_df.iterrows():
        status_icon = "🟢 [已核对]" if str(row.get('处理进度')) == '已核对' else "🔴 [未处理]"
        options.append(f"{status_icon} {row['考号']} | {row['姓名']} | {row['学校']} {row['班级']}")
    
    selected_option = st.selectbox("🔍 请搜索或下拉选择要处理的考生：", options, index=0)
    
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
            
            # 分两列排版：左边看图，右边打标记
            view_col, action_col = st.columns([7, 3])
            
            with view_col:
                st.markdown(f"**当前核对：** `{student_name} ({student_id})` 　|　 ❌ **未扫科目：** `{unscanned_subjs}`")
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
                            <div style="text-align:center; margin-top:4px;">
                                <a href="{url}" target="_blank" style="text-decoration:none; font-size:12px;">🔍 点此放大查看</a>
                            </div>
                            <br>
                            '''
                            st.markdown(html_img, unsafe_allow_html=True)
                else:
                    st.info("⚠️ 该考生暂无匹配的试卷第一页图片。")
            
            # 右侧操作表单
            with action_col:
                with st.form(key=f"form_{student_id}"):
                    st.subheader("📝 标记操作")
                    new_status = st.radio("处理进度：", ["未处理", "已核对"], index=0 if current_status == '未处理' else 1)
                    new_remark = st.text_area("异常原因备注：", value=current_remark if current_remark != 'nan' else '', height=120, placeholder="例如：缺考 / 走错考场 / 答题卡损坏...")
                    
                    if st.form_submit_button("✅ 保存标记结果", type="primary", use_container_width=True):
                        # 更新内存里的 DataFrame
                        idx_to_update = st.session_state.diff_df.index[st.session_state.diff_df['考号'].astype(str) == student_id].tolist()
                        if idx_to_update:
                            st.session_state.diff_df.at[idx_to_update[0], '处理进度'] = new_status
                            st.session_state.diff_df.at[idx_to_update[0], '处理备注'] = new_remark
                            
                            # 重新生成 Excel 字节流
                            generate_latest_excel()
                            
                            st.toast(f"✅ {student_name} 的处理结果已保存！")
                            st.rerun() # 瞬间刷新页面，更新顶部下拉框状态
        else:
            st.error("⚠️ 未能匹配到该考生的详细数据，请重新选择。")
