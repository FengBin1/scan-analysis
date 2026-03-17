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
    st.session_state.current_idx = 0
    st.session_state.current_filter = "全部显示"
    st.session_state.data_changed = False 

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
st.set_page_config(page_title="多科目异常追踪系统", page_icon="🎯", layout="wide")
st.title("🎯 多科目异常追踪与极速打标系统")
st.markdown("不仅永久记忆您的标记，更引入了**「快捷键翻页」「快捷标签秒杀」与「按校批量处理」**三大效率神器！")

# 注入键盘事件监听（前端黑科技）
st.markdown("""
<script>
    if (!window.parent.customNavListener) {
        window.parent.document.addEventListener('keydown', function(e) {
            if (e.key === 'ArrowLeft') {
                const btn = Array.from(window.parent.document.querySelectorAll('button')).find(el => el.innerText.includes('上一个'));
                if (btn) btn.click();
            } else if (e.key === 'ArrowRight') {
                const btn = Array.from(window.parent.document.querySelectorAll('button')).find(el => el.innerText.includes('下一个'));
                if (btn) btn.click();
            }
        });
        window.parent.customNavListener = true;
    }
</script>
""", unsafe_allow_html=True)

col_a, col_b, col_c = st.columns(3)
with col_a:
    raw_files = st.file_uploader("1️⃣ 必填：上传最新各科 Excel 数据", type=['xlsx', 'xls'], accept_multiple_files=True)
with col_b:
    history_file = st.file_uploader("2️⃣ 可选：上传上次导出的本系统标记报告", type=['xlsx', 'xls'], accept_multiple_files=False)
with col_c:
    txt_files = st.file_uploader("3️⃣ 可选：上传图片映射 TXT", type=['txt'], accept_multiple_files=True)

if st.button("🚀 开始极速分析与继承标记", type="primary", use_container_width=True):
    if not raw_files or len(raw_files) < 2:
        st.warning("⚠️ 必须至少上传 2 个最新科目的 Excel 文件才能进行对比！")
        st.stop()

    with st.spinner('🚀 正在合并数据并继承历史记忆...'):
        st.session_state.report_sheets = {}
        merged_data = []
        with ThreadPoolExecutor(max_workers=min(len(raw_files), 8)) as executor:
            futures = {executor.submit(load_uploaded_file, f): f for f in raw_files}
            for future in as_completed(futures):
                file_data, subject = future.result()
                if file_data is not None and not file_data.empty: merged_data.append(file_data)

        all_merged = pd.concat(merged_data, ignore_index=True)
        classification_result = classify_scan_status(all_merged)
        
        st.session_state.report_sheets['名单数据'] = generate_student_list_data(all_merged)
        pivot_tables = create_scan_pivot_table(all_merged)
        st.session_state.report_sheets['扫描数量透视表'] = pivot_tables['数量透视表']
        st.session_state.report_sheets['扫描百分比透视表'] = pivot_tables['百分比透视表']
        
        new_diff_df = classification_result['状态差异'].copy()
        if not new_diff_df.empty:
            new_diff_df.insert(0, '处理进度', '未处理')
            new_diff_df.insert(1, '处理备注', '')
        
        inherited_count = 0
        if history_file and not new_diff_df.empty:
            try:
                hist_df = pd.read_excel(history_file, sheet_name='状态差异')
                if '考号' in hist_df.columns and '处理进度' in hist_df.columns:
                    hist_status = hist_df.set_index(hist_df['考号'].astype(str))['处理进度'].to_dict()
                    hist_remark = hist_df.set_index(hist_df['考号'].astype(str))['处理备注'].to_dict() if '处理备注' in hist_df.columns else {}
                    new_diff_df['处理进度'] = new_diff_df.apply(lambda r: hist_status.get(str(r['考号']), '未处理'), axis=1)
                    new_diff_df['处理备注'] = new_diff_df.apply(lambda r: hist_remark.get(str(r['考号']), '') if pd.notna(hist_remark.get(str(r['考号']), '')) else '', axis=1)
                    inherited_count = len(new_diff_df[new_diff_df['处理进度'] == '已标记'])
            except Exception as e:
                st.warning(f"⚠️ 提取历史继承数据时出错: {e}")

        for sheet_name, df in classification_result.items():
            if sheet_name == '状态差异': st.session_state.report_sheets[sheet_name] = new_diff_df
            elif not df.empty: st.session_state.report_sheets[sheet_name] = df
                
        st.session_state.diff_df = new_diff_df
        
        if txt_files:
            st.session_state.img_mapping = parse_txt_mappings(txt_files)
            st.session_state.enable_viewer = True
        else:
            st.session_state.enable_viewer = False
            st.session_state.img_mapping = {}

        st.session_state.analysis_completed = True
        st.session_state.current_idx = 0
        st.session_state.current_filter = "全部显示"
        st.session_state.data_changed = False
        generate_latest_excel()
        if inherited_count > 0: st.toast(f"🎉 成功继承了 {inherited_count} 名考生的历史标记！")

# -------------------------- 沉浸式双核处理台 --------------------------
if st.session_state.analysis_completed:
    st.divider()
    diff_df = st.session_state.diff_df
    
    if diff_df.empty:
        st.success("🎉 太棒了！本次最新数据中没有任何状态差异的考生。")
        st.stop()
        
    total_diff = len(diff_df)
    processed_count = len(diff_df[diff_df['处理进度'] == '已标记'])
    pending_count = total_diff - processed_count
    
    st.header("💻 异常名单智能处理台")
    col1, col2, col3 = st.columns(3)
    col1.metric("⚠️ 差异大名单总人数", f"{total_diff} 人")
    col2.metric("✅ 已知情 / 已标记", f"{processed_count} 人")
    col3.metric("⏳ 尚未查明待处理", f"{pending_count} 人")
    
    # -------------------------- 极速下载控制台 --------------------------
    st.markdown("### 💾 数据保存与导出")
    dl_col1, dl_col2 = st.columns(2)
    with dl_col1:
        if st.session_state.get('data_changed', False):
            if st.button("🔄 1. 进度已更新，点击打包最新文件", use_container_width=True):
                with st.spinner("📦 正在极速生成最新 Excel..."):
                    generate_latest_excel()
                    st.session_state.data_changed = False
                    st.rerun()
        else:
            st.button("✅ 1. 生成功能已是最新状态", disabled=True, use_container_width=True)
            
    with dl_col2:
        if not st.session_state.get('data_changed', False) and st.session_state.excel_bytes:
            st.download_button(
                label="📥 2. 一键下载最新 Excel 报告 (作为下次继承凭证)",
                data=st.session_state.excel_bytes,
                file_name="多科目状态追踪与处理报告.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                type="primary",
                use_container_width=True
            )
        else:
            st.button("📥 2. 请先点击左侧按钮打包数据", disabled=True, use_container_width=True)

    st.markdown("---")
    
    # -------------------------- 双模式工作台 Tab --------------------------
    tab1, tab2 = st.tabs(["🎯 逐个精细核对 (支持快捷键)", "🗂️ 按学校批量打标"])
    
    # ========================== TAB 1: 逐个核对模式 ==========================
    with tab1:
        selected_filter = st.radio(
            "🔎 **快速筛选视图：**", ["全部显示", "🔴 只看未处理", "🟢 只看已标记"], horizontal=True, 
            index=["全部显示", "🔴 只看未处理", "🟢 只看已标记"].index(st.session_state.current_filter)
        )
        
        if selected_filter != st.session_state.current_filter:
            st.session_state.current_filter = selected_filter
            st.session_state.current_idx = 0
            st.rerun()

        if selected_filter == "🔴 只看未处理": display_df = diff_df[diff_df['处理进度'] == '未处理']
        elif selected_filter == "🟢 只看已标记": display_df = diff_df[diff_df['处理进度'] == '已标记']
        else: display_df = diff_df

        if display_df.empty:
            st.info(f"✨ 当前视图下 ({selected_filter}) 暂无需要处理的考生。")
        else:
            options = []
            for _, row in display_df.iterrows():
                status_icon = "🟢 [已标记]" if str(row.get('处理进度')) == '已标记' else "🔴 [未处理]"
                options.append(f"{status_icon} {row['考号']} | {row['姓名']} | {row['学校']} {row['班级']}")
            
            if st.session_state.current_idx >= len(options): st.session_state.current_idx = max(0, len(options) - 1)

            st.write("🔍 **快速定位或快捷键切换 (支持 ← 左方向键 / 右方向键 →)：**")
            nav_col1, nav_col2, nav_col3 = st.columns([1, 8, 1])
            with nav_col1:
                if st.button("⬅️ 上一个", disabled=(st.session_state.current_idx == 0), use_container_width=True):
                    st.session_state.current_idx -= 1
                    st.rerun()
            with nav_col2:
                selected_option = st.selectbox("隐藏", options, index=st.session_state.current_idx, label_visibility="collapsed")
                actual_idx = options.index(selected_option)
                if actual_idx != st.session_state.current_idx:
                    st.session_state.current_idx = actual_idx
                    st.rerun()
            with nav_col3:
                if st.button("下一个 ➡️", disabled=(st.session_state.current_idx == len(options) - 1), use_container_width=True):
                    st.session_state.current_idx += 1
                    st.rerun()
            
            st.markdown("<br>", unsafe_allow_html=True)
            
            student_id = selected_option.split(" | ")[0].split("] ")[1].strip()
            student_name = selected_option.split(" | ")[1].strip()
            matched_rows = display_df[display_df['考号'].astype(str) == student_id]
            
            if not matched_rows.empty:
                row_data = matched_rows.iloc[0]
                current_remark = str(row_data.get('处理备注', ''))
                scanned_subjs = str(row_data['已扫科目']).split('; ')
                unscanned_subjs = str(row_data['未扫科目'])
                
                view_col, action_col = st.columns([6, 4])
                
                with view_col:
                    st.markdown(f"**当前查验：** `{student_name} ({student_id})` 　|　 ❌ **差异科目：** `{unscanned_subjs}`")
                    if not st.session_state.enable_viewer:
                        st.info("ℹ️ 未上传图片映射 TXT，如已知晓原因，请直接在右侧填写备注并标记。")
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
                                    st.markdown(f'''<a href="{url}" target="_blank"><img src="{url}" style="width:100%; border-radius:4px; border:1px solid #ccc;"/></a>''', unsafe_allow_html=True)
                        else:
                            st.warning("⚠️ 暂无匹配的试卷第一页图片。")
                
                with action_col:
                    with st.container(border=True):
                        st.subheader("⚡ 快捷标签一键秒杀")
                        st.caption("点击下方按钮，将自动填入备注并跳到下一个人")
                        
                        def save_and_go_next(remark):
                            idx_to_update = st.session_state.diff_df.index[st.session_state.diff_df['考号'].astype(str) == student_id].tolist()
                            if idx_to_update:
                                st.session_state.diff_df.at[idx_to_update[0], '处理进度'] = '已标记'
                                st.session_state.diff_df.at[idx_to_update[0], '处理备注'] = remark
                                st.session_state.data_changed = True
                                if selected_filter == "🔴 只看未处理": pass
                                else:
                                    if st.session_state.current_idx < len(options) - 1: st.session_state.current_idx += 1
                                st.rerun()

                        tag_col1, tag_col2 = st.columns(2)
                        if tag_col1.button("🚨 缺考", use_container_width=True): save_and_go_next("缺考")
                        if tag_col2.button("📝 白卷", use_container_width=True): save_and_go_next("白卷")
                        if tag_col1.button("✂️ 条码破损", use_container_width=True): save_and_go_next("条码破损")
                        if tag_col2.button("🚫 走错考场", use_container_width=True): save_and_go_next("走错考场")
                        if st.button("⏪ 撤销标记 (退回未处理)", use_container_width=True):
                            idx_to_update = st.session_state.diff_df.index[st.session_state.diff_df['考号'].astype(str) == student_id].tolist()
                            if idx_to_update:
                                st.session_state.diff_df.at[idx_to_update[0], '处理进度'] = '未处理'
                                st.session_state.diff_df.at[idx_to_update[0], '处理备注'] = ''
                                st.session_state.data_changed = True
                                if selected_filter == "🟢 只看已标记": pass
                                else:
                                    if st.session_state.current_idx < len(options) - 1: st.session_state.current_idx += 1
                                st.rerun()

                        st.divider()
                        st.subheader("✏️ 或者手动输入其他原因")
                        custom_remark = st.text_input("差异原因：", value=current_remark if current_remark != 'nan' else '')
                        if st.button("✅ 手动保存并下一个", type="primary", use_container_width=True):
                            save_and_go_next(custom_remark)

    # ========================== TAB 2: 按学校批量处理 ==========================
    with tab2:
        st.subheader("🏢 按学校快速批量标记")
        st.caption("对于因为未收齐等原因导致的某个学校群体性异常，您可以在这里一键全选标记。")
        
        # 只提取未处理的数据进行批量操作
        unprocessed_df = diff_df[diff_df['处理进度'] == '未处理']
        
        if unprocessed_df.empty:
            st.success("🎉 太棒了，当前没有任何「未处理」的学生需要批量打标！")
        else:
            schools = unprocessed_df['学校'].unique()
            selected_school = st.selectbox("1️⃣ 请选择要批量处理的学校：", schools)
            
            school_df = unprocessed_df[unprocessed_df['学校'] == selected_school]
            school_count = len(school_df)
            
            st.info(f"📊 数据检测：该学校目前共有 **{school_count}** 名未处理的异常考生。")
            
            batch_remark = st.text_input("2️⃣ 请输入批量统一的异常原因：", placeholder="例如：该考场试卷未回收...")
            
            if st.button(f"⚡ 确认将这 {school_count} 人全部标记为上述原因", type="primary"):
                if not batch_remark:
                    st.warning("⚠️ 请先输入异常原因！")
                else:
                    # 获取这批学生在总表中的真实索引并统一修改
                    batch_indexes = school_df.index
                    st.session_state.diff_df.loc[batch_indexes, '处理进度'] = '已标记'
                    st.session_state.diff_df.loc[batch_indexes, '处理备注'] = batch_remark
                    st.session_state.data_changed = True
                    st.session_state.current_idx = 0 # 重置单兵模式的指针
                    
                    st.toast(f"✅ 成功将 {selected_school} 的 {school_count} 名考生批量标记！")
                    st.rerun()
