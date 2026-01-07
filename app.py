"""
Advanced Data Refinery - OpenRefine升级版
一个比OpenRefine更先进的数据清洗工具，支持：
1. AI自动字段类型识别
2. 多格式日期清洗（含歧义检测）
3. 数值范围异常检测（保留原始值）
4. 可视化异常面板
5. 结构化清洗日志（JSON导出）
"""

import streamlit as st
import pandas as pd
import numpy as np
import json
import io
from datetime import datetime
import re
from pathlib import Path



# ========== Page Config ==========
st.set_page_config(
    page_title="Advanced Data Refinery",
    page_icon="🔬",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ========== Custom CSS ==========
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&display=swap');

    .stApp {
        background: linear-gradient(135deg, #f8fafc 0%, #e0f2fe 50%, #f8fafc 100%);
        font-family: 'Inter', sans-serif;
    }

    /* Hero Section */
    .hero-banner {
        background: linear-gradient(135deg, #1e40af 0%, #3b82f6 50%, #2563eb 100%);
        padding: 2rem;
        border-radius: 16px;
        color: white;
        margin-bottom: 2rem;
        box-shadow: 0 10px 25px -5px rgba(59, 130, 246, 0.4);
    }

    .hero-title {
        font-size: 2.5rem;
        font-weight: 700;
        margin-bottom: 0.5rem;
    }

    .hero-subtitle {
        font-size: 1.1rem;
        opacity: 0.9;
    }

    /* Stats Cards */
    .stat-card {
        background: white;
        padding: 1.5rem;
        border-radius: 12px;
        box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1);
        border-left: 4px solid #3b82f6;
    }

    .stat-value {
        font-size: 2rem;
        font-weight: 700;
        color: #1e293b;
    }

    .stat-label {
        font-size: 0.875rem;
        color: #64748b;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }

    /* Issue Badges */
    .issue-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 9999px;
        font-size: 0.75rem;
        font-weight: 600;
        margin-right: 0.5rem;
    }

    .issue-out_of_range {
        background-color: #fef2f2;
        color: #dc2626;
        border: 1px solid #fecaca;
    }

    .issue-invalid_format {
        background-color: #fff7ed;
        color: #ea580c;
        border: 1px solid #fed7aa;
    }

    .issue-ambiguous_date {
        background-color: #fefce8;
        color: #ca8a04;
        border: 1px solid #fef08a;
    }

    .issue-not_numeric {
        background-color: #faf5ff;
        color: #9333ea;
        border: 1px solid #e9d5ff;
    }

    .issue-invalid_email {
        background-color: #eff6ff;
        color: #2563eb;
        border: 1px solid #bfdbfe;
    }

    /* Anomaly Card */
    .anomaly-card {
        background: white;
        border-left: 4px solid #f59e0b;
        border-radius: 8px;
        padding: 1rem;
        margin-bottom: 0.75rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.05);
    }

    /* Code Block */
    .code-block {
        background: #1e293b;
        color: #10b981;
        padding: 1rem;
        border-radius: 8px;
        font-family: 'Courier New', monospace;
        font-size: 0.875rem;
        overflow-x: auto;
    }
</style>
""", unsafe_allow_html=True)


# ========== Core Functions ==========

class FieldTypeDetector:
    """AI驱动的字段类型检测引擎"""

    @staticmethod
    def detect_type(series):
        """自动检测字段类型"""
        # 移除空值
        non_null = series.dropna()
        if len(non_null) == 0:
            return 'text'

        # 数字检测
        numeric_count = pd.to_numeric(non_null, errors='coerce').notna().sum()
        if numeric_count / len(non_null) > 0.8:
            return 'number'

        # 日期检测
        date_patterns = [
            r'^\d{4}[-/]\d{1,2}[-/]\d{1,2}$',
            r'^\d{1,2}[-/]\d{1,2}[-/]\d{4}$',
            r'^\d{4}\.\d{1,2}\.\d{1,2}$'
        ]
        date_count = sum(
            non_null.astype(str).str.match(pattern).sum()
            for pattern in date_patterns
        )
        if date_count / len(non_null) > 0.7:
            return 'date'

        # 邮箱检测
        if 'email' in series.name.lower() or 'mail' in series.name.lower():
            return 'email'

            # 邮箱检测：增加对 # 号的宽容度，用于初次识别
            # 只要包含 "字母 + @或# + 字母" 就初步认定为邮箱
        email_like_pattern = r'[a-zA-Z0-9._%+-]+[@#][a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
        email_hits = non_null.str.match(email_like_pattern).sum()
        if email_hits / len(non_null) > 0.6:  # 只要有 60% 像邮箱就认定是
            return 'email'

        # 布尔检测
        bool_values = ['true', 'false', 'yes', 'no', '1', '0', 'y', 'n', 'T', 'F']
        bool_count = non_null.astype(str).str.lower().isin(bool_values).sum()
        if bool_count / len(non_null) > 0.8:
            return 'boolean'

        return 'text'


# class DataCleaner:
#     """高级数据清洗引擎"""
#
#     def __init__(self):
#         self.cleaning_log = []
#         self.rules = {
#             'age': {'min': 0, 'max': 120},
#             'salary': {'min': 0, 'max': 10000000},
#             'price': {'min': 0, 'max': 1000000},
#             'quantity': {'min': 0, 'max': 100000}
#         }
#
#     def clean_date(self, value, row_idx, col_name):
#         """多格式日期清洗（含歧义检测）"""
#         if pd.isna(value):
#             return value
#
#         str_val = str(value).strip()
#
#         # 格式1: YYYY-MM-DD 或 YYYY/MM/DD
#         match = re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$', str_val)
#         if match:
#             year, month, day = match.groups()
#             return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
#
#         # 格式2: DD-MM-YYYY 或 MM-DD-YYYY (歧义检测)
#         match = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$', str_val)
#         if match:
#             p1, p2, year = match.groups()
#             if int(p1) <= 12 and int(p2) <= 12:
#                 # 歧义日期
#                 self.cleaning_log.append({
#                     'row': row_idx + 1,
#                     'column': col_name,
#                     'raw': value,
#                     'cleaned': f"{year}-{p1.zfill(2)}-{p2.zfill(2)}",
#                     'issue': 'ambiguous_date',
#                     'rule': None,
#                     'hint': f'可能是{p1}月{p2}日 或 {p2}月{p1}日'
#                 })
#                 return f"{year}-{p1.zfill(2)}-{p2.zfill(2)}"
#             else:
#                 # 明确的日期
#                 month = p1 if int(p1) <= 12 else p2
#                 day = p2 if int(p1) <= 12 else p1
#                 return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
#
#         # 格式3: YYYY.MM.DD
#         match = re.match(r'^(\d{4})\.(\d{1,2})\.(\d{1,2})$', str_val)
#         if match:
#             year, month, day = match.groups()
#             return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
#
#         # 无法解析
#         self.cleaning_log.append({
#             'row': row_idx + 1,
#             'column': col_name,
#             'raw': value,
#             'cleaned': None,
#             'issue': 'invalid_format',
#             'rule': 'valid_date_format',
#             'hint': '支持格式: YYYY-MM-DD, DD/MM/YYYY, YYYY.MM.DD'
#         })
#         return value
#
#     def clean_numeric(self, value, row_idx, col_name):
#         """数值清洗（含范围检测）"""
#         try:
#             num = float(value)
#         except (ValueError, TypeError):
#             self.cleaning_log.append({
#                 'row': row_idx + 1,
#                 'column': col_name,
#                 'raw': value,
#                 'cleaned': None,
#                 'issue': 'not_numeric',
#                 'rule': 'numeric_type',
#                 'hint': None
#             })
#             return value
#
#         # 检查范围规则
#         col_lower = col_name.lower()
#         for rule_key, rule_val in self.rules.items():
#             if rule_key in col_lower:
#                 if 'min' in rule_val and num < rule_val['min']:
#                     self.cleaning_log.append({
    #                     'row': row_idx + 1,
    #                     'column': col_name,
    #                     'raw': value,
    #                     'cleaned': None,
    #                     'issue': 'out_of_range',
    #                     'rule': f"{col_name} >= {rule_val['min']}",
    #                     'hint': None
    #                 })
    #                 return None
    #
    #             if 'max' in rule_val and num > rule_val['max']:
    #                 self.cleaning_log.append({
    #                     'row': row_idx + 1,
    #                     'column': col_name,
    #                     'raw': value,
    #                     'cleaned': None,
    #                     'issue': 'out_of_range',
    #                     'rule': f"{col_name} <= {rule_val['max']}",
    #                     'hint': None
    #                 })
    #                 return None
    #
    #     return num
    #
    # def clean_email(self, value, row_idx, col_name):
    #     """邮箱清洗"""
    #     if pd.isna(value):
    #         return value
    #
    #     email = str(value).strip().lower()
    #     # 修复常见错误
    #     email = email.replace('#', '@').replace('＠', '@')
    #
    #     # 验证格式
    #     email_pattern = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
    #     if not re.match(email_pattern, email):
    #         self.cleaning_log.append({
    #             'row': row_idx + 1,
    #             'column': col_name,
    #             'raw': value,
    #             'cleaned': email,
    #             'issue': 'invalid_email',
    #             'rule': 'valid_email_format',
    #             'hint': '缺少@或域名不完整'
    #         })
    #
    #     return email
    #
    # def clean_dataframe(self, df, field_types):
    #     """清洗整个DataFrame"""
    #     self.cleaning_log = []  # 重置日志
    #     df_cleaned = df.copy()
    #     df_original = df.copy()  # 保存原始数据
    #
    #     for col in df.columns:
    #         if col not in field_types:
    #             continue
    #
    #         field_type = field_types[col]
    #
    #         if field_type == 'date':
    #             df_cleaned[col] = [
    #                 self.clean_date(val, idx, col)
    #                 for idx, val in enumerate(df[col])
    #             ]
    #
    #         elif field_type == 'number':
    #             df_cleaned[col] = [
    #                 self.clean_numeric(val, idx, col)
    #                 for idx, val in enumerate(df[col])
    #             ]
    #
    #         elif field_type == 'email':
    #             df_cleaned[col] = [
    #                 self.clean_email(val, idx, col)
    #                 for idx, val in enumerate(df[col])
    #             ]
    #
    #     return df_cleaned, df_original, self.cleaning_log
                    #替换前classdataclean的版本

class DataCleaner:
    """高级数据清洗引擎 (全功能合并版)"""

    def __init__(self):
        self.cleaning_log = []
        # 1. 保留你原始定义的业务规则
        self.rules = {
            'age': {'min': 0, 'max': 120},
            'salary': {'min': 0, 'max': 10000000},
            'price': {'min': 0, 'max': 1000000},
            'quantity': {'min': 0, 'max': 100000}
        }

    def clean_date(self, value, row_idx, col_name):
        """多格式 + 自然语言 + 歧义检测"""
        if pd.isna(value): return value
        str_val = str(value).strip().lower()

        # --- A. 新增：处理自然语言 (yesterday) ---
        from datetime import timedelta
        if str_val == 'yesterday':
            return (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
        if str_val == 'today':
            return datetime.now().strftime('%Y-%m-%d')

        # --- B. 保留：原有正则匹配逻辑 ---
        # 格式1: YYYY-MM-DD 或 YYYY/MM/DD
        match_ymd = re.match(r'^(\d{4})[-/](\d{1,2})[-/](\d{1,2})$', str_val)
        if match_ymd:
            y, m, d = match_ymd.groups()
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"

        # 格式2: DD-MM-YYYY 或 MM-DD-YYYY (含原有的歧义检测)
        match_dmy = re.match(r'^(\d{1,2})[-/](\d{1,2})[-/](\d{4})$', str_val)
        if match_dmy:
            p1, p2, year = match_dmy.groups()
            if int(p1) <= 12 and int(p2) <= 12:
                self.cleaning_log.append({
                    'row': row_idx + 1, 'column': col_name, 'raw': value,
                    'cleaned': f"{year}-{p1.zfill(2)}-{p2.zfill(2)}",
                    'issue': 'ambiguous_date', 'rule': None,
                    'hint': f'可能是{p1}月{p2}日 或 {p2}月{p1}日'
                })
                return f"{year}-{p1.zfill(2)}-{p2.zfill(2)}"
            else:
                month = p1 if int(p1) <= 12 else p2
                day = p2 if int(p1) <= 12 else p1
                return f"{year}-{month.zfill(2)}-{day.zfill(2)}"

        # 格式3: YYYY.MM.DD (原有的点号分隔)
        match_dot = re.match(r'^(\d{4})\.(\d{1,2})\.(\d{1,2})$', str_val)
        if match_dot:
            y, m, d = match_dot.groups()
            return f"{y}-{m.zfill(2)}-{d.zfill(2)}"

        # 无法解析
        self.cleaning_log.append({
            'row': row_idx + 1, 'column': col_name, 'raw': value,
            'cleaned': None, 'issue': 'invalid_format', 'rule': 'valid_date_format',
            'hint': '支持格式: YYYY-MM-DD, DD/MM/YYYY, YYYY.MM.DD, yesterday'
        })
        return value

    def clean_numeric(self, value, row_idx, col_name):
        if pd.isna(value) or str(value).strip() == "":
            return None

        raw_str = str(value).strip()
        str_val = raw_str.lower()

        # 1. 识别无效占位符
        null_keywords = ['unknown', 'not a number', 'not_a_number', 'nan', 'n/a', '?', 'none', 'null', '-', 'undefined']
        if str_val in null_keywords:
            self.add_log(row_idx, col_name, raw_str, None, 'placeholder_value', f'识别为无效占位符: {raw_str}')
            return None

        # 2. 预处理：移除货币符号和千分位逗号 (让 $7,000 变成 7000)
        temp_val = str_val.replace('$', '').replace('￥', '').replace(',', '')

        # 3. 提取数字部分
        import re
        clean_num_match = re.search(r'[-+]?\d*\.?\d+', temp_val)

        try:
            if not clean_num_match:
                raise ValueError
            num = float(clean_num_match.group())
        except (ValueError, TypeError):
            self.add_log(row_idx, col_name, raw_str, None, 'not_numeric', '无法解析为数值')
            return None

        # 4. 范围检查 (根据你的 rules 配置)
        col_lower = col_name.lower()
        if hasattr(self, 'rules'):
            for rule_key, rule_val in self.rules.items():
                if rule_key in col_lower:
                    if 'min' in rule_val and num < rule_val['min']:
                        self.add_log(row_idx, col_name, raw_str, None, 'out_of_range', '数值低于最小值')
                        return None
                    if 'max' in rule_val and num > rule_val['max']:
                        self.add_log(row_idx, col_name, raw_str, None, 'out_of_range', '数值超出最大值')
                        return None

        return num

    def clean_email(self, value, row_idx, col_name):
        """邮箱清洗 (含 # 自动修复)"""
        if pd.isna(value): return value
        email = str(value).strip().lower()

        # --- 新增：修复符号错误 ---
        email = email.replace('#', '@').replace('＠', '@')

        # --- 保留：原有格式验证 ---
        email_pattern = r'^[^\s@]+@[^\s@]+\.[^\s@]+$'
        if not re.match(email_pattern, email):
            self.cleaning_log.append({
                'row': row_idx + 1, 'column': col_name, 'raw': value,
                'cleaned': email, 'issue': 'invalid_email',
                'rule': 'valid_email_format', 'hint': '缺少@或域名后缀不完整'
            })
        return email

    def clean_dataframe(self, df, field_types):
        """执行清洗管道 (保持原结构)"""
        self.cleaning_log = []
        df_cleaned = df.copy()
        df_original = df.copy()

        for col in df.columns:
            if col not in field_types:
                continue

            f_type = field_types[col]

            if f_type == 'number':
            # 1. 使用列表推导式，传入正确的行索引 i
            cleaned_list = [
                self.clean_numeric(val, i, col)
                for i, val in enumerate(df[col])
            ]

            # 2. 转换为 Series
            cleaned_series = pd.Series(cleaned_list, index=df.index)

            # 3. 【核心修复】强制转换并赋值
            # 这一步会确保 $7000 变成 7000.0，且整列 dtype 变为 float64
            df_cleaned[col] = pd.to_numeric(cleaned_series, errors='coerce')

            elif f_type == 'email':
                # 处理 Email
                df_cleaned[col] = df[col].apply(lambda x: self.clean_email(x, 0, col))

            elif f_type == 'date':
                # 处理日期
                df_cleaned[col] = df[col].apply(lambda x: self.clean_date(x, 0, col))

        # --- 最终兜底：如果列名里有 salary，再次确保它是 float ---
        for col in df_cleaned.columns:
            if 'salary' in col.lower():
                df_cleaned[col] = pd.to_numeric(df_cleaned[col], errors='coerce')

        return df_cleaned, df_original, self.cleaning_log






# ========== Session State Initialization ==========
if 'data' not in st.session_state:
    st.session_state.data = None
if 'data_cleaned' not in st.session_state:
    st.session_state.data_cleaned = None
if 'data_original' not in st.session_state:
    st.session_state.data_original = None
if 'field_types' not in st.session_state:
    st.session_state.field_types = {}
if 'cleaning_log' not in st.session_state:
    st.session_state.cleaning_log = []
if 'show_original' not in st.session_state:
    st.session_state.show_original = False


# ========== Main App ==========

def main():
    # Hero Section
    st.markdown("""
        <div class="hero-banner">
            <div class="hero-title">🔬 Advanced Data Refinery</div>
            <div class="hero-subtitle">
                AI-Powered Data Cleaning Engine with Smart Anomaly Detection & Structured Logging
            </div>
        </div>
    """, unsafe_allow_html=True)

    # Sidebar
    with st.sidebar:
        st.header("⚙️ Configuration")

        uploaded_file = st.file_uploader(
            "Upload Dataset",
            type=['csv', 'xlsx', 'xls'],
            help="支持CSV和Excel格式"
        )

        if uploaded_file:
            try:
                if uploaded_file.name.endswith('.csv'):
                    df = pd.read_csv(uploaded_file)
                else:
                    df = pd.read_excel(uploaded_file)

                st.session_state.data = df
                st.success(f"✅ Loaded {len(df)} rows × {len(df.columns)} columns")

                # 自动检测字段类型
                if st.button("🤖 Auto-Detect Field Types", use_container_width=True):
                    detector = FieldTypeDetector()
                    types = {}
                    for col in df.columns:
                        types[col] = detector.detect_type(df[col])
                    st.session_state.field_types = types
                    st.success("✅ Field types detected!")
                    st.rerun()

            except Exception as e:
                st.error(f"Error loading file: {e}")

        st.markdown("---")

        # 使用示例数据
        if st.button("📊 Load Sample Data", use_container_width=True):
            sample_data = pd.DataFrame({
                'id': [1, 2, 3, 4, 5],
                'name': ['Zhang San', 'Li Si', 'Wang Wu', 'Zhao Liu', 'Qian Qi'],
                'age': [25, 30, 150, 'invalid', 28],
                'email': ['zhang#example.com', 'lisi@example', 'wang@test.com', 'zhao@company.cn', 'qian@tech.io'],
                'date': ['2024-01-15', '01/02/2024', '2024.03.20', '2024-04-01', 'not-a-date'],
                'salary': [5000, 6000, -1000, 8000, 7500]
            })
            st.session_state.data = sample_data

            # 自动检测类型
            detector = FieldTypeDetector()
            types = {}
            for col in sample_data.columns:
                if col != 'id':
                    types[col] = detector.detect_type(sample_data[col])
            st.session_state.field_types = types
            st.success("✅ Sample data loaded!")
            st.rerun()

        st.markdown("---")
        st.markdown("### About")
        st.info(
            "**Advanced Data Refinery v2.0**\n\n"
            "比OpenRefine更先进:\n"
            "• AI自动类型识别\n"
            "• 日期歧义检测\n"
            "• 原始值保留\n"
            "• 结构化日志"
        )

    # Main Content
    if st.session_state.data is None:
        st.info("👈 请从侧边栏上传数据或加载示例数据")
        return

    df = st.session_state.data

    # Stats Bar
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.markdown("""
            <div class="stat-card">
                <div class="stat-label">Total Records</div>
                <div class="stat-value">{}</div>
            </div>
        """.format(len(df)), unsafe_allow_html=True)

    with col2:
        st.markdown("""
            <div class="stat-card">
                <div class="stat-label">Fields Detected</div>
                <div class="stat-value">{}</div>
            </div>
        """.format(len(st.session_state.field_types)), unsafe_allow_html=True)

    with col3:
        issues = len(st.session_state.cleaning_log)
        st.markdown("""
            <div class="stat-card" style="border-left-color: #f59e0b;">
                <div class="stat-label">Issues Found</div>
                <div class="stat-value" style="color: #f59e0b;">{}</div>
            </div>
        """.format(issues), unsafe_allow_html=True)

    with col4:
        clean_rate = 100 if issues == 0 else max(0,
                                                 100 - (issues / (len(df) * len(st.session_state.field_types)) * 100))
        st.markdown("""
            <div class="stat-card" style="border-left-color: #10b981;">
                <div class="stat-label">Clean Rate</div>
                <div class="stat-value" style="color: #10b981;">{:.0f}%</div>
            </div>
        """.format(clean_rate), unsafe_allow_html=True)

    st.markdown("<br>", unsafe_allow_html=True)

    # Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "📊 Data View",
        "⚙️ Field Types",
        "🚨 Anomaly Panel",
        "📋 Cleaning Logs"
    ])

    # Tab 1: Data View
    with tab1:
        col1, col2 = st.columns([3, 1])
        with col1:
            st.subheader("Data Preview")
        with col2:
            view_mode = st.radio(
                "View Mode",
                ["Cleaned", "Original"],
                horizontal=True,
                key="view_mode"
            )

        display_df = st.session_state.data_original if view_mode == "Original" and st.session_state.data_original is not None else (
            st.session_state.data_cleaned if st.session_state.data_cleaned is not None else df)

        st.dataframe(
            display_df,
            use_container_width=True,
            height=400
        )

        # 下载清洗后数据
        if st.session_state.data_cleaned is not None:
            csv = st.session_state.data_cleaned.to_csv(index=False)
            st.download_button(
                label="📥 Download Cleaned Data (CSV)",
                data=csv,
                file_name=f"cleaned_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv"
            )

    # Tab 2: Field Types
    with tab2:
        st.subheader("Auto-Detected Field Types")

        if not st.session_state.field_types:
            st.warning("⚠️ 请先在侧边栏点击 'Auto-Detect Field Types'")
        else:
            type_icons = {
                'text': '📝',
                'number': '🔢',
                'date': '📅',
                'email': '📧',
                'boolean': '✓'
            }

            cols = st.columns(3)
            for idx, (field, ftype) in enumerate(st.session_state.field_types.items()):
                with cols[idx % 3]:
                    icon = type_icons.get(ftype, '❓')
                    st.markdown(f"""
                        <div style="background: white; padding: 1rem; border-radius: 8px; border-left: 4px solid #3b82f6; margin-bottom: 1rem;">
                            <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">{icon}</div>
                            <div style="font-weight: 600; color: #1e293b;">{field}</div>
                            <div style="font-size: 0.75rem; color: #64748b; text-transform: uppercase;">{ftype}</div>
                        </div>
                    """, unsafe_allow_html=True)

            st.markdown("---")

            # Cleaning Rules Configuration
            st.subheader("⚙️ Cleaning Rules")
            st.info("为数值字段配置验证规则（可选）")

            numeric_fields = [f for f, t in st.session_state.field_types.items() if t == 'number']

            if numeric_fields:
                for field in numeric_fields:
                    with st.expander(f"🔢 {field} - Range Rules"):
                        col1, col2 = st.columns(2)
                        with col1:
                            min_val = st.number_input(
                                f"Minimum value",
                                value=0.0,
                                key=f"min_{field}"
                            )
                        with col2:
                            max_val = st.number_input(
                                f"Maximum value",
                                value=1000000.0,
                                key=f"max_{field}"
                            )

            # Start Cleaning Button
            if st.button("🚀 Start Data Cleaning Pipeline", type="primary", use_container_width=True):
                with st.spinner("🔄 Processing data..."):
                    cleaner = DataCleaner()
                    df_cleaned, df_original, logs = cleaner.clean_dataframe(
                        df,
                        st.session_state.field_types
                    )
                    st.session_state.data_cleaned = df_cleaned
                    st.session_state.data_original = df_original
                    st.session_state.cleaning_log = logs
                    st.success(f"✅ Cleaning complete! Found {len(logs)} issues.")
                    st.rerun()

    # Tab 3: Anomaly Panel
    with tab3:
        if not st.session_state.cleaning_log:
            st.info("ℹ️ 没有检测到异常。请先运行数据清洗。")
        else:
            st.subheader("🚨 Anomaly Detection Panel")

            logs = st.session_state.cleaning_log

            # Issue Type Statistics
            issue_counts = {}
            for log in logs:
                issue = log['issue']
                issue_counts[issue] = issue_counts.get(issue, 0) + 1

            issue_labels = {
                'out_of_range': '超出范围',
                'invalid_format': '格式错误',
                'ambiguous_date': '日期歧义',
                'not_numeric': '非数值',
                'invalid_email': '邮箱格式'
            }

            st.markdown("#### 📊 Issue Statistics")
            cols = st.columns(len(issue_counts))
            for idx, (issue, count) in enumerate(issue_counts.items()):
                with cols[idx]:
                    st.markdown(f"""
                        <div style="background: white; padding: 1rem; border-radius: 8px; text-align: center; border: 2px solid #e2e8f0;">
                            <div style="font-size: 2rem; font-weight: 700; color: #1e293b;">{count}</div>
                            <div style="font-size: 0.75rem; color: #64748b;">{issue_labels.get(issue, issue)}</div>
                        </div>
                    """, unsafe_allow_html=True)

            st.markdown("---")

            # Filters
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                filter_column = st.selectbox(
                    "Filter by Column",
                    ["All"] + list(set(log['column'] for log in logs))
                )
            with col2:
                filter_issue = st.selectbox(
                    "Filter by Issue Type",
                    ["All"] + list(issue_counts.keys())
                )
            with col3:
                if st.button("📥 Export", use_container_width=True):
                    json_data = json.dumps(logs, indent=2, ensure_ascii=False)
                    st.download_button(
                        label="Download JSON",
                        data=json_data,
                        file_name=f"anomalies_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                        mime="application/json"
                    )

            # Filter logs
            filtered_logs = logs
            if filter_column != "All":
                filtered_logs = [l for l in filtered_logs if l['column'] == filter_column]
            if filter_issue != "All":
                filtered_logs = [l for l in filtered_logs if l['issue'] == filter_issue]

            st.markdown(f"#### Found {len(filtered_logs)} anomalies")

            # Display anomalies
            for log in filtered_logs:
                issue_class = f"issue-{log['issue']}"
                st.markdown(f"""
                    <div class="anomaly-card">
                        <div style="display: flex; justify-content: space-between; align-items: start; margin-bottom: 0.75rem;">
                            <div>
                                <strong>Row {log['row']}</strong> → <strong>{log['column']}</strong>
                                <span class="issue-badge {issue_class}">{issue_labels.get(log['issue'], log['issue'])}</span>
                            </div>
                        </div>
                        <div style="display: grid; grid-template-columns: 1fr 1fr 1fr; gap: 1rem; font-size: 0.875rem;">
                            <div>
                                <div style="color: #64748b; font-size: 0.75rem; margin-bottom: 0.25rem;">Original</div>
                                <code style="background: #f1f5f9; padding: 0.25rem 0.5rem; border-radius: 4px;">{log['raw']}</code>
                            </div>
                            <div>
                                <div style="color: #64748b; font-size: 0.75rem; margin-bottom: 0.25rem;">Cleaned</div>
                                <code style="background: #dcfce7; padding: 0.25rem 0.5rem; border-radius: 4px;">{log['cleaned'] if log['cleaned'] is not None else 'null'}</code>
                            </div>
                            {f'''<div>
                                <div style="color: #64748b; font-size: 0.75rem; margin-bottom: 0.25rem;">Rule</div>
                                <code style="background: #dbeafe; padding: 0.25rem 0.5rem; border-radius: 4px; font-size: 0.7rem;">{log['rule']}</code>
                            </div>''' if log['rule'] else ''}
                        </div>
                        {f'<div style="margin-top: 0.75rem; background: #fef3c7; padding: 0.5rem; border-radius: 4px; font-size: 0.8rem; color: #92400e;">💡 {log["hint"]}</div>' if log.get('hint') else ''}
                    </div>
                """, unsafe_allow_html=True)

    ## Tab 4: Cleaning Logs
    with tab4:
        st.subheader("📋 Structured Cleaning Logs (JSON)")

        if not st.session_state.cleaning_log:
            st.info("ℹ️ 没有清洗日志。请先运行数据清洗。")
        else:
            col1, col2 = st.columns([4, 1])

            json_data = json.dumps(st.session_state.cleaning_log, indent=2, ensure_ascii=False)

            with col2:
                st.download_button(
                    label="📥 下载 JSON",
                    data=json_data,
                    file_name=f"cleaning_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
                    mime="application/json",
                    use_container_width=True
                )

            # Display JSON
            st.markdown('<div class="code-block">', unsafe_allow_html=True)
            st.json(st.session_state.cleaning_log)
            st.markdown('</div>', unsafe_allow_html=True)

            # Log Summary
            st.markdown("---")
            st.markdown("#### 📊 Log Summary")

            summary_data = {
                'Total Issues': len(st.session_state.cleaning_log),
                'Affected Rows': len(set(log['row'] for log in st.session_state.cleaning_log)),
                'Affected Columns': len(set(log['column'] for log in st.session_state.cleaning_log)),
                'Issue Types': len(set(log['issue'] for log in st.session_state.cleaning_log))
            }

            cols = st.columns(4)
            for idx, (key, value) in enumerate(summary_data.items()):
                with cols[idx]:
                    st.metric(key, value)

# 确保文件末尾有这一行来启动应用
if __name__ == "__main__":
    main()