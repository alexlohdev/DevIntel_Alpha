import os
import csv
import glob
import io
import streamlit as st
import pandas as pd
from datetime import datetime

# =========================
# DATA CONFIG
# =========================
DATA_DIR = "data/pemaju"
HISTORY_FILE = "data/history_tracker.csv"
ACCESS_LOG_FILE = "data/access_logs.csv"


# =========================================================
# PAGE CONFIG
# =========================================================
st.set_page_config(
    page_title="DevIntel",
    layout="wide",
    initial_sidebar_state="expanded",
)

# =========================================================
# THEME (Dark fixed)
# =========================================================
def apply_theme():
    st.markdown(
        """
        <style>
        .stApp { background: #0B1220; color: #E6EAF2; }
        [data-testid="stSidebar"] { background: #0F1A2B; }
        .card { background: #121F35; border: 1px solid #203454; }
        .muted { color: #A9B4C7; }
        .titleBig { font-size: 48px; font-weight: 800; letter-spacing: -0.5px; }
        .titleSmall { font-size: 14px; font-weight: 700; color: #A9B4C7; }
        .kpiNum { font-size: 28px; font-weight: 800; }
        .pill { display:inline-block; padding:6px 10px; border-radius:999px; background:#1B2C47; border:1px solid #2A4168; font-size:12px; }
        .divider { height: 1px; background: #203454; margin: 10px 0 14px 0; }

        /* Bigger buttons */
        .stButton>button, .stDownloadButton>button {
            height: 44px;
            font-weight: 700;
            border-radius: 12px;
        }
        
        /* Metric comparison styling */
        .metric-label { font-size: 12px; color: #A9B4C7; text-transform: uppercase; letter-spacing: 1px; }
        .metric-val { font-size: 24px; font-weight: 700; color: #E6EAF2; }

        /* Mobile tweaks */
        @media (max-width: 768px) {
          .titleBig { font-size: 32px; }
          .titleSmall { font-size: 12px; }
          .kpiNum { font-size: 22px; }
          .pill { font-size: 11px; padding:5px 8px; }
          .card { padding: 12px; }
          [data-testid="stSidebar"] { width: 80vw !important; min-width: 280px; }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )

apply_theme()

# =========================================================
# AUTHENTICATION & LOGGING
# =========================================================
def log_access(name, org):
    """Log user access to a CSV file."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    header = ["Timestamp", "Name", "Organization"]
    
    # Check if file exists to write header
    file_exists = os.path.exists(ACCESS_LOG_FILE)
    
    try:
        with open(ACCESS_LOG_FILE, mode="a", newline="", encoding="utf-8") as f:
            writer = csv.writer(f)
            if not file_exists:
                writer.writerow(header)
            writer.writerow([timestamp, name, org])
    except Exception as e:
        print(f"Logging failed: {e}")



def check_login():
    """Simple gatekeeper ensuring user enters name."""
    if st.session_state.get("authenticated"):
        return

    st.markdown(
        """
        <div style='text-align: center; margin-top: 100px;'>
            <h1>🔐 Beta Access</h1>
            <p style='color: #A9B4C7;'>Please enter your details to access the DevIntel Dashboard.</p>
        </div>
        """,
        unsafe_allow_html=True
    )

    c1, c2, c3 = st.columns([1, 2, 1])
    with c2:
        with st.form("login_form"):
            name = st.text_input("Name", placeholder="E.g. John Doe")
            org = st.text_input("Organization (Optional)", placeholder="E.g. Company XYZ")
            submitted = st.form_submit_button("Enter Dashboard", use_container_width=True)
            
            if submitted:
                if name.strip():
                    log_access(name, org)
                    st.session_state["authenticated"] = True
                    st.session_state["user_name"] = name
                    st.rerun()
                else:
                    st.error("Please enter your name.")

    st.stop() # Stop execution if not authenticated

# =========================================================
# STATE
# =========================================================
check_login()

if "selected_pemaju" not in st.session_state:
    st.session_state.selected_pemaju = "All"

# =========================================================
# HELPERS
# =========================================================
def get_pemaju_list(data_dir):
    if not os.path.exists(data_dir):
        return []
    return sorted([
        d.strip() for d in os.listdir(data_dir)
        if os.path.isdir(os.path.join(data_dir, d))
    ])

def _pick_latest_file(folder: str, contains_text: str):
    files = sorted(
        glob.glob(os.path.join(folder, f"*{contains_text}*.csv")),
        reverse=True 
    )
    return files[0] if files else None

def _to_float_rm(x):
    s = str(x or "").strip()
    s = s.replace("RM", "").replace(",", "").strip()
    try:
        return float(s) if s else 0.0
    except:
        return 0.0

@st.cache_data(show_spinner=False)
def load_all_pemaju_data(data_dir: str):
    if not os.path.exists(data_dir):
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    pemaju_folders_raw = [
        d for d in os.listdir(data_dir)
        if os.path.isdir(os.path.join(data_dir, d))
    ]

    master_frames, unit_frames, house_frames = [], [], []

    for pemaju_folder in pemaju_folders_raw:
        pemaju = pemaju_folder.strip()
        folder = os.path.join(data_dir, pemaju_folder)

        master_file = _pick_latest_file(folder, "_MELAKA_ALL_PROJECTS_")
        unit_file   = _pick_latest_file(folder, "_MELAKA_UNIT_DETAILS_")
        house_file  = _pick_latest_file(folder, "_MELAKA_HOUSE_TYPE_")

        try:
            if master_file:
                dfm = pd.read_csv(master_file, encoding="utf-8-sig")
                dfm.columns = dfm.columns.astype(str).str.replace("\ufeff", "", regex=False).str.strip()
                dfm["Pemaju"] = pemaju
                master_frames.append(dfm)
        except Exception: pass

        try:
            if unit_file:
                dfu = pd.read_csv(unit_file, encoding="utf-8-sig")
                dfu.columns = dfu.columns.astype(str).str.replace("\ufeff", "", regex=False).str.strip()
                dfu["Pemaju"] = pemaju
                # Fix PyArrow mixed types
                for col_fix in ["Bil Bilik", "Bil Bilik Air"]:
                    if col_fix in dfu.columns:
                        dfu[col_fix] = dfu[col_fix].astype(str)
                unit_frames.append(dfu)
        except Exception: pass

        try:
            if house_file:
                dfh = pd.read_csv(house_file, encoding="utf-8-sig")
                dfh.columns = dfh.columns.astype(str).str.replace("\ufeff", "", regex=False).str.strip()
                dfh["Pemaju"] = pemaju
                # Fix PyArrow mixed types
                for col_fix in ["Bil Bilik", "Bil Bilik Air"]:
                    if col_fix in dfh.columns:
                        dfh[col_fix] = dfh[col_fix].astype(str)
                house_frames.append(dfh)
        except Exception: pass

    df_master_all = pd.concat(master_frames, ignore_index=True) if master_frames else pd.DataFrame()
    df_units_all  = pd.concat(unit_frames,  ignore_index=True) if unit_frames else pd.DataFrame()
    df_house_all  = pd.concat(house_frames, ignore_index=True) if house_frames else pd.DataFrame()

    return df_master_all, df_units_all, df_house_all
    
def get_last_sync(df_list):
    times = []
    for df in df_list:
        if df is None or df.empty:
            continue
        for col in ["Scraped_Timestamp", "Scraped_Date"]:
            if col in df.columns:
                t = pd.to_datetime(df[col], errors="coerce")
                times.append(t.max())
                break
    times = [x for x in times if pd.notna(x)]
    return max(times) if times else None

def build_project_overview(df_master_all: pd.DataFrame, df_units_all: pd.DataFrame):
    if df_units_all is None or df_units_all.empty:
        return pd.DataFrame(columns=["No.", "Pemaju", "Kod Projek & Nama Projek", "Total Unit", "Unit Terjual", "Unit Belum Jual", "Take-Up %", "Jumlah Jualan (RM)", "Unit Bumi", "Unit Non Bumi", "Daerah", "Negeri"])

    dfu = df_units_all.copy()
    if "Pemaju" not in dfu.columns: dfu["Pemaju"] = ""
    if "No. Permit" not in dfu.columns: return pd.DataFrame()

    dfu["__status"] = dfu.get("Status Jualan", "").astype(str).str.lower()
    dfu["__is_sold"] = dfu["__status"].str.contains("telah dijual", na=False)
    dfu["__is_unsold"] = dfu["__status"].str.contains("belum dijual", na=False)
    dfu["__harga"] = dfu.get("Harga Jualan (RM)", "").apply(_to_float_rm)
    dfu["__is_bumi"] = dfu.get("Kuota Bumi", "").astype(str).str.strip().str.lower().eq("ya")

    gcols = ["Pemaju", "No. Permit"]
    agg = dfu.groupby(gcols, as_index=False).agg(
        **{
            "Total Unit": ("No Unit", "count"),
            "Unit Terjual": ("__is_sold", "sum"),
            "Unit Belum Jual": ("__is_unsold", "sum"),
            "Jumlah Jualan (RM)": ("__harga", lambda s: float(s[dfu.loc[s.index, "__is_sold"]].sum())),
            "Unit Bumi": ("__is_bumi", "sum"),
        }
    )
    agg["Unit Non Bumi"] = agg["Total Unit"] - agg["Unit Bumi"]

    if df_master_all is not None and not df_master_all.empty:
        dfm = df_master_all.copy()
        if "Pemaju" not in dfm.columns: dfm["Pemaju"] = ""
        keep_loc = [c for c in ["Pemaju", "No. Permit", "Daerah Projek", "Negeri Projek"] if c in dfm.columns]
        df_loc = dfm[keep_loc].drop_duplicates() if keep_loc else pd.DataFrame()
        if not df_loc.empty:
            agg = agg.merge(df_loc, on=["Pemaju", "No. Permit"], how="left")
            agg = agg.rename(columns={"Daerah Projek": "Daerah", "Negeri Projek": "Negeri"})
        else:
            agg["Daerah"] = ""; agg["Negeri"] = ""

        if "Kod Projek & Nama Projek" in dfm.columns:
            df_name = dfm[["Pemaju", "No. Permit", "Kod Projek & Nama Projek"]].drop_duplicates()
            agg = agg.merge(df_name, on=["Pemaju", "No. Permit"], how="left")
        else:
            agg["Kod Projek & Nama Projek"] = ""
    else:
        agg["Daerah"] = ""; agg["Negeri"] = ""; agg["Kod Projek & Nama Projek"] = ""

    agg["Take-Up %"] = (agg["Unit Terjual"] / agg["Total Unit"] * 100).fillna(0).round(1)
    
    agg = agg[[
        "Pemaju", "Kod Projek & Nama Projek", "Total Unit", "Unit Terjual",
        "Unit Belum Jual", "Take-Up %", "Jumlah Jualan (RM)", "Unit Bumi",
        "Unit Non Bumi", "Daerah", "Negeri",
    ]].copy()

    agg = agg.sort_values(["Pemaju", "Kod Projek & Nama Projek"], na_position="last").reset_index(drop=True)
    agg.insert(0, "No.", agg.index + 1)
    return agg

def calculate_kpis(df):
    """Returns a dictionary of KPI values for a given dataframe."""
    if df.empty:
        return {
            "projects": 0, "units": 0, "sold": 0, "unsold": 0, 
            "sales_rm": 0.0, "bumi": 0, "non_bumi": 0, "take_up": 0.0
        }
    
    total_units = int(df["Total Unit"].sum())
    total_sold = int(df["Unit Terjual"].sum())
    
    return {
        "projects": int(df.shape[0]),
        "units": total_units,
        "sold": total_sold,
        "unsold": int(df["Unit Belum Jual"].sum()),
        "sales_rm": float(df["Jumlah Jualan (RM)"].sum()),
        "bumi": int(df["Unit Bumi"].sum()),
        "non_bumi": int(df["Unit Non Bumi"].sum()),
        "take_up": (total_sold / total_units * 100) if total_units > 0 else 0.0
    }

# =========================================================
# SIDEBAR
# =========================================================
with st.sidebar:
    st.markdown("### DevIntel")
    st.markdown('<span class="pill">Beta</span>', unsafe_allow_html=True)
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    
    # Cleaned up Navigation - Removed unused pages
    nav_items = ["Overview", "Projects", "Trends"]
    page = st.radio("Navigation", nav_items, index=0)



# =========================================================
# LOAD DATA
# =========================================================
df_master_all, df_units_all, df_house_all = load_all_pemaju_data(DATA_DIR)
df_projects_all = build_project_overview(df_master_all, df_units_all)
last_sync = get_last_sync([df_master_all, df_units_all, df_house_all])

pemaju_list = get_pemaju_list(DATA_DIR)
pemaju_options = ["All"] + pemaju_list

# =========================================================
# UI Components
# =========================================================
def card(title: str, value: str, sub: str = ""):
    st.markdown(
        f"""
        <div class="card" style="border-radius:16px;padding:16px; height:100%;">
            <div class="titleSmall">{title}</div>
            <div class="kpiNum">{value}</div>
            <div class="muted" style="margin-top:4px;">{sub}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

def compare_card(title, val_a, val_b, is_currency=False):
    """Visual component for comparing two values side by side"""
    
    fmt_a = f"RM {val_a:,.0f}" if is_currency else f"{val_a:,}"
    fmt_b = f"RM {val_b:,.0f}" if is_currency else f"{val_b:,}"
    
    # Simple color logic: Green if A > B, else neutral (just for visual variation)
    color_a = "#E6EAF2"
    color_b = "#E6EAF2"

    st.markdown(
        f"""
        <div class="card" style="border-radius:12px; padding:16px; margin-bottom:12px;">
            <div class="titleSmall" style="margin-bottom:8px;">{title}</div>
            <div style="display:flex; justify-content:space-between; align-items:center;">
                <div style="text-align:left;">
                    <div style="font-size:10px; color:#A9B4C7; margin-bottom:2px;">DEVEL. A</div>
                    <div style="font-size:20px; font-weight:700; color:{color_a};">{fmt_a}</div>
                </div>
                <div style="width:1px; height:30px; background:#203454;"></div>
                <div style="text-align:right;">
                    <div style="font-size:10px; color:#A9B4C7; margin-bottom:2px;">DEVEL. B</div>
                    <div style="font-size:20px; font-weight:700; color:{color_b};">{fmt_b}</div>
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True
    )

def hero_total_sales(value_rm: float, subtitle="Across selected projects"):
    pretty = f"RM {value_rm:,.0f}"
    st.markdown(
        f"""
        <div class="card" style="border-radius:18px;padding:18px 20px;">
            <div class="titleSmall">Total Sales (RM)</div>
            <div class="titleBig">{pretty}</div>
            <div class="muted">{subtitle}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# =========================================================
# PAGE: OVERVIEW
# =========================================================
if page == "Overview":
    
    # 1. Header & View Mode Switch
    c1, c2 = st.columns([2, 1])
    with c1:
        st.markdown("## Dashboard")
        st.caption("Melaka • Competitive Intelligence")
    with c2:
        view_mode = st.radio("View Mode", ["Single View", "Compare Developers"], horizontal=True)

    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)

    # ==========================
    # MODE: SINGLE VIEW
    # ==========================
    if view_mode == "Single View":
        # Filter
        _last = st.session_state.get("selected_pemaju", "All")
        default_index = pemaju_options.index(_last) if _last in pemaju_options else 0
        selected = st.selectbox("Select Pemaju", pemaju_options, index=default_index)
        st.session_state.selected_pemaju = selected

        # Data subset
        if selected != "All":
            df_projects = df_projects_all[df_projects_all["Pemaju"] == selected].copy()
            df_house = df_house_all[df_house_all.get("Pemaju", "") == selected].copy()
        else:
            df_projects = df_projects_all.copy()
            df_house = df_house_all.copy()

        # KPIs
        kpis = calculate_kpis(df_projects)

        # Layout
        left, hero, right = st.columns([1.1, 2.2, 1.1])
        with left:
            card("Total Projects", f"{kpis['projects']}", "Projects found")
            st.write("")
            card("Total Units", f"{kpis['units']:,}", "Total units")
        with hero:
            hero_total_sales(kpis['sales_rm'])
            r1, r2, r3, r4 = st.columns(4)
            with r1: card("Sold", f"{kpis['sold']:,}")
            with r2: card("Unsold", f"{kpis['unsold']:,}")
            with r3: card("Bumi", f"{kpis['bumi']:,}")
            with r4: card("Non-Bumi", f"{kpis['non_bumi']:,}")
        with right:
            # Take up rate card
            card("Take-Up Rate", f"{kpis['take_up']:.1f}%", "Overall performance")
            st.write("")
            last_sync_str = last_sync.strftime("%Y-%m-%d") if last_sync is not None else "—"
            card("Last Sync", last_sync_str, "Date")

        # Table
        st.markdown("### Project Overview")
        bar1, bar2 = st.columns([3, 1])
        with bar1:
            q = st.text_input("Search", value="", placeholder="Search project...")
        with bar2:
            st.markdown("<div style='height: 28px;'></div>", unsafe_allow_html=True)
            csv_data = df_projects.to_csv(index=False).encode("utf-8-sig") if not df_projects.empty else b""
            st.download_button("⬇️ CSV", data=csv_data, file_name="data.csv", mime="text/csv", use_container_width=True, disabled=df_projects.empty)

        show_df = df_projects.copy()
        if q.strip() and not show_df.empty:
            qq = q.strip().lower()
            mask = show_df.astype(str).apply(lambda col: col.str.lower().str.contains(qq, na=False))
            show_df = show_df[mask.any(axis=1)]

        if not show_df.empty:
            format_dict = {"Jumlah Jualan (RM)": "RM {:,.0f}", "Take-Up %": "{:.1f}%"}
            st.dataframe(show_df.style.format(format_dict), use_container_width=True, hide_index=True)
        else:
            st.dataframe(show_df, use_container_width=True, hide_index=True)

        # House Types
        st.markdown("### House Type Details")
        if not df_house.empty:
            st.dataframe(df_house, use_container_width=True, hide_index=True)
        else:
            st.info("No house type data.")

    # ==========================
    # MODE: COMPARE VIEW
    # ==========================
    else:
        st.markdown("### ⚔️ Developer Comparison")
        
        # Selectors
        col_sel_a, col_sel_b = st.columns(2)
        with col_sel_a:
            pemaju_a = st.selectbox("Developer A", pemaju_list, index=0 if len(pemaju_list) > 0 else 0)
        with col_sel_b:
            # Try to pick a different default for B if possible
            default_b = 1 if len(pemaju_list) > 1 else 0
            pemaju_b = st.selectbox("Developer B", pemaju_list, index=default_b)

        # Get Data & Project Lists
        # ------------------------------------------------
        # Developer A
        raw_df_a = df_projects_all[df_projects_all["Pemaju"] == pemaju_a]
        projects_a = sorted(raw_df_a["Kod Projek & Nama Projek"].unique()) if not raw_df_a.empty else []
        
        with col_sel_a:
            sel_projects_a = st.multiselect("Projects (Dev A)", projects_a, default=[])
            
        if sel_projects_a:
            df_a = raw_df_a[raw_df_a["Kod Projek & Nama Projek"].isin(sel_projects_a)]
        else:
            df_a = raw_df_a  # Fallback if user clears all, usually showing nothing or everything is a design choice. Let's show filtered.
            if not projects_a: df_a = raw_df_a # No projects to select
            else: df_a = raw_df_a[raw_df_a["Kod Projek & Nama Projek"].isin([])] # User explicitly cleared selection

        # Developer B
        raw_df_b = df_projects_all[df_projects_all["Pemaju"] == pemaju_b]
        projects_b = sorted(raw_df_b["Kod Projek & Nama Projek"].unique()) if not raw_df_b.empty else []
        
        with col_sel_b:
            sel_projects_b = st.multiselect("Projects (Dev B)", projects_b, default=[])
            
        if sel_projects_b:
            df_b = raw_df_b[raw_df_b["Kod Projek & Nama Projek"].isin(sel_projects_b)]
        else:
            df_b = raw_df_b
            if not projects_b: df_b = raw_df_b
            else: df_b = raw_df_b[raw_df_b["Kod Projek & Nama Projek"].isin([])]

        # Calculate KPIs
        kpi_a = calculate_kpis(df_a)
        kpi_b = calculate_kpis(df_b)

        # Visual Comparison
        c_left, c_right = st.columns(2)

        with c_left:
            st.markdown(f"#### {pemaju_a}")
            hero_total_sales(kpi_a['sales_rm'], "Total Sales Value")
            st.write("")
            r1, r2 = st.columns(2)
            with r1: card("Projects", str(kpi_a['projects']))
            with r2: card("Take-Up Rate", f"{kpi_a['take_up']:.1f}%")
            
        with c_right:
            st.markdown(f"#### {pemaju_b}")
            hero_total_sales(kpi_b['sales_rm'], "Total Sales Value")
            st.write("")
            r1, r2 = st.columns(2)
            with r1: card("Projects", str(kpi_b['projects']))
            with r2: card("Take-Up Rate", f"{kpi_b['take_up']:.1f}%")

        # Side by Side Metrics
        st.markdown("#### Side-by-Side Breakdown")
        
        row1_1, row1_2, row1_3 = st.columns(3)
        with row1_1: compare_card("Total Units", kpi_a['units'], kpi_b['units'])
        with row1_2: compare_card("Units Sold", kpi_a['sold'], kpi_b['sold'])
        with row1_3: compare_card("Units Unsold", kpi_a['unsold'], kpi_b['unsold'])
        
        row2_1, row2_2, row2_3 = st.columns(3)
        with row2_1: compare_card("Bumi Units", kpi_a['bumi'], kpi_b['bumi'])
        with row2_2: compare_card("Non-Bumi Units", kpi_a['non_bumi'], kpi_b['non_bumi'])
        # Placeholder or gap
        
        # Detailed Projects Table for both
        st.markdown(f"#### Project List: {pemaju_a}")
        st.dataframe(df_a[["Kod Projek & Nama Projek", "Total Unit", "Unit Terjual", "Take-Up %", "Jumlah Jualan (RM)"]], use_container_width=True, hide_index=True)
        
        st.markdown(f"#### Project List: {pemaju_b}")
        st.dataframe(df_b[["Kod Projek & Nama Projek", "Total Unit", "Unit Terjual", "Take-Up %", "Jumlah Jualan (RM)"]], use_container_width=True, hide_index=True)


# =========================================================
# PAGE: PROJECTS
# =========================================================
elif page == "Projects":
    st.markdown("## Project Directory")
    
    # Simple table of all projects
    if not df_projects_all.empty:
        search_term = st.text_input("Search Projects", placeholder="Type to search...")
        
        display_df = df_projects_all.copy()
        if search_term:
            display_df = display_df[display_df.astype(str).apply(lambda x: x.str.contains(search_term, case=False, na=False)).any(axis=1)]

        format_dict = {"Jumlah Jualan (RM)": "RM {:,.0f}", "Take-Up %": "{:.1f}%"}
        st.dataframe(
            display_df.style.format(format_dict),
            use_container_width=True,
            hide_index=True,
            height=600
        )
    else:
        st.info("No projects found.")


# =========================================================
# PAGE: TRENDS
# =========================================================
elif page == "Trends":
    st.markdown("## 📈 Sales Trends (Historical)")
    
    
    if not os.path.exists(HISTORY_FILE):
        st.warning(f"No history file found at `{HISTORY_FILE}`. Run the publisher script to generate it.")
    else:
        try:
            # 1. Load History
            df_hist = pd.read_csv(HISTORY_FILE)
            
            # 2. Filter by the currently selected developer (from Sidebar logic, or add local select)
            # Since sidebar navigation handles selection context in Overview, here we let user choose freely
            dev_list = sorted(df_hist["Developer"].unique())
            sel_dev = st.selectbox("Select Developer", dev_list)
            
            df_dev_hist = df_hist[df_hist["Developer"] == sel_dev]
                
            if df_dev_hist.empty:
                st.info(f"No history records found for developer: {sel_dev}")
            else:
                # 3. Dropdown to pick a specific project
                projects = sorted(df_dev_hist["Project"].astype(str).unique())
                selected_proj = st.selectbox("Select Project to Analyze", projects, key="trend_proj_select")
                
                # 4. Filter Data for that project
                chart_data = df_dev_hist[df_dev_hist["Project"] == selected_proj].sort_values("Date")
                
                # 5. Render Chart - Comparing SOLD UNITS
                st.subheader(f"Sold Units Trend: {selected_proj}")
                
                # Ensure Date is actually datetime objects for correct X-axis scaling
                chart_data["Date"] = pd.to_datetime(chart_data["Date"])
                
                # Determine which column to plot
                y_col = "Unit Terjual"
                if y_col not in chart_data.columns:
                    if "Sold Units" in chart_data.columns: y_col = "Sold Units"
                    elif "Sold_Units" in chart_data.columns: y_col = "Sold_Units"
                
                if y_col in chart_data.columns:
                    st.line_chart(chart_data, x="Date", y=y_col)
                    st.caption(f"Tracking metric: {y_col}")
                else:
                    st.error("Column 'Unit Terjual' not found in history file. Please check CSV headers.")
                
                # 6. Show raw data
                with st.expander("View Raw Historical Data"):
                    st.dataframe(chart_data, use_container_width=True)

        except Exception as e:
            st.error(f"Error loading history data: {e}")

# =========================================================
# DEBUG PANEL
# =========================================================
with st.expander("🛠 Debug Panel", expanded=False):
    st.write(f"Data Dir: {DATA_DIR}")
    st.write(f"Projects Loaded: {len(df_projects_all)}")
