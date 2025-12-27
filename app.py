import os
import csv
import glob
import io
import streamlit as st
import pandas as pd
from datetime import datetime
from sqlalchemy import text

# =========================
# DATA CONFIG
# =========================
# Note: HISTORY_FILE and ACCESS_LOG_FILE are still local.
# You can migrate these to Supabase later if needed.
DATA_DIR = "data/pemaju" # Kept for fallback, though we use DB now
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
    """Log user access to Supabase."""
    try:
        conn = st.connection("supabase", type="sql")
        # specific SQL query to insert data
        with conn.session as session:
            session.execute(
                text("INSERT INTO access_logs (user_name, organization) VALUES (:name, :org);"),
                {"name": name, "org": org}
            )
            session.commit()
    except Exception as e:
        st.error(f"Logging failed: {e}")

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
# DATABASE LOADERS & HELPERS
# =========================================================

def _to_float_rm(x):
    """Cleans currency strings like 'RM 1,200.00' to float."""
    s = str(x or "").strip()
    s = s.replace("RM", "").replace(",", "").strip()
    try:
        return float(s) if s else 0.0
    except:
        return 0.0

@st.cache_data(ttl=600, show_spinner=False)
def load_data_from_supabase():
    """Fetches data from Supabase and formats it for the dashboard."""
    # Connect using Streamlit's secrets
    conn = st.connection("supabase", type="sql")

    try:
        df_projects = conn.query("SELECT * FROM projects_master;", ttl=600)
        df_units = conn.query("SELECT * FROM units_detail;", ttl=600)
        df_house = conn.query("SELECT * FROM house_types;", ttl=600)
    except Exception as e:
        st.error(f"Failed to connect to database: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

    # Helper to combine Code + Name into one display column
    def create_display_name(df):
        if not df.empty and "project_code" in df.columns and "project_name" in df.columns:
            # Handle potential None/NaN values
            code = df["project_code"].fillna("")
            name = df["project_name"].fillna("")
            return code + " " + name
        return ""

    # 1. PREPARE PROJECTS MASTER
    if not df_projects.empty:
        # Create the unified name column (Crucial for UI)
        df_projects["Kod Projek & Nama Projek"] = create_display_name(df_projects)
        
        # Ensure date columns are datetime
        for col in ["scraped_date", "scraped_timestamp"]:
            if col in df_projects.columns:
                df_projects[col] = pd.to_datetime(df_projects[col], errors='coerce')

    # 2. PREPARE UNITS DETAIL
    if not df_units.empty:
        df_units["Kod Projek & Nama Projek"] = create_display_name(df_units)
        
    # 3. PREPARE HOUSE TYPES
    if not df_house.empty:
        df_house["Kod Projek & Nama Projek"] = create_display_name(df_house)

    return df_projects, df_units, df_house

def get_last_sync(df_list):
    """Finds the latest scraped timestamp across all dataframes."""
    times = []
    for df in df_list:
        if df is None or df.empty:
            continue
        # Check both naming conventions just in case
        for col in ["scraped_timestamp", "Scraped_Timestamp", "scraped_date", "Scraped_Date"]:
            if col in df.columns:
                t = pd.to_datetime(df[col], errors="coerce")
                times.append(t.max())
                break
    times = [x for x in times if pd.notna(x)]
    return max(times) if times else None

def build_project_overview(df_master_all: pd.DataFrame, df_units_all: pd.DataFrame):
    """
    Aggregates unit-level data into project-level statistics.
    Uses English database columns but outputs Malay headers to match UI expectations.
    """
    if df_units_all is None or df_units_all.empty:
        # Return empty structure with expected headers
        return pd.DataFrame(columns=["No.", "Pemaju", "Kod Projek & Nama Projek", "Total Unit", "Unit Terjual", 
                                   "Unit Belum Jual", "Take-Up %", "Jumlah Jualan (RM)", 
                                   "Unit Bumi", "Unit Non Bumi", "Daerah", "Negeri"])

    dfu = df_units_all.copy()
    
    # --- 1. Prepare Calculation Columns (using English DB names) ---
    # Status: Check for both Malay (from scraping) and English keywords
    dfu["__status"] = dfu.get("status", "").astype(str).str.lower()
    dfu["__is_sold"] = dfu["__status"].str.contains("telah dijual", na=False) | dfu["__status"].str.contains("sold", na=False)
    dfu["__is_unsold"] = dfu["__status"].str.contains("belum dijual", na=False) | dfu["__status"].str.contains("unsold", na=False)
    
    # Price
    dfu["__harga"] = dfu.get("price_sales", "").apply(_to_float_rm)
    
    # Bumi Quota
    dfu["__is_bumi"] = dfu.get("bumi_quota", "").astype(str).str.strip().str.lower().eq("ya")

    # --- 2. Group By (Developer & Project) ---
    # We group by the Display Name we created earlier to keep it aligned with UI
    gcols = ["pemaju_name", "Kod Projek & Nama Projek"]
    
    # Safety check
    if "pemaju_name" not in dfu.columns or "Kod Projek & Nama Projek" not in dfu.columns:
        return pd.DataFrame()

    agg = dfu.groupby(gcols, as_index=False).agg(
        **{
            "Total Unit": ("unit_no", "count"),
            "Unit Terjual": ("__is_sold", "sum"),
            "Unit Belum Jual": ("__is_unsold", "sum"),
            "Jumlah Jualan (RM)": ("__harga", lambda s: float(s[dfu.loc[s.index, "__is_sold"]].sum())),
            "Unit Bumi": ("__is_bumi", "sum"),
        }
    )
    agg["Unit Non Bumi"] = agg["Total Unit"] - agg["Unit Bumi"]

    # --- 3. Merge Location Data from Master ---
    if df_master_all is not None and not df_master_all.empty:
        dfm = df_master_all.copy()
        
        # We need a common key. Project Code is safest, but we grouped by Name.
        # Let's try to grab location based on the 'Kod Projek & Nama Projek' we created in both DFs.
        keep_cols = ["Kod Projek & Nama Projek", "location_district", "location_state"]
        keep_cols = [c for c in keep_cols if c in dfm.columns]
        
        df_loc = dfm[keep_cols].drop_duplicates(subset=["Kod Projek & Nama Projek"])
        
        if not df_loc.empty:
            agg = agg.merge(df_loc, on="Kod Projek & Nama Projek", how="left")
            agg = agg.rename(columns={"location_district": "Daerah", "location_state": "Negeri"})
        else:
            agg["Daerah"] = ""; agg["Negeri"] = ""
    else:
        agg["Daerah"] = ""; agg["Negeri"] = ""

    # --- 4. Final Formatting ---
    agg["Take-Up %"] = (agg["Unit Terjual"] / agg["Total Unit"] * 100).fillna(0).round(1)
    
    # Rename 'pemaju_name' to 'Pemaju' for the UI
    agg = agg.rename(columns={"pemaju_name": "Pemaju"})

    # Select and Reorder columns to match exactly what the UI expects
    target_cols = [
        "Pemaju", "Kod Projek & Nama Projek", "Total Unit", "Unit Terjual",
        "Unit Belum Jual", "Take-Up %", "Jumlah Jualan (RM)", "Unit Bumi",
        "Unit Non Bumi", "Daerah", "Negeri",
    ]
    
    # Filter only existing columns (handles edge cases)
    final_cols = [c for c in target_cols if c in agg.columns]
    agg = agg[final_cols].copy()

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

def get_pemaju_list(df_master):
    """Extracts unique developer names."""
    # Check for English column name first, then fallback
    if "pemaju_name" in df_master.columns:
        return sorted(df_master["pemaju_name"].dropna().unique().tolist())
    elif "Pemaju" in df_master.columns:
        return sorted(df_master["Pemaju"].dropna().unique().tolist())
    return []

# =========================================================
# SIDEBAR
# =========================================================
with st.sidebar:
    st.markdown("### DevIntel")
    st.markdown('<span class="pill">Beta</span>', unsafe_allow_html=True)
    st.markdown('<div class="divider"></div>', unsafe_allow_html=True)
    
    nav_items = ["Overview", "Projects", "Trends"]
    page = st.radio("Navigation", nav_items, index=0)


# =========================================================
# LOAD DATA (EXECUTION)
# =========================================================
df_master_all, df_units_all, df_house_all = load_data_from_supabase()

# Build the main overview table
df_projects_all = build_project_overview(df_master_all, df_units_all)

# Get Sync Time
last_sync = get_last_sync([df_master_all, df_units_all, df_house_all])

# Get Developer List
# We use the master DF which has 'pemaju_name', pass that to helper
pemaju_list = get_pemaju_list(df_master_all)
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
        # Ensure selection is valid
        default_index = pemaju_options.index(_last) if _last in pemaju_options else 0
        selected = st.selectbox("Select Pemaju", pemaju_options, index=default_index)
        st.session_state.selected_pemaju = selected

        # Data subset
        if selected != "All":
            df_projects = df_projects_all[df_projects_all["Pemaju"] == selected].copy()
            # Note: df_house has 'pemaju_name' from DB, we didn't rename it in loader
            # but we should check which column to filter on.
            if not df_house_all.empty:
                if "pemaju_name" in df_house_all.columns:
                     df_house = df_house_all[df_house_all["pemaju_name"] == selected].copy()
                elif "Pemaju" in df_house_all.columns:
                     df_house = df_house_all[df_house_all["Pemaju"] == selected].copy()
                else:
                    df_house = pd.DataFrame()
            else:
                df_house = pd.DataFrame()
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
            # Drop technical ID/Timestamp columns for cleaner view if desired
            display_cols = [c for c in df_house.columns if c not in ['id', 'created_at', 'scraped_timestamp']]
            st.dataframe(df_house[display_cols], use_container_width=True, hide_index=True)
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
            # Default to all if none selected, or strict? 
            # Original logic: "fallback... usually showing nothing or everything". 
            # Let's show ALL projects by default if none selected for easier comparison.
            df_a = raw_df_a 

        # Developer B
        raw_df_b = df_projects_all[df_projects_all["Pemaju"] == pemaju_b]
        projects_b = sorted(raw_df_b["Kod Projek & Nama Projek"].unique()) if not raw_df_b.empty else []
        
        with col_sel_b:
            sel_projects_b = st.multiselect("Projects (Dev B)", projects_b, default=[])
            
        if sel_projects_b:
            df_b = raw_df_b[raw_df_b["Kod Projek & Nama Projek"].isin(sel_projects_b)]
        else:
            df_b = raw_df_b

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
        if not df_a.empty:
            st.dataframe(df_a[["Kod Projek & Nama Projek", "Total Unit", "Unit Terjual", "Take-Up %", "Jumlah Jualan (RM)"]], use_container_width=True, hide_index=True)
        else:
            st.info("No data")
        
        st.markdown(f"#### Project List: {pemaju_b}")
        if not df_b.empty:
            st.dataframe(df_b[["Kod Projek & Nama Projek", "Total Unit", "Unit Terjual", "Take-Up %", "Jumlah Jualan (RM)"]], use_container_width=True, hide_index=True)
        else:
            st.info("No data")


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
# =========================================================
# PAGE: TRENDS (Connected to Supabase)
# =========================================================
elif page == "Trends":
    st.markdown("## 📈 Sales Trends")
    st.caption("Data source: Supabase (history_logs)")

    # 1. Connect to Database
    conn = st.connection("supabase", type="sql")
    
    # 2. Fetch History Data
    try:
        # We order by date to ensure the chart looks right
        df_hist = conn.query("SELECT * FROM history_logs ORDER BY scraped_date ASC;", ttl=0)
    except Exception as e:
        st.error(f"Error connecting to database: {e}")
        st.stop()

    if df_hist.empty:
        st.info("No history logs available yet. (Run the publisher script to generate data!)")
    else:
        # 3. Filter by Developer
        # We use 'developer_name' because that's the column in the history_logs table
        dev_list = sorted(df_hist["developer_name"].unique())
        sel_dev = st.selectbox("Select Developer", dev_list)
        
        df_dev_hist = df_hist[df_hist["developer_name"] == sel_dev]
        
        if df_dev_hist.empty:
            st.info("No data for this developer.")
        else:
            # 4. Filter by Project
            projects = sorted(df_dev_hist["project_name"].unique())
            selected_proj = st.selectbox("Select Project", projects)
            
            # 5. Prepare Chart Data
            chart_data = df_dev_hist[df_dev_hist["project_name"] == selected_proj].copy()
            
            # Ensure date is datetime format for Streamlit charts
            chart_data["scraped_date"] = pd.to_datetime(chart_data["scraped_date"])
            
            # 6. Render Chart
            st.subheader(f"Sold Units Trend: {selected_proj}")
            
            # We don't need 'if y_col in...' checks because the DB column 'units_sold' is guaranteed
            st.line_chart(chart_data, x="scraped_date", y="units_sold")
            
            st.caption(f"Tracking metric: units_sold")

            # Optional: Show raw data table below chart
            with st.expander("View Raw Historical Data"):
                st.dataframe(chart_data, use_container_width=True)

# =========================================================
# DEBUG PANEL
# =========================================================
with st.expander("🛠 Debug Panel", expanded=False):
    st.write(f"Supabase Connection Active")
    st.write(f"Projects Loaded: {len(df_projects_all)}")




