import os
import pandas as pd
import glob
from datetime import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ================= CONFIGURE THIS =================
# If running on GitHub Actions, use env vars. If local, use hardcoded strings (not recommended).
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "YOUR_DB_PASSWORD") 
DB_HOST = os.getenv("DB_HOST", "aws-0-ap-southeast-1.pooler.supabase.com")
DB_PORT = os.getenv("DB_PORT", "6543")
DB_NAME = os.getenv("DB_NAME", "postgres")

# Folder where your scraper saves CSVs
DATA_DIR = "KPKT_SCRAPED_DATA/data"  # Updated to match your scraper's ROOT_DIR

def get_engine():
    password = quote_plus(DB_PASS)
    url = f"postgresql+psycopg2://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    return create_engine(url)

def clean_money(val):
    """Converts 'RM 1,200.00' to float."""
    try:
        if pd.isna(val): return 0.0
        s = str(val).replace("RM", "").replace(",", "").strip()
        return float(s)
    except:
        return 0.0

def process_and_upload():
    print("🚀 Starting Publisher...")
    
    # 1. GET TODAY'S DATE STRING (Must match scraper's DATE_SUFFIX)
    # ---------------------------------------------------------
    today_str = datetime.now().strftime("%Y%m%d") # e.g., "20260120"
    print(f"📅 Looking for files dated: {today_str}")

    engine = get_engine()

    # 2. READ ONLY TODAY'S CSV FILES
    # ---------------------------------------------------------
    all_units = []
    all_projects = []
    all_houses = []

    files_found_count = 0

    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            # --- SAFETY FILTER: SKIP OLD FILES ---
            if today_str not in file:
                continue

            full_path = os.path.join(root, file)
            files_found_count += 1
            
            # --- A. UNIT DETAILS ---
            if "_MELAKA_UNIT_DETAILS_" in file:
                print(f"   Found Units: {file}")
                df = pd.read_csv(full_path)
                rename_map = {
                    "Kod Projek & Nama Projek": "project_name_raw",
                    "Kod Pemaju & Nama Pemaju": "pemaju_name",
                    "No. Permit": "permit_no",
                    "No Unit": "unit_no",
                    "Harga Jualan (RM)": "price_sales",
                    "Status Jualan": "status",
                    "Kuota Bumi": "bumi_quota",
                    "Scraped_Date": "scraped_date",
                    "Scraped_Timestamp": "scraped_timestamp"
                }
                df = df.rename(columns=rename_map)
                
                if "project_name_raw" in df.columns:
                    split = df["project_name_raw"].str.split(n=1, expand=True)
                    df["project_code"] = split[0]
                    df["project_name"] = split[1] if split.shape[1] > 1 else ""
                all_units.append(df)

            # --- B. PROJECTS MASTER ---
            elif "_MELAKA_ALL_PROJECTS_" in file:
                print(f"   Found Master: {file}")
                df = pd.read_csv(full_path)
                rename_map = {
                    "Kod Projek & Nama Projek": "project_name_raw",
                    "Kod Pemaju & Nama Pemaju": "pemaju_name",
                    "No. Permit": "permit_no",
                    "Status Projek Keseluruhan": "status_overall",
                    "Maklumat Pembangunan": "development_info",
                    "Daerah Projek": "location_district",
                    "Negeri Projek": "location_state",
                    "Tarikh Sah Laku Permit Terkini": "permit_valid_date",
                    "Scraped_Date": "scraped_date",
                    "Scraped_Timestamp": "scraped_timestamp"
                }
                df = df.rename(columns=rename_map)
                
                if "project_name_raw" in df.columns:
                    split = df["project_name_raw"].str.split(n=1, expand=True)
                    df["project_code"] = split[0]
                    df["project_name"] = split[1] if split.shape[1] > 1 else ""
                all_projects.append(df)

            # --- C. HOUSE TYPES ---
            elif "_MELAKA_HOUSE_TYPE_" in file:
                print(f"   Found House Types: {file}")
                df = pd.read_csv(full_path)
                all_houses.append(df)

    if files_found_count == 0:
        print(f"⚠️ No files found for date {today_str}. Please run the scraper first.")
        return

    # Combine into single DataFrames
    df_units_final = pd.concat(all_units, ignore_index=True) if all_units else pd.DataFrame()
    df_projects_final = pd.concat(all_projects, ignore_index=True) if all_projects else pd.DataFrame()
    df_houses_final = pd.concat(all_houses, ignore_index=True) if all_houses else pd.DataFrame()

    if df_units_final.empty:
        print("⚠️ Files found, but unit data is empty. Aborting.")
        return

    # 3. UPDATE LIVE TABLES (WIPE & REPLACE)
    # ---------------------------------------------------------
    print("🔄 Updating Live Tables (Wiping old data)...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE units_detail RESTART IDENTITY;"))
        conn.execute(text("TRUNCATE TABLE projects_master RESTART IDENTITY;"))
        conn.execute(text("TRUNCATE TABLE house_types RESTART IDENTITY;"))
    
    # --- 3a. UPLOAD UNITS DETAIL ---
    valid_unit_cols = ["project_code", "project_name", "pemaju_name", "permit_no", "unit_no", "price_sales", "status", "bumi_quota", "scraped_date", "scraped_timestamp"]
    cols_to_use = [c for c in valid_unit_cols if c in df_units_final.columns]
    df_units_upload = df_units_final[cols_to_use].copy()
    
    df_units_upload.to_sql("units_detail", engine, if_exists="append", index=False)
    print(f"   -> Uploaded {len(df_units_upload)} rows to units_detail")
    
    # --- 3b. UPLOAD PROJECTS MASTER ---
    if not df_projects_final.empty:
        valid_proj_cols = [
            "project_code", "project_name", "pemaju_name", "permit_no", 
            "status_overall", "development_info", "location_district", 
            "location_state", "permit_valid_date", "scraped_date", "scraped_timestamp"
        ]
        cols_to_use = [c for c in valid_proj_cols if c in df_projects_final.columns]
        df_projects_upload = df_projects_final[cols_to_use].copy()
        
        df_projects_upload.to_sql("projects_master", engine, if_exists="append", index=False)
        print(f"   -> Uploaded {len(df_projects_upload)} rows to projects_master")

    # --- 3c. UPLOAD HOUSE TYPES ---
    if not df_houses_final.empty:
         rename_house = {
            "Kod Projek": "project_code", "Nama Projek": "project_name", 
            "Jenis Rumah": "house_type", "Bil Tingkat": "num_floors", 
            "Bil Bilik": "num_rooms", "Bil Tandas": "num_bathrooms",
            "Keluasan Binaan (Mps)": "built_up_size", "Bil.Unit": "total_units",
            "Harga Minimum (RM)": "price_min", "Harga Maksimum (RM)": "price_max",
            "Peratus Sebenar %": "percent_actual", "Status Komponen": "component_status",
            "Tarikh CCC/CFO": "date_ccc_cfo", "Tarikh VP": "date_vp",
            "Scraped_Date": "scraped_date", "Scraped_Timestamp": "scraped_timestamp"
         }
         df_houses_final = df_houses_final.rename(columns=rename_house)
         
         valid_house_cols = list(rename_house.values())
         cols_to_use = [c for c in valid_house_cols if c in df_houses_final.columns]
         df_houses_upload = df_houses_final[cols_to_use].copy()

         df_houses_upload.to_sql("house_types", engine, if_exists="append", index=False)
         print(f"   -> Uploaded {len(df_houses_upload)} rows to house_types")


    # 4. GENERATE & UPLOAD HISTORY LOGS
    # ---------------------------------------------------------
    print("📈 Generating History Logs...")
    
    df_calc = df_units_final.copy()
    df_calc["is_sold"] = df_calc["status"].astype(str).str.lower().str.contains("telah dijual")
    df_calc["is_bumi"] = df_calc["bumi_quota"].astype(str).str.lower().str.strip() == "ya"
    df_calc["price"] = df_calc["price_sales"].apply(clean_money)

    history_df = df_calc.groupby(["project_code", "project_name", "pemaju_name", "scraped_date"], as_index=False).agg(
        total_units=("unit_no", "count"),
        units_sold=("is_sold", "sum"),
        units_bumi=("is_bumi", "sum"),
        sales_value=("price", lambda x: x[df_calc.loc[x.index, "is_sold"]].sum())
    )
    
    history_df["units_unsold"] = history_df["total_units"] - history_df["units_sold"]
    history_df["take_up_rate"] = (history_df["units_sold"] / history_df["total_units"]) * 100
    
    history_df = history_df.rename(columns={"pemaju_name": "developer_name"})
    
    # IMPORTANT: Filter duplicates before appending to History
    # This prevents adding the exact same log twice if you run the script 2x in one day
    existing_logs = pd.read_sql("SELECT project_code, scraped_date FROM history_logs", engine)
    
    if not existing_logs.empty and not history_df.empty:
        # Create a unique key for matching
        history_df["_key"] = history_df["project_code"].astype(str) + "_" + history_df["scraped_date"].astype(str)
        existing_logs["_key"] = existing_logs["project_code"].astype(str) + "_" + existing_logs["scraped_date"].astype(str)
        
        # Filter out rows that already exist
        initial_len = len(history_df)
        history_df = history_df[~history_df["_key"].isin(existing_logs["_key"])]
        history_df = history_df.drop(columns=["_key"])
        print(f"   -> Skipped {initial_len - len(history_df)} duplicate history logs")

    if not history_df.empty:
        history_df.to_sql("history_logs", engine, if_exists="append", index=False)
        print(f"   -> Added {len(history_df)} new logs to history_logs")
    else:
        print("   -> No new history logs to add.")

    print("✅ Done!")

if __name__ == "__main__":
    process_and_upload()
