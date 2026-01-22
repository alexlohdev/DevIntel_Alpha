import os
import pandas as pd
import glob
from datetime import datetime
from sqlalchemy import create_engine, text
from urllib.parse import quote_plus

# ================= CONFIGURE THIS =================
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASS", "YOUR_DB_PASSWORD") 
DB_HOST = os.getenv("DB_HOST", "aws-0-ap-southeast-1.pooler.supabase.com")
DB_PORT = os.getenv("DB_PORT", "6543")
DB_NAME = os.getenv("DB_NAME", "postgres")
DATA_DIR = "data"

def get_engine():
    password = quote_plus(DB_PASS)
    url = f"postgresql+psycopg2://{DB_USER}:{password}@{DB_HOST}:{DB_PORT}/{DB_NAME}?sslmode=require"
    return create_engine(url)

def clean_money(val):
    try:
        if pd.isna(val): return 0.0
        s = str(val).replace("RM", "").replace(",", "").strip()
        return float(s)
    except:
        return 0.0

def process_and_upload():
    print("🚀 Starting Publisher...")
    today_str = datetime.now().strftime("%Y%m%d") 
    print(f"📅 Looking for files dated: {today_str}")

    engine = get_engine()
    all_units, all_projects, all_houses = [], [], []
    files_found = 0

    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            if today_str not in file: continue
            
            full_path = os.path.join(root, file)
            files_found += 1
            
            try:
                # --- A. UNIT DETAILS ---
                if "_MELAKA_UNIT_DETAILS_" in file:
                    print(f"   Found Units: {file}")
                    df = pd.read_csv(full_path)
                    if df.empty: continue
                    
                    df = df.rename(columns={
                        "Kod Projek & Nama Projek": "project_name_raw",
                        "Kod Pemaju & Nama Pemaju": "pemaju_name",
                        "No. Permit": "permit_no",
                        "No Unit": "unit_no",
                        "Harga Jualan (RM)": "price_sales",
                        "Status Jualan": "status",
                        "Kuota Bumi": "bumi_quota",
                        "Scraped_Date": "scraped_date",
                        "Scraped_Timestamp": "scraped_timestamp"
                    })

                    # SAFE SPLIT LOGIC
                    if "project_name_raw" in df.columns:
                        # Ensure string type and handle NaNs
                        df["project_name_raw"] = df["project_name_raw"].astype(str)
                        # Split safely
                        split_data = df["project_name_raw"].str.split(n=1, expand=True)
                        
                        if split_data.shape[1] >= 2:
                            df["project_code"] = split_data[0]
                            df["project_name"] = split_data[1]
                        elif split_data.shape[1] == 1:
                            df["project_code"] = split_data[0]
                            df["project_name"] = ""
                        else:
                            df["project_code"] = df["project_name_raw"]
                            df["project_name"] = ""
                            
                    all_units.append(df)

                # --- B. PROJECTS MASTER ---
                elif "_MELAKA_ALL_PROJECTS_" in file:
                    print(f"   Found Master: {file}")
                    df = pd.read_csv(full_path)
                    if df.empty: continue

                    df = df.rename(columns={
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
                    })
                    
                    if "project_name_raw" in df.columns:
                        df["project_name_raw"] = df["project_name_raw"].astype(str)
                        split_data = df["project_name_raw"].str.split(n=1, expand=True)
                        if split_data.shape[1] >= 2:
                            df["project_code"] = split_data[0]
                            df["project_name"] = split_data[1]
                        else:
                            df["project_code"] = df["project_name_raw"]
                            df["project_name"] = ""

                    all_projects.append(df)

                # --- C. HOUSE TYPES ---
                elif "_MELAKA_HOUSE_TYPE_" in file:
                    print(f"   Found House Types: {file}")
                    df = pd.read_csv(full_path)
                    if not df.empty: all_houses.append(df)

            except Exception as e:
                print(f"⚠️ Error processing {file}: {e}")

    if files_found == 0:
        print(f"⚠️ No files found for {today_str}. Scraper might not have run yet.")
        return

    # Combine
    df_units_final = pd.concat(all_units, ignore_index=True) if all_units else pd.DataFrame()
    df_projects_final = pd.concat(all_projects, ignore_index=True) if all_projects else pd.DataFrame()
    df_houses_final = pd.concat(all_houses, ignore_index=True) if all_houses else pd.DataFrame()

    if df_units_final.empty:
        print("⚠️ No unit data to upload.")
        return

    # UPLOAD
    print("🔄 Updating Live Tables...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE units_detail RESTART IDENTITY;"))
        conn.execute(text("TRUNCATE TABLE projects_master RESTART IDENTITY;"))
        conn.execute(text("TRUNCATE TABLE house_types RESTART IDENTITY;"))
    
    # (Upload logic same as before, simplified for brevity...)
    valid_unit_cols = ["project_code", "project_name", "pemaju_name", "permit_no", "unit_no", "price_sales", "status", "bumi_quota", "scraped_date", "scraped_timestamp"]
    df_units_final[[c for c in valid_unit_cols if c in df_units_final.columns]].to_sql("units_detail", engine, if_exists="append", index=False)
    print("   -> Uploaded Units")

    if not df_projects_final.empty:
        valid_proj_cols = ["project_code", "project_name", "pemaju_name", "permit_no", "status_overall", "development_info", "location_district", "location_state", "permit_valid_date", "scraped_date", "scraped_timestamp"]
        df_projects_final[[c for c in valid_proj_cols if c in df_projects_final.columns]].to_sql("projects_master", engine, if_exists="append", index=False)
        print("   -> Uploaded Projects")

    if not df_houses_final.empty:
        rename_house = { "Kod Projek": "project_code", "Nama Projek": "project_name", "Jenis Rumah": "house_type", "Bil Tingkat": "num_floors", "Bil Bilik": "num_rooms", "Bil Tandas": "num_bathrooms", "Keluasan Binaan (Mps)": "built_up_size", "Bil.Unit": "total_units", "Harga Minimum (RM)": "price_min", "Harga Maksimum (RM)": "price_max", "Peratus Sebenar %": "percent_actual", "Status Komponen": "component_status", "Tarikh CCC/CFO": "date_ccc_cfo", "Tarikh VP": "date_vp", "Scraped_Date": "scraped_date", "Scraped_Timestamp": "scraped_timestamp" }
        df_houses_final = df_houses_final.rename(columns=rename_house)
        df_houses_final[[c for c in rename_house.values() if c in df_houses_final.columns]].to_sql("house_types", engine, if_exists="append", index=False)
        print("   -> Uploaded House Types")

    # HISTORY LOGS
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

    # Deduplicate
    existing_logs = pd.read_sql("SELECT project_code, scraped_date FROM history_logs", engine)
    if not existing_logs.empty and not history_df.empty:
        history_df["_key"] = history_df["project_code"].astype(str) + "_" + history_df["scraped_date"].astype(str)
        existing_logs["_key"] = existing_logs["project_code"].astype(str) + "_" + existing_logs["scraped_date"].astype(str)
        history_df = history_df[~history_df["_key"].isin(existing_logs["_key"])].drop(columns=["_key"])

    if not history_df.empty:
        history_df.to_sql("history_logs", engine, if_exists="append", index=False)
        print(f"   -> Added {len(history_df)} new logs")
    else:
        print("   -> No new history logs to add.")

    print("✅ Done!")

if __name__ == "__main__":
    process_and_upload()
