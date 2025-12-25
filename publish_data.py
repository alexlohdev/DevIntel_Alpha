import os
import pandas as pd
import glob
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
DATA_DIR = "data/pemaju" 

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
    engine = get_engine()

    # 1. READ ALL CSV FILES
    # ---------------------------------------------------------
    # Finds the latest file for each category in the folders
    # Assumes structure: data/pemaju/DEVELOPER_NAME/....csv
    all_units = []
    all_projects = []
    all_houses = []

    for root, dirs, files in os.walk(DATA_DIR):
        for file in files:
            full_path = os.path.join(root, file)
            
            # Simple logic: Read everything. 
            # (Ideally, your scraper should clean the folder or you filter by date here)
            if "_MELAKA_UNIT_DETAILS_" in file:
                df = pd.read_csv(full_path)
                # Normalize columns to English for DB
                rename_map = {
                    "Kod Projek & Nama Projek": "project_name_raw", # Temp col to split later
                    "Kod Pemaju & Nama Pemaju": "pemaju_name",
                    "No. Permit": "permit_no",
                    "No Unit": "unit_no",
                    "Harga Jualan (RM)": "price_sales",
                    "Status Jualan": "status",
                    "Kuota Bumi": "bumi_quota",
                    "Scraped_Date": "scraped_date",
                    "Scraped_Timestamp": "scraped_timestamp"
                }
                # Only keep cols we need + rename
                df = df.rename(columns=rename_map)
                # Split Code and Name
                if "project_name_raw" in df.columns:
                    split = df["project_name_raw"].str.split(n=1, expand=True)
                    df["project_code"] = split[0]
                    df["project_name"] = split[1] if split.shape[1] > 1 else ""
                all_units.append(df)

            elif "_MELAKA_ALL_PROJECTS_" in file:
                df = pd.read_csv(full_path)
                rename_map = {
                    "Kod Projek & Nama Projek": "project_name_raw",
                    "Kod Pemaju & Nama Pemaju": "pemaju_name",
                    "Scraped_Date": "scraped_date",
                    "Scraped_Timestamp": "scraped_timestamp"
                    # Add other columns if you want them in projects_master
                }
                df = df.rename(columns=rename_map)
                if "project_name_raw" in df.columns:
                    split = df["project_name_raw"].str.split(n=1, expand=True)
                    df["project_code"] = split[0]
                    df["project_name"] = split[1] if split.shape[1] > 1 else ""
                all_projects.append(df)

            elif "_MELAKA_HOUSE_TYPE_" in file:
                df = pd.read_csv(full_path)
                # Add mapping for house type if needed
                all_houses.append(df)

    # Combine into single DataFrames
    df_units_final = pd.concat(all_units, ignore_index=True) if all_units else pd.DataFrame()
    df_projects_final = pd.concat(all_projects, ignore_index=True) if all_projects else pd.DataFrame()
    # df_houses_final... (similarly)

    if df_units_final.empty:
        print("⚠️ No unit data found. Aborting.")
        return

    # 2. UPDATE LIVE TABLES (WIPE & REPLACE)
    # ---------------------------------------------------------
    print("🔄 Updating Live Tables (Wiping old data)...")
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE units_detail RESTART IDENTITY;"))
        conn.execute(text("TRUNCATE TABLE projects_master RESTART IDENTITY;"))
        # conn.execute(text("TRUNCATE TABLE house_types RESTART IDENTITY;"))
    
    # Upload fresh data
    # (Ensure your DataFrame columns match Supabase columns exactly!)
    # You might need a final filter here to match specific DB columns
    # For this example, I assume your DF headers match the DB English headers we set up.
    
    # Selecting only columns that exist in DB to prevent errors
    valid_unit_cols = ["project_code", "project_name", "pemaju_name", "permit_no", "unit_no", "price_sales", "status", "bumi_quota", "scraped_date", "scraped_timestamp"]
    df_units_upload = df_units_final[valid_unit_cols].copy()
    
    df_units_upload.to_sql("units_detail", engine, if_exists="append", index=False)
    print(f"   -> Uploaded {len(df_units_upload)} rows to units_detail")
    
    # Do the same for projects_master...

    # 3. GENERATE & UPLOAD HISTORY LOGS
    # ---------------------------------------------------------
    print("📈 Generating History Logs...")
    
    # Calculate stats from the fresh df_units_final
    df_calc = df_units_final.copy()
    df_calc["is_sold"] = df_calc["status"].astype(str).str.lower().str.contains("telah dijual")
    df_calc["is_bumi"] = df_calc["bumi_quota"].astype(str).str.lower().str.strip() == "ya"
    df_calc["price"] = df_calc["price_sales"].apply(clean_money)

    # Group by Project
    history_df = df_calc.groupby(["project_code", "project_name", "pemaju_name", "scraped_date"], as_index=False).agg(
        total_units=("unit_no", "count"),
        units_sold=("is_sold", "sum"),
        units_bumi=("is_bumi", "sum"),
        sales_value=("price", lambda x: x[df_calc.loc[x.index, "is_sold"]].sum())
    )
    
    history_df["units_unsold"] = history_df["total_units"] - history_df["units_sold"]
    history_df["take_up_rate"] = (history_df["units_sold"] / history_df["total_units"]) * 100
    
    # Rename for DB
    history_df = history_df.rename(columns={"pemaju_name": "developer_name"})
    
    # Append to History Table (Do NOT truncate this one!)
    history_df.to_sql("history_logs", engine, if_exists="append", index=False)
    print(f"   -> Added {len(history_df)} logs to history_logs")

    print("✅ Done!")

if __name__ == "__main__":
    process_and_upload()
