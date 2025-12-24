import os
import shutil
import glob
import pandas as pd
from datetime import datetime

# =========================
# CONFIG
# =========================
SOURCE_DIR = "KPKT_SCRAPED_DATA/data"  # Raw Scraper Output
DEST_DIR = "data/pemaju"               # Dashboard Production Data
HISTORY_FILE = "data/history_tracker.csv" # <--- NEW: The Time Machine

def get_latest_file(folder, substring):
    """Finds the latest CSV in a folder matching the substring."""
    files = glob.glob(os.path.join(folder, f"*{substring}*.csv"))
    if not files: return None
    # Sort by name (YYYYMMDD) descending to get latest
    return sorted(files, reverse=True)[0]

def update_history(dest_folder, dev_name):
    """
    Reads the just-published unit file and appends stats to history.csv
    """
    unit_file = get_latest_file(dest_folder, "_MELAKA_UNIT_DETAILS_")
    
    if not unit_file: return

    try:
        # 1. Read Data
        df = pd.read_csv(unit_file)
        
        # 2. Extract Timestamp
        if "Scraped_Date" in df.columns:
            scrape_date = df["Scraped_Date"].iloc[0]
        else:
            scrape_date = datetime.now().strftime("%Y-%m-%d")

        # 3. Calculate Stats per Project
        # We group by Project because a developer has multiple projects
        if "Kod Projek & Nama Projek" not in df.columns: return

        # Normalize Status
        df["status_lower"] = df["Status Jualan"].astype(str).str.lower()
        df["is_sold"] = df["status_lower"].str.contains("telah dijual")
        df["is_unsold"] = df["status_lower"].str.contains("belum dijual")

        stats = df.groupby("Kod Projek & Nama Projek").agg({
            "No Unit": "count",
            "is_sold": "sum",
            "is_unsold": "sum"
        }).reset_index()

        # 4. Prepare History Rows
        history_rows = []
        for _, row in stats.iterrows():
            total = row["No Unit"]
            sold = row["is_sold"]
            unsold = row["is_unsold"]
            take_up = (sold / total * 100) if total > 0 else 0

            history_rows.append({
                "Date": scrape_date,
                "Developer": dev_name,
                "Project": row["Kod Projek & Nama Projek"],
                "Total_Units": total,
                "Sold_Units": sold,
                "Unsold_Units": unsold,
                "Take_Up_Rate": round(take_up, 1)
            })

        # 5. Append to History File
        if history_rows:
            new_df = pd.DataFrame(history_rows)
            
            # Check if file exists to determine if we need a header
            file_exists = os.path.exists(HISTORY_FILE)
            
            # Append mode 'a', header only if file is new
            new_df.to_csv(HISTORY_FILE, mode='a', header=not file_exists, index=False)
            print(f"   📈 Logged history for {len(history_rows)} projects.")

    except Exception as e:
        print(f"   ⚠️ Failed to update history for {dev_name}: {e}")

def publish():
    print("🚀 Starting Data Publish & History Log...")
    
    if not os.path.exists(SOURCE_DIR):
        print(f"❌ Source directory not found: {SOURCE_DIR}")
        return

    # Ensure data dir exists for the history file
    os.makedirs("data", exist_ok=True)

    count = 0
    for root, dirs, files in os.walk(SOURCE_DIR):
        # We only care about the root of each developer folder
        if root == SOURCE_DIR: continue 

        dev_name = os.path.basename(root)
        
        # 1. Standard Publish (Copy Files)
        rel_path = os.path.relpath(root, SOURCE_DIR)
        dest_folder = os.path.join(DEST_DIR, rel_path)
        os.makedirs(dest_folder, exist_ok=True)
        
        files_copied = False
        for file in files:
            if file.endswith(".csv"):
                src_file = os.path.join(root, file)
                dst_file = os.path.join(dest_folder, file)
                shutil.copy2(src_file, dst_file)
                files_copied = True
        
        if files_copied:
            print(f"✅ Published: {dev_name}")
            count += 1
            # 2. Update History (The New Feature)
            update_history(dest_folder, dev_name)

    print(f"\n🎉 Done! Published {count} developers.")
    print(f"📊 History tracked in: {HISTORY_FILE}")

if __name__ == "__main__":
    publish()