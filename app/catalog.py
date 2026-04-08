import os
import pandas as pd
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")

MANUAL_METADATA = {
    "jobs": {
        "name": "Ethiopian Airlines Local Job Posts",
        "source": "Ethiopian Airlines Official Website",
        "description": "Local job openings posted by Ethiopian Airlines.",
    },
    "injobs": {
        "name": "Ethiopian Airlines International Job Posts",
        "source": "Ethiopian Airlines Official Website",
        "description": "International job openings posted by Ethiopian Airlines.",
    },
    "result": {
        "name": "Ethiopian Airlines Exam & Hiring Results",
        "source": "Ethiopian Airlines Official Website",
        "description": "Published results for exams, interviews, and hiring stages.",
    },
    "ETB_fx": {
        "name": "Ethiopia Daily Exchange Rate (ETB)",
        "source": "National Bank of Ethiopia",
        "description": "Daily ETB exchange rates scraped from the official central bank website.",
    },
    "EGP_fx": {
        "name": "Egypt Daily Exchange Rate (EGP)",
        "source": "Central Bank of Egypt",
        "description": "Daily EGP exchange rates scraped from the official central bank website.",
    },
    "ZMW_fx": {
        "name": "Zambia Daily Exchange Rate (ZMW)",
        "source": "Bank of Zambia",
        "description": "Daily ZMW exchange rates scraped from the official central bank website.",
    },
    "DZD_fx": {
        "name": "Algeria Daily Exchange Rate (DZD)",
        "source": "Bank of Algeria",
        "description": "Daily DZD exchange rates scraped from the official central bank website.",
    },
    "NGN_fx": {
        "name": "Nigeria Daily Exchange Rate (NGN)",
        "source": "Central Bank of Nigeria",
        "description": "Daily NGN exchange rates scraped from the official central bank website.",
    },
    "jobs.parquet": {
        "name": "Jobs.et Job Listings",
        "source": "Jobs.et",
        "description": "Aggregated job postings scraped from Jobs.et covering multiple sectors and employers.",
    },
}


def get_dataframe(file_path, file_type):
    if file_type == "CSV":
        return pd.read_csv(file_path)
    elif file_type == "PARQUET":
        return pd.read_parquet(file_path)
    elif file_type == "JSONL":
        return pd.read_json(file_path, lines=True)
    return None


def build_catalog():
    catalog = []

    for filename in os.listdir(DATA_DIR):
        if not filename.endswith((".csv", ".parquet", ".jsonl")):
            continue

        file_path = os.path.join(DATA_DIR, filename)
        ext = filename.rsplit(".", 1)[1].lower()
        file_type = ext.upper() if ext != "jsonl" else "JSONL"

        # Match metadata — full filename first, then stem
        file_stem = filename.rsplit(".", 1)[0]
        manual = MANUAL_METADATA.get(filename) or MANUAL_METADATA.get(file_stem, {})

        # File size
        file_size_kb = round(os.path.getsize(file_path) / 1024, 1)

        # Last modified date
        last_updated = datetime.fromtimestamp(
            os.path.getmtime(file_path)
        ).strftime("%Y-%m-%d")

        # Row count + missing values
        try:
            df = get_dataframe(file_path, file_type)
            row_count = len(df)
            missing_values = int(df.isnull().sum().sum())
        except Exception:
            row_count = None
            missing_values = None

        catalog.append({
            "id": file_stem,
            "name": manual.get("name", file_stem.replace("_", " ").title()),
            "source": manual.get("source", "Unknown"),
            "description": manual.get("description", "No description available."),
            "file_type": file_type,
            "file_path": file_path,
            "file_size_kb": file_size_kb,
            "row_count": row_count,
            "missing_values": missing_values,
            "last_updated": last_updated,
        })

    return catalog