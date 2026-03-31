# Sales Data Pipeline (Python + PostgreSQL) 🚀

A production-style ETL pipeline that automates sales data processing.

## What it does:
- Reads sales data from CSV files
- Cleans and validates data automatically
- Removes duplicates (file + database level)
- Loads data into PostgreSQL
- Uses incremental loading (only new records inserted)

## Tech Stack:
Python | Pandas | PostgreSQL | SQLAlchemy | Logging

## Pipeline Steps:
- **Extract:** Read CSV sales data
- **Transform:** Validate amounts, clean names, parse dates
- **Load:** Insert new records only (incremental)

## Features:
- ✅ Incremental loading (no reprocessing old data)
- ✅ Data validation & cleaning
- ✅ Duplicate prevention (Python + Database level)
- ✅ Automated logging to sales_data.log
- ✅ Secure password via ENV variable

## Sample Log Output:
2026-03-31 17:47:40,033 - INFO - Loaded 100000 records from file
2026-03-31 17:47:40,186 - INFO - Transformed data: 72159 valid records remaining
2026-03-31 17:47:40,296 - INFO - Incremental load: 0 new records
2026-03-31 17:47:40,296 - INFO - No new data to insert


## How to Run:

### 1. Install dependencies:
```bash
pip install -r requirements.txt
```

### 2. Set environment variable:
```bash
export PGPASSWORD=your_password
```

### 3. Run pipeline:
```bash
python main.py
```

## Database Setup:
```sql
ALTER TABLE sales
ADD CONSTRAINT unique_sale UNIQUE (client, sale_date);
```

## Use Case:
This pipeline helps businesses:
- Replace manual Excel work
- Automate data updates
- Prepare data for dashboards and reports

---
📩 Open to freelance work | Data Engineering & ETL Pipelines
🔗 github.com/oussama259796
