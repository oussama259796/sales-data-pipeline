import pandas as pd
import logging
import os
from sqlalchemy import create_engine, text

# -------------------------
# Logging Configuration
# -------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filename="sales_data.log"
)

# -------------------------
# Database Engine (reuse connection)
# -------------------------
engine = create_engine(
    f"postgresql+psycopg2://postgres:{os.getenv('PGPASSWORD')}@localhost:5432/postgres"
)

# -------------------------
# Extract
# -------------------------
def load_sales_data(file_path):
    """
    Load CSV file into pandas DataFrame
    """
    try:
        df = pd.read_csv(file_path)
        logging.info(f"Loaded {len(df)} records from file")
        return df
    except FileNotFoundError:
        logging.error(f"File not found: {file_path}")
        return None
    except Exception as e:
        logging.error(f"Error loading data: {e}")
        return None


# -------------------------
# Transform & Validate
# -------------------------
def transform_sales_data(df):
    """
    Clean and validate data:
    - Remove invalid rows
    - Convert data types safely
    - Normalize text fields
    """
    try:
        # Convert amount safely (invalid → NaN)
        df["amount"] = pd.to_numeric(df["amount"], errors="coerce")

        # Convert date safely
        df["sale_date"] = pd.to_datetime(df["sale_date"], errors="coerce")

        # Drop invalid rows
        df = df.dropna(subset=["amount", "client", "sale_date"])

        # Business rules
        df = df[df["amount"] > 0]

        # Clean text
        df["client"] = df["client"].str.strip().str.title()

        # Remove duplicates inside file
        df = df.drop_duplicates(subset=["client", "sale_date"])

        logging.info(f"Transformed data: {len(df)} valid records remaining")
        return df

    except Exception as e:
        logging.error(f"Error transforming data: {e}")
        return None


# -------------------------
# Incremental Load Helper
# -------------------------
def get_last_sale_date():
    """
    Get the latest sale_date from database
    Used for incremental loading
    """
    try:
        with engine.connect() as conn:
            result = conn.execute(text("SELECT MAX(sale_date) FROM sales"))
            last_date = result.scalar()
            return last_date
    except Exception as e:
        logging.warning(f"Could not fetch last sale_date: {e}")
        return None


# -------------------------
# Load (with incremental logic)
# -------------------------
def load_to_postgres(df):
    """
    Load data into PostgreSQL:
    - Only insert new records (incremental load)
    - Avoid duplicates
    """
    try:
        last_date = get_last_sale_date()

        if last_date:
            df = df[df["sale_date"] > last_date]
            logging.info(f"Incremental load: {len(df)} new records")

        if df.empty:
            logging.info("No new data to insert")
            return

        # Insert into database
        df.to_sql("sales", engine, if_exists="append", index=False)

        logging.info(f"{len(df)} records inserted into PostgreSQL")

    except Exception as e:
        logging.error(f"Error loading data to PostgreSQL: {e}")


# -------------------------
# Pipeline Orchestration
# -------------------------
def run_pipeline():
    """
    Full ETL pipeline:
    1. Extract
    2. Transform
    3. Load
    """
    file_name = "large_sales_data.csv"

    # Extract
    data = load_sales_data(file_name)

    if data is not None:
        # Transform
        clean_data = transform_sales_data(data)

        if clean_data is not None:
            # Load
            load_to_postgres(clean_data)

            print("🚀 Pipeline executed successfully. Check logs for details.")


# -------------------------
# Entry Point
# -------------------------
if __name__ == "__main__":
    run_pipeline()
