import pandas as pd
import pip
from sqlalchemy import create_engine, text
import numpy as np
import warnings


# --- 1. Read data and Preprocessing ---
FILE_PATH = "E:\\111zxy.ntu\\Database systems\\data10.6\\imdb_top_1000.csv"
TABLE_NAME = 'imdb_top_1000_movies'

print(f"Reading file: {FILE_PATH}")
df = pd.read_csv(FILE_PATH)

# Data cleaning：Process the 'Gross' column
# 1. Remove commas
# 2. Convert to nullable integer type (Int64)
df['Gross'] = df['Gross'].str.replace(',', '', regex=False)
df['Gross'] = pd.to_numeric(df['Gross'], errors='coerce').astype('Int64')

# Data preprocessing：The 'Released_Year' column
# Ensure all data is string to prevent potential import issues with non-year data
df['Released_Year'] = df['Released_Year'].astype(str)

print(f"Data ready. Total rows to import: {len(df)}")

# --- 2. MySQL Connection Information ---
DATABASE_TYPE = 'mysql'
DBAPI = 'mysqlconnector' 
USER = 'root'
PASSWORD = '123456'
HOST = 'localhost'
PORT = '3306'
DATABASE = 'imdb' # The name of the database you are importing into
# ----------------------------------------------------

# Construct the connection string
mysql_url = f"{DATABASE_TYPE}+{DBAPI}://{USER}:{PASSWORD}@{HOST}:{PORT}/{DATABASE}"

# Create SQLAlchemy engine
engine = None
try:
    engine = create_engine(mysql_url)
    print("2. SQLAlchemy Engine created successfully.")
except Exception as e:
    print(f"2. Error creating engine: {e}")
    exit()

# --- 4. Import DataFrame to Database (df.to_sql) ---
if engine:
    print(f"3. Attempting to write {len(df)} rows to MySQL table: {TABLE_NAME}")
    try:
        df.to_sql(
            name=TABLE_NAME,
            con=engine,
            if_exists='replace',  # Replace the existing table
            index=False,
            method='multi'  # Bulk insert for better efficiency
        )
        print("Data successfully loaded into MySQL.")
    except Exception as e:
        print(f"3. Error during database write. Check your MySQL configuration (max_allowed_packet, wait_timeout): {e}")
        # Stop further SQL processing if import fails
        exit()

# --- 5. In-Database SQL Preprocessing (Clean Runtime, Missing Value Imputation, Create Indexes) ---
sql_statements = [
    # A. Runtime Cleaning and Type Conversion
    f"""
    UPDATE `{TABLE_NAME}`
    SET Runtime = REPLACE(Runtime, ' min', '');
    """,
    f"""
    ALTER TABLE `{TABLE_NAME}`
    MODIFY COLUMN Runtime INT;
    """,

    # B. Missing Value Imputation (Meta_score: Fill with Average)
    f"SET @avg_meta_score = (SELECT AVG(Meta_score) FROM {TABLE_NAME});",
    f"""
    UPDATE `{TABLE_NAME}`
    SET Meta_score = @avg_meta_score
    WHERE Meta_score IS NULL;
    """,

    # C. Missing Value Imputation (Gross: Fill with Average)
    f"SET @avg_gross = (SELECT AVG(Gross) FROM {TABLE_NAME});",
    f"""
    UPDATE `{TABLE_NAME}`
    SET Gross = @avg_gross
    WHERE Gross IS NULL;
    """,

    # D. Missing Value Imputation (Certificate: Fill with 'Unknown')
    f"""
    UPDATE `{TABLE_NAME}`
    SET Certificate = 'Unknown'
    WHERE Certificate IS NULL;
    """,

    # E. Create Indexes (Speed up visualization queries)
    f"""
    CREATE INDEX idx_title ON `imdb_top_1000_movies` (Series_Title(255));
    """,
    f"""
    CREATE INDEX idx_rating ON `{TABLE_NAME}` (IMDB_Rating);
    """
]

# Execute SQL Preprocessing
if engine:
    with engine.connect() as connection:
        print("\n4. Starting SQL Preprocessing (Runtime, Missing Values, Indexes)...")
        for sql in sql_statements:
            # Simplified print for tracking
            statement_desc = sql.splitlines()[1].strip() if len(sql.splitlines()) > 1 else sql.strip()
            print(f"   Executing: {statement_desc}")
            try:
                connection.execute(text(sql))
                connection.commit()
            except Exception as e:
                # Ignore non-critical errors like "index already exists"
                if "already exists" not in str(e):
                    print(f"   ...Failed: {e}")

        print("\nSQL Preprocessing Complete. Data is ready for visualization queries.")