import os
import pandas as pd
import sqlalchemy
import dotenv

# Load environment variables from .env file
dotenv.load_dotenv(dotenv_path=".env", override=True)

# Load the database URL from environment variables
DATABASE_URL = os.getenv('DATABASE_URL')
if not DATABASE_URL:
    raise ValueError("DATABASE_URL environment variable is not set")

# Create a database connection
engine = sqlalchemy.create_engine(DATABASE_URL)

# Query to fetch data
query = "SELECT * FROM historical_data ORDER BY timestamp"

# Execute the query and load data into a DataFrame
with engine.connect() as connection:
    df = pd.read_sql(query, connection)

# Save the data to a CSV file (Excel has a limit of ~1M rows)
output_file = "historical_data.csv"
df.to_csv(output_file, index=False)

print(f"Data has been saved to {output_file}")
print(f"Total rows: {len(df):,}")