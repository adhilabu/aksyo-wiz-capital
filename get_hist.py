import os
import pandas as pd
import sqlalchemy

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

# Save the data to an Excel file
output_file = "historical_data.xlsx"
df.to_excel(output_file, index=False)

print(f"Data has been saved to {output_file}")