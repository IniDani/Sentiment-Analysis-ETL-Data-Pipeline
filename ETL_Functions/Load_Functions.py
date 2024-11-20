import pandas as pd
from sqlalchemy import create_engine

def Load_Dataframe_To_PostgreSQL(df, table_name, host, port, user, password, database):
    # Create a PostgreSQL connection string
    connection_string = (
        f"postgresql://{user}:{password}@{host}:{port}/{database}"
    )

    try:
        # Create a SQLAlchemy engine
        engine = create_engine(connection_string)

        # Load the DataFrame into the PostgreSQL table, appending data to the existing table
        df.to_sql(table_name, engine, if_exists = 'append', index = False)
        
        print(f"Data successfully loaded into the {table_name} table.")
    except Exception as e:
        print(f"Error loading data into PostgreSQL: {e}")