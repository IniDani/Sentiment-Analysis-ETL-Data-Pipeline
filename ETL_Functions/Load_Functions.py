import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.postgres_hook import PostgresHook
import urllib.parse
import logging

# The function below has no purpose, it's just for documentation purposes
def Load_Dataframe_To_PostgreSQL(df, table_name, host, port, user, password, database):
    # connection_string = "postgresql://DanFaRa%40rekdatsentimenanalysis:Qwerty123@rekdatsentimenanalysis.postgres.database.azure.com:5432/sentiment_analysis_etl?sslmode=require"
    # engine = create_engine(connection_string)
    # connection = engine.connect()
    # print("Connection successful!")
    # connection.close()

    db_config = {
        "username": user,
        "password": password,
        "host": host,
        "port": port,
        "database": database
    }

    # Create the database URL
    db_url = f"postgresql+psycopg2://{db_config['username']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}?sslmode=require"
    logging.info(f"Database connection URL: {db_url}")

    # Create the SQLAlchemy engine
    engine = create_engine(db_url, pool_pre_ping=True)

    try:
        # Check if the DataFrame is not empty
        if not df.empty:
            # Write DataFrame to PostgreSQL table in chunks
            df.to_sql(table_name, con = engine, index = False, if_exists="append", chunksize=1000)
            logging.info(f"Data successfully loaded into the {table_name} table.")
        else:
            logging.warning("DataFrame is empty. No data was loaded into the database.")

    except Exception as e:
        # Log the error in case of failure
        logging.error(f"Error loading data into PostgreSQL: {e}")
        raise
    finally:
        # Dispose of the engine to close connections
        engine.dispose()
        logging.info("Database connection closed.")
    
    # # Create a PostgreSQL connection string
    # encoded_password = urllib.parse.quote_plus(password)
    # # connection_string = f"postgresql://{user}:{encoded_password}@{host}:{port}/{database}?sslmode=require"
    # connection_string = "postgresql://DanFaRa@ekdatsentimenanalysis:Qwerty123@rekdatsentimenanalysis.postgres.database.azure.com:5432/sentiment_analysis_etl?sslmode=require"
    # # connection_string = "postgresql://ecycle_be@ecycle-db:JunPro16@ecycle-db.postgres.database.azure.com:5432/ecycle-db?sslmode=require"
    # logging.info(connection_string)

    # try:
    #     # Create the SQLAlchemy engine
    #     engine = create_engine(connection_string)
    #     with engine.connect() as connection:
    #         # Iterate through DataFrame rows and execute INSERT queries
    #         for index, row in df.iterrows():
    #             insert_query = f"""
    #             INSERT INTO {table_name} (content, context, sentiment_score)
    #             VALUES (%s, %s, %s);
    #             """
    #             connection.execute(insert_query, (row['Content'], row['Context'], row['Sentiment Score']))
    #         logging.info(f"Data successfully loaded into the {table_name} table.")
    # except Exception as e:
    #     logging.error(f"Error loading data into PostgreSQL: {e}")
    #     raise

    # try:
    #     # Create a SQLAlchemy engine
    #     engine = create_engine(connection_string)

    #     # Load the DataFrame into the PostgreSQL table, appending data to the existing table
    #     df.to_sql(table_name, engine, if_exists = 'append', index = False)
        
    #     logging.info(f"Data successfully loaded into the {table_name} table.")
    # except Exception as e:
    #     logging.error(f"Error loading data into PostgreSQL: {e}")




def Load_To_PostgreSQL(df, table_name, postgres_conn_id):
    try:
        # Initialize PostgresHook with the connection ID
        postgres_hook = PostgresHook(postgres_conn_id = postgres_conn_id)
        
        # Get the connection and cursor
        connection = postgres_hook.get_conn()
        cursor = connection.cursor()

        # Log the connection being used
        logging.info(f"Using Postgres connection: {postgres_conn_id}")
        
        # Iterate through DataFrame rows and execute INSERT queries
        for index, row in df.iterrows():
            insert_query = f"""
            INSERT INTO {table_name} (content, context, sentiment_score)
            VALUES (%s, %s, %s);
            """
            cursor.execute(insert_query, (row['Content'], row['Context'], row['Sentiment Score']))
        
        # Commit the transaction
        connection.commit()
        logging.info(f"Data successfully loaded into the {table_name} table.")

        # Close the cursor and connection
        cursor.close()
        connection.close()
    except Exception as e:
        logging.error(f"Error loading data into PostgreSQL: {e}")
        raise
