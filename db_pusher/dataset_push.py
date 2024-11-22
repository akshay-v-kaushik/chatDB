import pandas as pd
from sqlalchemy import types
import config
import os
from sqlalchemy.dialects.mysql import BIGINT
from sqlalchemy import inspect, text
from sqlalchemy.orm import sessionmaker


class DatasetPusher:
    def __init__(self):
        self.mysql_config = config.MYSQL_CONFIG
        self.mongodb_uri = config.MONGODB_URI

    def push_mysql(self, dataset_file, connection):
        df = pd.read_csv(dataset_file)
        
        # Detect appropriate column types based on column name
        dtype_mapping = {}
        for column in df.columns:
            sample_value = str(df[column].dropna().iloc[0])
            if (len(sample_value) >= 1 and sample_value[0].isdigit() and "-" not in sample_value) or (len(sample_value) > 1 and sample_value[1].isdigit() and "-" not in sample_value[1:]):  # Check if the second char is a digit
                if '.' in sample_value:  # Check for decimal
                    dtype_mapping[column] = types.FLOAT
                else:  # Otherwise, it's an integer
                    dtype_mapping[column] = BIGINT
            elif 'date' in column.lower():
                # Convert to DATETIME if it seems to contain both date and time information
                if len(df[column].dropna().astype(str).iloc[0]) > 10:
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                    dtype_mapping[column] = types.DATETIME
                else:
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.date
                    dtype_mapping[column] = types.DATE
            elif 'year' in column.lower():
                # Convert to YEAR if length is appropriate, otherwise use DATE or DATETIME
                if len(df[column].dropna().astype(str).iloc[0]) <= 5:
                    df[column] = pd.to_datetime(df[column], errors='coerce').dt.year
                    dtype_mapping[column] = types.YEAR
                else:
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                    dtype_mapping[column] = types.DATE
            elif 'time' in column.lower():
                # Convert to TIME if the column contains only time values
                if len(df[column].dropna().astype(str).iloc[0]) < 10:
                    df[column] = pd.to_datetime(df[column], format='%H:%M:%S', errors='coerce').dt.time
                    dtype_mapping[column] = types.TIME
                else:
                    df[column] = pd.to_datetime(df[column], errors='coerce')
                    dtype_mapping[column] = types.DATETIME

        # Extract table name from file name
        table_name = os.path.splitext(os.path.basename(dataset_file))[0]

        # Push the DataFrame to MySQL with detected column types
        df.to_sql(name=table_name, con=connection, if_exists='replace', index=False, dtype=dtype_mapping)
        
        self.cleanup_null_rows(connection, table_name)
        print("Dataset pushed to MySQL successfully with appropriate data types.")

    def push_mongodb(self, dataset_file, connection):
        if dataset_file.endswith('.csv'):
            df = pd.read_csv(dataset_file)
        elif dataset_file.endswith('.json'):
            df = pd.read_json(dataset_file)
        else:
            raise ValueError("Unsupported file format. Use CSV or JSON.")
        
        db_name = self.mysql_config['database']
        collection_name = os.path.splitext(os.path.basename(dataset_file))[0]
        db = connection[db_name]
        collection = db[collection_name]
        collection.insert_many(df.to_dict('records'))

        self.cleanup_nan_rows(connection, db_name, collection_name)

        
        print("Dataset pushed to MongoDB successfully.")


    def cleanup_null_rows(self, engine, table_name):
        try:
            # Create a session
            Session = sessionmaker(bind=engine)
            session = Session()

            # Inspect the table structure to get column names
            inspector = inspect(engine)
            columns = [col['name'] for col in inspector.get_columns(table_name)]

            # Build the DELETE query
            null_conditions = " OR ".join([f"{col} IS NULL" for col in columns])
            delete_query = text(f"DELETE FROM {table_name} WHERE {null_conditions};")  # Use text() here

            # Execute the query
            result = session.execute(delete_query)
            session.commit()

            print(f"Deleted {result.rowcount} rows containing NULL values.")
            return result.rowcount
        except Exception as e:
            print(f"Error: {e}")
            return 0
        finally:
            session.close()


    def cleanup_nan_rows(self, connection, db_name, collection_name):
        # Connect to the database and collection
        db = connection[db_name]
        collection = db[collection_name]

        # Query to match rows with NaN in any field
        query = {
            "$or": [
                {field: {"$eq": float('NaN')}} for field in collection.find_one().keys() if field != "_id"
            ]
        }

        # Delete matching rows
        result = collection.delete_many(query)
        
        print(f"Deleted {result.deleted_count} documents containing NaN values.")
        return result.deleted_count

    def push_dataset(self, db_type, dataset_file, connections):
        if db_type == 'mysql':
            self.push_mysql(dataset_file, connections[0])
        elif db_type == 'mongodb':
            self.push_mongodb(dataset_file, connections[1])
        else:
            raise ValueError("Unsupported db_type. Use 'mysql' or 'mongodb'.")
