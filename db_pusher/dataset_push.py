import pandas as pd
from sqlalchemy import create_engine, types
import config
import os

class DatasetPusher:
    def __init__(self):
        self.mysql_config = config.MYSQL_CONFIG
        self.mongodb_uri = config.MONGODB_URI

    def push_mysql(self, dataset_file, connection):
        df = pd.read_csv(dataset_file)
        
        # Detect appropriate column types based on column name
        dtype_mapping = {}
        for column in df.columns:
            if 'date' in column.lower():
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
        print("Dataset pushed to MongoDB successfully.")

    def push_dataset(self, db_type, dataset_file, connections):
        if db_type == 'mysql':
            self.push_mysql(dataset_file, connections[0])
        elif db_type == 'mongodb':
            self.push_mongodb(dataset_file, connections[1])
        else:
            raise ValueError("Unsupported db_type. Use 'mysql' or 'mongodb'.")
