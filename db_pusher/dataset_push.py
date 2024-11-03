import pandas as pd
from sqlalchemy import create_engine
import pymongo
import config
import os

class DatasetPusher:
    def __init__(self):
        self.mysql_config = config.MYSQL_CONFIG
        self.mongodb_uri = config.MONGODB_URI

    def push_mysql(self, dataset_file):
        df = pd.read_csv(dataset_file)
        table_name = os.path.splitext(os.path.basename(dataset_file))[0]
        connection_string = f"mysql+pymysql://{self.mysql_config['user']}:{self.mysql_config['password']}@{self.mysql_config['host']}/{self.mysql_config['database']}"
        engine = create_engine(connection_string)
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print("Dataset pushed to MySQL successfully.")

    def push_mongodb(self, dataset_file):
        if dataset_file.endswith('.csv'):
            df = pd.read_csv(dataset_file)
        elif dataset_file.endswith('.json'):
            df = pd.read_json(dataset_file)
        else:
            raise ValueError("Unsupported file format. Use CSV or JSON.")
        
        client = pymongo.MongoClient(self.mongodb_uri)
        db_name = self.mysql_config['database']
        collection_name = os.path.splitext(os.path.basename(dataset_file))[0]
        db = client[db_name]
        collection = db[collection_name]
        collection.insert_many(df.to_dict('records'))
        print("Dataset pushed to MongoDB successfully.")

    def push_dataset(self, db_type, dataset_file):
        if db_type == 'mysql':
            self.push_mysql(dataset_file)
        elif db_type == 'mongodb':
            self.push_mongodb(dataset_file)
        else:
            raise ValueError("Unsupported db_type. Use 'mysql' or 'mongodb'.")