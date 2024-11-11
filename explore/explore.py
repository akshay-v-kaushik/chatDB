import pandas as pd
from sqlalchemy import inspect
import config
from utils.common import select_table_or_collection

def explore_database(db_type, connections):
    if db_type == 'mysql':
        explore_mysql(connections[0])
    elif db_type == 'mongodb':
        explore_mongodb(connections[1])
    else:
        print("Unsupported db_type. Use 'mysql' or 'mongodb'.")

def explore_mysql(connection):
    inspector = inspect(connection)
    
    table_name = select_table_or_collection('mysql')
    if table_name:
        print(f"\nMetadata for table '{table_name}':")
        columns = inspector.get_columns(table_name)
        for column in columns:
            print(f"{column['name']} \t {column['type']}")
        
        df = pd.read_sql_table(table_name, connection)
        print(f"\nContents of table '{table_name}':")
        print(df.head())
    else:
        print("Invalid selection.")

def explore_mongodb(connection):
    db = connection[config.MYSQL_CONFIG['database']]
    
    collection_name = select_table_or_collection('mongodb')
    if collection_name:
        print(f"\nMetadata for collection '{collection_name}':")
        sample_doc = db[collection_name].find_one()
        if sample_doc:
            for key, value in sample_doc.items():
                print(f"{key} \t {type(value).__name__}")
        
        df = pd.DataFrame(list(db[collection_name].find()))
        print(f"\nContents of collection '{collection_name}':")
        print(df.head())
    else:
        print("Invalid selection.")