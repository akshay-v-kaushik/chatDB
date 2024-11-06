import pandas as pd
from sqlalchemy import create_engine, inspect
import pymongo
import config

def explore_database(db_type):
    if db_type == 'mysql':
        explore_mysql()
    elif db_type == 'mongodb':
        explore_mongodb()
    else:
        print("Unsupported db_Stype. Use 'mysql' or 'mongodb'.")

def explore_mysql():
    connection_string = f"mysql+pymysql://{config.MYSQL_CONFIG['user']}:{config.MYSQL_CONFIG['password']}@{config.MYSQL_CONFIG['host']}/{config.MYSQL_CONFIG['database']}"
    engine = create_engine(connection_string)
    inspector = inspect(engine)
    
    tables = inspector.get_table_names()
    print("Tables in MySQL database:")
    for idx, table in enumerate(tables, start=1):
        print(f"{idx}. {table}")
    
    choice = int(input("Select a table to view metadata and contents: ")) - 1
    if 0 <= choice < len(tables):
        table_name = tables[choice]
        print(f"\nMetadata for table '{table_name}':")
        columns = inspector.get_columns(table_name)
        for column in columns:
            print(f"{column['name']} \t {column['type']}")
        
        df = pd.read_sql_table(table_name, engine)
        print(f"\nContents of table '{table_name}':")
        print(df.head())
    else:
        print("Invalid selection.")

def explore_mongodb():
    client = pymongo.MongoClient(config.MONGODB_URI)
    db = client[config.MYSQL_CONFIG['database']]
    
    collections = db.list_collection_names()
    print("Collections in MongoDB database:")
    for idx, collection in enumerate(collections, start=1):
        print(f"{idx}. {collection}")
    
    choice = int(input("Select a collection to view metadata and contents: ")) - 1
    if 0 <= choice < len(collections):
        collection_name = collections[choice]
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