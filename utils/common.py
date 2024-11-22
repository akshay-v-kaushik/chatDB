from sqlalchemy import create_engine, inspect
import pymongo
import config

def get_db_type():
    print("\nSelect Database System:")
    for idx, dbms in enumerate(config.DBMS_OPTIONS, start=1):
        print(f"{idx}. {dbms.capitalize()}")

    choice = input("Enter your choice: ")
    if choice in ["1", "2"]:
        return config.DBMS_OPTIONS[int(choice) - 1]
    else:
        print("Invalid selection. Defaulting to MySQL.")
        return "mysql"

def get_tables_or_collections(db_type):
    if db_type == 'mysql':
        return get_mysql_tables()
    elif db_type == 'mongodb':
        return get_mongodb_collections()
    else:
        print("Unsupported db_type. Use 'mysql' or 'mongodb'.")
        return []

def get_mysql_tables():
    connection_string = f"mysql+pymysql://{config.MYSQL_CONFIG['user']}:{config.MYSQL_CONFIG['password']}@{config.MYSQL_CONFIG['host']}/{config.MYSQL_CONFIG['database']}"
    engine = create_engine(connection_string)
    inspector = inspect(engine)
    return inspector.get_table_names()

def get_mongodb_collections():
    client = pymongo.MongoClient(config.MONGODB_URI)
    db = client[config.MYSQL_CONFIG['database']]
    return db.list_collection_names()

def select_table_or_collection(db_type):
    tables_or_collections = get_tables_or_collections(db_type)
    if not tables_or_collections:
        print("No tables or collections found.")
        return None

    item_type = "table" if db_type == 'mysql' else "collection"
    print(f"\n{db_type.capitalize()} {item_type}s:")
    for idx, item in enumerate(tables_or_collections, start=1):
        print(f"{idx}. {item}")

    choice = int(input(f"Select a {item_type}: ")) - 1
    if 0 <= choice < len(tables_or_collections):
        return tables_or_collections[choice]
    else:
        print("Invalid selection.")
        return None
