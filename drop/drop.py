from utils.common import select_table_or_collection
import config

def delete_mysql_dataset(connection):
    table_name = select_table_or_collection('mysql')
    if table_name:
        cursor = connection.raw_connection().cursor()
        cursor.execute(f"DROP TABLE {table_name};")
        connection.raw_connection().commit()
        cursor.close()
        print(f"Table '{table_name}' has been deleted.")
    else:
        print("Invalid selection.")

def delete_mongodb_dataset(connection):
    db = connection[config.MYSQL_CONFIG['database']]
    collection_name = select_table_or_collection('mongodb')
    if collection_name:
        db.drop_collection(collection_name)
        print(f"Collection '{collection_name}' has been deleted.")
    else:
        print("Invalid selection.")

def delete_dataset(db_type, connections):
    if db_type == 'mysql':
        delete_mysql_dataset(connections[0])
    elif db_type == 'mongodb':
        delete_mongodb_dataset(connections[1])
    else:
        print("Unsupported db_type. Use 'mysql' or 'mongodb'.")

