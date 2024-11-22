from utils.common import select_table_or_collection
from .sql_helpers import get_random_sql, execute_and_print_sql, gather_sql_metrics
from .mongo_helpers import get_random_mongo, execute_and_print_mongo, gather_mongo_metrics
from pprintpp import pprint

def generate_random_query(db_type, connections):
    if db_type == 'mysql':
        generate_mysql(connections[0])
    elif db_type == 'mongodb':
        generate_mongodb(connections[1])
    else:
        print("Unsupported db_type. Use 'mysql' or 'mongodb'.")

def generate_mysql(connection):
    table_name = select_table_or_collection('mysql')
    
    if table_name:
        table_info = gather_sql_metrics(connection, table_name)
        # pprint(table_info)
        print("Randomly Generated Queries:")
        for _ in range(5): 
            query, description = get_random_sql(table_name, table_info)
            print(f"Query: {query}\nDescription: {description}\n")
            execute_and_print_sql(connection, query)
    else:
        print("Invalid table selection.")

def generate_mongodb(connection):
    collection_name = select_table_or_collection('mongodb')
    if collection_name:
        collection_info = gather_mongo_metrics(connection, collection_name)
        # pprint(collection_info)
        print("Randomly Generated Queries:")
        for _ in range(5): 
            query, description, query_obj = get_random_mongo(collection_name, collection_info)
            print(f"Query: {query}\nDescription: {description}")
            execute_and_print_mongo(connection, query_obj, collection_name)
    else:
        print("Invalid collection selection.")
