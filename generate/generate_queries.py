from utils.common import select_table_or_collection
from .sql_helpers import gather_metrics, get_random_sql, execute_and_print_query

def generate_mongodb(connection):
    print("Generating random MongoDB query... [Functionality to be implemented]")

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
        table_info = gather_metrics(connection, table_name)
        print("Randomly Generated Queries:")
        for _ in range(5): 
            query, description = get_random_sql(table_name, table_info)
            print(f"Query: {query}\nDescription: {description}\n")
            execute_and_print_query(connection, query)
    else:
        print("Invalid table selection.")

