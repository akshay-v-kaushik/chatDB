import random
from utils.common import select_table_or_collection
from sqlalchemy import inspect
from .templates import query_templates
from .sql_helpers import select_column_type_group, get_additional_param, gather_metrics

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
    inspector = inspect(connection)
    table_name = select_table_or_collection('mysql')
    
    if table_name:
        table_info = gather_metrics(connection, table_name)
        print("Randomly Generated Queries:")
        for _ in range(5): 
            query, description = get_random_sql(table_name, table_info)
            print(f"Query: {query}\nDescription: {description}\n")
    else:
        print("Invalid table selection.")

# Select a random query and generate based on types
def get_random_sql(table_name, table_info):
    query_template = random.choice(query_templates)
    query_lambda = query_template[0]
    required_column_types = query_template[1] if len(query_template) > 1 else ['any']
    
    # Determine if multiple columns are required
    if 'columns' in query_lambda.__code__.co_varnames:
        columns = []
        for col_type_group in required_column_types:
            selected_type = select_column_type_group(col_type_group, table_info)
            if selected_type:
                columns.append(selected_type)
            else:
                return "No suitable column found.", "No suitable column found for this query template."

        # Handle additional parameters if needed
        additional_param = get_additional_param(query_lambda, table_info, columns[1] if len(columns) > 1 else columns[0])
        query, description = query_lambda(table_name, columns, additional_param) if additional_param else query_lambda(table_name, columns)
    
    else:
        # Single-column templates
        selected_type = select_column_type_group(required_column_types[0], table_info)
        if not selected_type:
            return "No suitable column found.", "No suitable column found for this query template."

        additional_param = get_additional_param(query_lambda, table_info, selected_type)
        query, description = query_lambda(table_name, selected_type, additional_param) if additional_param else query_lambda(table_name, selected_type)
    
    return query, description

