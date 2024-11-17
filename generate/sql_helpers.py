import random
from .sql_templates import query_templates
from prettytable import PrettyTable

# Helper function to select column based on type group (handles '/' options)
def select_column_type_group(col_type_group, table_info):
    col_types = col_type_group.split('/')
    random.shuffle(col_types)  # Randomly reorder the list of column types
    for col_type in col_types:
        column = select_column(table_info, col_type)
        if column:
            return column
    return None

# Select column based on specified type
def select_column(table_info, column_type):
    if column_type == 'numeric' and 'numeric' in table_info:
        return random.choice(list(table_info['numeric'].keys()))
    elif column_type == 'categorical' and 'categorical' in table_info:
        return random.choice(list(table_info['categorical'].keys()))
    elif column_type == 'date' and 'date' in table_info:
        return random.choice(list(table_info['date'].keys()))
    elif column_type == 'any':
        all_columns = list(table_info['numeric'].keys()) + list(table_info['categorical'].keys()) + list(table_info['date'].keys())
        return random.choice(all_columns) if all_columns else None
    return None

# Get additional parameters (min_max or date_range)
def get_additional_param(query_lambda, table_info, column):
    if "min_max" in query_lambda.__code__.co_varnames:
        return get_min_max_for_column(table_info, column)
    elif "date_range" in query_lambda.__code__.co_varnames:
        return get_date_range_for_column(table_info, column)
    return None

def get_min_max_for_column(table_info, column):
    if column in table_info['numeric']:
        min_val = table_info['numeric'][column]['min']
        max_val = table_info['numeric'][column]['max']
        return (min_val, max_val)
    return None

def get_date_range_for_column(table_info, column):
    if column in table_info['date']:
        start_date = table_info['date'][column]['earliest']
        end_date = table_info['date'][column]['latest']
        return (start_date, end_date)
    return None

def gather_metrics(connection, table_name):
    raw_connection = connection.raw_connection()
    cursor = raw_connection.cursor()
    
    # Step 1: Fetch column names, data types, and total rows in the table
    cursor.execute(f"SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE table_name = '{table_name}';")
    schema = cursor.fetchall()
    cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
    total_rows = cursor.fetchone()[0]
    
    # Structure to store column information
    table_info = {
        'numeric': {},
        'categorical': {},
        'date': {},
        'others': [] 
    }
    numeric_types = ['int', 'bigint', 'float', 'double', 'decimal']
    prop_map = {}

    # Step 2: Collect additional information for each column based on its type
    for column, data_type in schema:
        cursor.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table_name};")
        
        unique_values_count = cursor.fetchone()[0]
        
        unique_value_proportion = unique_values_count / total_rows if total_rows > 0 else 0
        prop_map[column] = unique_value_proportion

        if ("id" in column.lower() or "key" in column.lower() or
            unique_values_count == 1 or
            (unique_value_proportion >= 0.75 and data_type not in numeric_types)):
            table_info['others'].append(column)
            continue

        if data_type in numeric_types and unique_value_proportion >= 0.2:
            cursor.execute(f"SELECT MIN({column}), MAX({column}) FROM {table_name};")
            min_value, max_value = cursor.fetchone()
            table_info['numeric'][column] = {'min': min_value, 'max': max_value}
        elif 'date' in data_type or 'time' in data_type:
            cursor.execute(f"SELECT MIN({column}), MAX({column}) FROM {table_name};")
            earliest, latest = cursor.fetchone()
            table_info['date'][column] = {'earliest': earliest, 'latest': latest}
        else:
            cursor.execute(f"SELECT DISTINCT {column} FROM {table_name};")
            unique_values = [row[0] for row in cursor.fetchall()]
            table_info['categorical'][column] = {'unique_values': unique_values}

    cursor.close()
    return table_info

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
    
    ### add randomly limit to the query (10)

    return query, description

from prettytable import PrettyTable

def execute_and_print_query(connection, query):
    try:
        raw_connection = connection.raw_connection()
        cursor = raw_connection.cursor()

        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()

        table = PrettyTable()
        table.field_names = columns  # Set column headers
        table.align = "l"  # Left-align the content

        for row in rows:
            table.add_row(row)

        print(table)

        print(f"\n{len(rows)} row(s) in set")

    except Exception as err:
        print(f"Error: {err}")
