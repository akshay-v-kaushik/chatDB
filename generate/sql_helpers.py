import random
from datetime import timedelta

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

def random_number(min_val, max_val): 
    if isinstance(min_val, int) and isinstance(max_val, int):
        # Return an integer within the range
        return random.randint(min_val, max_val)
    elif isinstance(min_val, float) or isinstance(max_val, float):
        # Return a decimal within the range
        return random.uniform(min_val, max_val)
    else:
        raise ValueError("min_val and max_val must be either int or float")

def random_date(start_date, end_date):
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return start_date + timedelta(days=random_days)

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
