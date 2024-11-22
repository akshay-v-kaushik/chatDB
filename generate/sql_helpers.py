import config
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
        all_columns = list(table_info['numeric'].keys()) + list(table_info['categorical'].keys()) + list(table_info['date'].keys()) + list(table_info['others'])
        return random.choice(all_columns) if all_columns else None
    elif column_type == 'others':
        return random.choice(table_info['others']) if table_info['others'] else None
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

# Select a random query and generate based on types
def get_random_sql(table_name, table_info):
    max_attempts = len(query_templates)  # Prevent infinite loops
    attempts = 0

    while attempts < max_attempts:
        # Pick a random query template
        query_template = random.choice(query_templates)
        query_lambda = query_template[0]
        required_column_types = query_template[1] if len(query_template) > 1 else ['any']

        try:
            # Determine if multiple columns are required
            if 'columns' in query_lambda.__code__.co_varnames:
                columns = []
                for col_type_group in required_column_types:
                    selected_type = select_column_type_group(col_type_group, table_info)
                    if selected_type:
                        columns.append(selected_type)
                    else:
                        raise ValueError("No suitable column found for this type group.")
                
                # Handle additional parameters if needed
                additional_param = get_additional_param(
                    query_lambda, table_info, 
                    columns[1] if len(columns) > 1 else columns[0]
                )
                query, description = (
                    query_lambda(table_name, columns, additional_param)
                    if additional_param else query_lambda(table_name, columns)
                )
            
            else:
                # Single-column templates
                selected_type = select_column_type_group(required_column_types[0], table_info)
                if not selected_type:
                    raise ValueError("No suitable column found for this type group.")

                additional_param = get_additional_param(query_lambda, table_info, selected_type)
                query, description = (
                    query_lambda(table_name, selected_type, additional_param)
                    if additional_param else query_lambda(table_name, selected_type)
                )
            
            return query, description  # Successful query generation

        except ValueError:
            # Increment attempts and try another template
            attempts += 1

    # If no valid query is found after exhausting all templates
    return "No suitable query could be generated.", "All query templates failed for the given table."

    
def gather_sql_metrics(connection, table_name):
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
        cursor.execute(f"SELECT COUNT(DISTINCT `{column}`) FROM {table_name};")
        
        unique_values_count = cursor.fetchone()[0]
        
        unique_value_proportion = unique_values_count / total_rows if total_rows > 0 else 0
        # if "battery" in column.lower():
        #     print(column, unique_values_count, total_rows, unique_value_proportion)

        prop_map[column] = unique_value_proportion

        if (("id" in column.lower()  or
            unique_values_count == 1) and data_type not in numeric_types):
            table_info['others'].append(column)
            continue

        if data_type in numeric_types and unique_value_proportion >= config.NUMERIC_UNIQUE:
            cursor.execute(f"SELECT MIN(`{column}`), MAX(`{column}`) FROM {table_name};")
            min_value, max_value = cursor.fetchone()
            table_info['numeric'][column] = {'min': min_value, 'max': max_value}
        elif 'date' in data_type or 'time' in data_type:
            cursor.execute(f"SELECT MIN(`{column}`), MAX(`{column}`) FROM {table_name};")
            earliest, latest = cursor.fetchone()
            table_info['date'][column] = {'earliest': earliest, 'latest': latest}
        elif unique_value_proportion >= config.OTHERS_UNQIUE:
            table_info['others'].append(column)
        else:
            cursor.execute(f"SELECT DISTINCT `{column}` FROM {table_name};")
            unique_values = [row[0] for row in cursor.fetchall()]
            table_info['categorical'][column] = {'unique_values': unique_values}
        

    cursor.close()
    return table_info

# def execute_and_print_sql(connection, query):
    try:
        raw_connection = connection.raw_connection()
        cursor = raw_connection.cursor()

        cursor.execute(query)

        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()

        table = PrettyTable()
        table.field_names = columns  # Set column headers
        table.align = "l"  # Left-align the content

        if len(rows) > 30:
            for row in rows[:10]:
                table.add_row(row)
            table.add_row(["..."])
            for row in rows[-5:]:
                table.add_row(row)
        else:
            for row in rows:
                table.add_row(row)

        print(table)

        print(f"\n{len(rows)} row(s) in set")

    except Exception as err:
        print(f"Error: {err}")

def execute_and_print_sql(connection, query):
    try:
        raw_connection = connection.raw_connection()
        cursor = raw_connection.cursor()

        # Execute the query
        cursor.execute(query)

        # Fetch columns and rows
        columns = [desc[0] for desc in cursor.description] if cursor.description else []
        rows = cursor.fetchall()

        # Adjust rows to match column count
        adjusted_rows = []
        for row in rows:
            if len(row) < len(columns):
                # Add None for missing values
                adjusted_row = list(row) + [None] * (len(columns) - len(row))
            elif len(row) > len(columns):
                # Truncate extra values
                adjusted_row = row[:len(columns)]
            else:
                adjusted_row = row
            adjusted_rows.append(adjusted_row)

        # Initialize PrettyTable
        table = PrettyTable()
        table.field_names = columns  # Set column headers
        table.align = "l"  # Left-align the content

        # Handle large result sets
        if len(adjusted_rows) > 30:
            for row in adjusted_rows[:10]:
                table.add_row(row)
            table.add_row(["..."] * len(columns))  # Add "..." for skipped rows
            for row in adjusted_rows[-5:]:
                table.add_row(row)
        else:
            for row in adjusted_rows:
                table.add_row(row)

        # Print the table
        print(table)
        print(f"\n{len(adjusted_rows)} row(s) in set")

    except Exception as err:
        print(f"Error: {err}")
