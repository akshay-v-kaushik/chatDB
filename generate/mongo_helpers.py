import random
import config
from .mongo_templates import query_templates
from pprintpp import pprint


# Helper function to select column based on type group (handles '/' options)
def select_column_type_group(col_type_group, collection_info):
    col_types = col_type_group.split('/')
    random.shuffle(col_types)  # Randomly reorder the list of column types
    for col_type in col_types:
        column = select_column(collection_info, col_type)
        if column:
            return column
    return None

# Select column based on specified type
def select_column(collection_info, column_type):
    if column_type == 'numeric' and 'numeric' in collection_info:
        return random.choice(list(collection_info['numeric'].keys()))
    elif column_type == 'categorical' and 'categorical' in collection_info:
        return random.choice(list(collection_info['categorical'].keys()))
    elif column_type == 'date' and 'date' in collection_info:
        return random.choice(list(collection_info['date'].keys()))
    elif column_type == 'any':
        all_columns = list(collection_info['numeric'].keys()) + list(collection_info['categorical'].keys()) + list(collection_info['date'].keys() + list(collection_info['others']))
        return random.choice(all_columns) if all_columns else None
    elif column_type == 'others':
        return random.choice(collection_info['others']) if collection_info['others'] else None
    return None

# Get additional parameters (min_max or date_range)
def get_additional_param(query_lambda, collection_info, column):
    if "min_max" in query_lambda.__code__.co_varnames:
        return get_min_max_for_column(collection_info, column)
    elif "date_range" in query_lambda.__code__.co_varnames:
        return get_date_range_for_column(collection_info, column)
    return None

def get_min_max_for_column(collection_info, column):
    if column in collection_info['numeric']:
        min_val = collection_info['numeric'][column]['min']
        max_val = collection_info['numeric'][column]['max']
        return (min_val, max_val)
    return None

def get_date_range_for_column(collection_info, column):
    if column in collection_info['date']:
        start_date = collection_info['date'][column]['earliest']
        end_date = collection_info['date'][column]['latest']
        return (start_date, end_date)
    return None

# Select a random query and generate based on types
def get_random_mongo(collection_name, collection_info):
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
                    selected_type = select_column_type_group(col_type_group, collection_info)
                    if selected_type:
                        columns.append(selected_type)
                    else:
                        raise ValueError("No suitable column found for this type group.")
                
                # Handle additional parameters if needed
                additional_param = get_additional_param(
                    query_lambda, collection_info, 
                    columns[1] if len(columns) > 1 else columns[0]
                )
                query, description, query_obj = (
                    query_lambda(collection_name, columns, additional_param)
                    if additional_param else query_lambda(collection_name, columns)
                )
            
            else:
                # Single-column templates
                selected_type = select_column_type_group(required_column_types[0], collection_info)
                if not selected_type:
                    raise ValueError("No suitable column found for this type group.")

                additional_param = get_additional_param(query_lambda, collection_info, selected_type)
                query, description, query_obj = (
                    query_lambda(collection_name, selected_type, additional_param)
                    if additional_param else query_lambda(collection_name, selected_type)
                )
            
            return query, description, query_obj  # Successful query generation

        except ValueError:
            # Increment attempts and try another template
            attempts += 1

    # If no valid query is found after exhausting all templates
    return "No suitable query could be generated.", "All query templates failed for the given table."



def gather_mongo_metrics(connection, collection_name):
    db_name = config.MYSQL_CONFIG['database']
    db = connection[db_name]
    collection = db[collection_name]
    
    total_rows = collection.count_documents({})
    
    # Initialize the structure to store field information
    collection_info = {
        'numeric': {},
        'categorical': {},
        'date': {},
        'others': []
    }
    numeric_types = [int, float]
    prop_map = {}

    # Get all documents to infer schema
    documents = list(collection.find())
    # pprint(documents)
    # Analyze fields and determine types
    doc = documents[0] if documents else {}
    for column, value in doc.items():
        unique_values = collection.distinct(column)
        unique_values_count = len(unique_values)
        unique_value_proportion = unique_values_count / total_rows if total_rows > 0 else 0
        
        prop_map[column] = unique_value_proportion

        # Skip certain fields or handle them as 'others'
        if (("id" in column.lower() or unique_values_count == 1) and type(value) not in numeric_types):
            collection_info['others'].append(column)
            continue

        # Numeric columns
        if (type(value) in numeric_types and unique_value_proportion >= config.NUMERIC_UNIQUE) or (isinstance(value, str) and len(value) >= 1 and value[0].isdigit() and "-" not in value and unique_value_proportion >= config.NUMERIC_UNIQUE):
            min_value = collection.find_one({column: { "$type": ["int", "long", "double", "decimal"], "$not": { "$eq": float('NaN') }}}, {column: 1, "_id": 0}, sort=[(column, 1)])[column]
            max_value = collection.find_one({column: { "$type": ["int", "long", "double", "decimal"], "$not": { "$eq": float('NaN') } }}, {column: 1, "_id": 0}, sort=[(column, -1)])[column]
            collection_info['numeric'][column] = {'min': min_value, 'max': max_value}

        # Date columns
        elif "date" in column.lower() or "time" in column.lower() or "year" in column.lower() or "month" in column.lower() or "day" in column.lower(): 
            earliest = collection.find_one({column: {"$type": "string"}}, {column: 1, "_id": 0}, sort=[(column, 1)])[column]
            latest = collection.find_one({column: {"$type": "string"}}, {column: 1, "_id": 0}, sort=[(column, -1)])[column]
            collection_info['date'][column] = {'earliest': earliest, 'latest': latest}

        # High proportion of unique values: classify as 'others'
        elif unique_value_proportion >= config.OTHERS_UNQIUE:
            collection_info['others'].append(column)

        # Categorical columns
        else:
            collection_info['categorical'][column] = {'unique_values': unique_values}

    return collection_info

def execute_and_print_mongo(connection, query_object, collection_name):
    try:
        # Extract database and method
        db_name = config.MYSQL_CONFIG['database']
        db = connection[db_name]
        collection = db[collection_name]
        
        method = query_object["method"]
        
        # Execute the query based on the method
        if method == "find":
            cursor = collection.find(query_object["query"], query_object.get("projection", {}))
            # Apply modifiers
            if "modifiers" in query_object:
                for mod, args in query_object["modifiers"].items():
                    cursor = getattr(cursor, mod)(args)
            result = list(cursor)
        elif method == "aggregate":
            result = list(collection.aggregate(query_object["pipeline"]))
        elif method == "distinct":
            result = collection.distinct(query_object["query"])
        else:
            raise ValueError(f"Unsupported query method: {method}")

        # Print the results
        if len(result) > 30:
            print("Showing the first 15 rows:")
            pprint(result[:10])  # Print the first 10 rows
            print(f"Total number of rows: {len(result)}")
        else:
            pprint(result)  # Print all rows
            print(f"Total number of rows: {len(result)}")


    except Exception as e:
        print(f"Error executing query: {e}")