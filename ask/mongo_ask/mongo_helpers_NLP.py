from pymongo import MongoClient
import re
from datetime import datetime
from ask.mongo_ask.mongo_globals_NLP import FIELD_MAPPING, KNOWN_STORE_LOCATIONS
import config
# Normalize date formats
def normalize_date(date_string):
    """Normalize natural language dates to MongoDB-compatible format."""
    date_string = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", date_string)  # Remove ordinal suffixes
    date_formats = ["%B %d, %Y", "%B %d %Y", "%Y-%m-%d", "%B %d"]  # Example formats
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_string, fmt)
            if "%Y" not in fmt:
                parsed_date = parsed_date.replace(year=datetime.now().year)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None

# Normalize location keywords
def normalize_location_from_keywords(keywords, known_store_locations):
    """Normalize location using combined keywords and known store locations."""
    combined_keywords = " ".join(keywords).lower()
    for key, normalized_location in known_store_locations.items():
        if key in combined_keywords:
            return normalized_location  # Return the full location name
    return None


def connect_to_db():
    """Establish connection to MongoDB."""
    try:
        # client = MongoClient("mongodb+srv://jahnavikiran21:1234@cluster0.lsgd7ew.mongodb.net/?retryWrites=true&w=majority")
        client = MongoClient(config.MONGODB_URI)
        db = client[config.MYSQL_CONFIG['database']]  # Your database name
        # print("Connected to MongoDB.")
        return db
    except Exception as e:
        print(f"MongoDB Connection Error: {e}")
        return None



def gather_metrics(db, collection_name):
    
    global FIELD_MAPPING, KNOWN_STORE_LOCATIONS

    # Clear previous mappings
    FIELD_MAPPING.clear()
    KNOWN_STORE_LOCATIONS.clear()

    # Get the collection
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
    documents = collection.find_one()
    # pprint(documents)
    # Analyze fields and determine types
    # doc = documents

    for column, value in documents.items():

        FIELD_MAPPING[column.lower()] = column

        # Detect and map special fields
        if re.search(r'(qty|quantity|count)', column.lower()):
            FIELD_MAPPING['quantity'] = column
        elif re.search(r'(price_usd|price|cost|amount)', column.lower()):
            FIELD_MAPPING['price'] = column
        elif re.search(r'(product|model)', column.lower()):
            FIELD_MAPPING['product'] = column
        elif re.search(r'(name|artist)', column.lower()):
            FIELD_MAPPING['name'] = column


        unique_values = collection.distinct(column)
        unique_values_count = len(unique_values)
        unique_value_proportion = unique_values_count / total_rows if total_rows > 0 else 0
        
        prop_map[column] = unique_value_proportion

        # Skip certain fields or handle them as 'others'
        if (("id" in column.lower() or unique_values_count == 1) and type(value) not in numeric_types):
            collection_info['others'].append(column)
            continue

        # Numeric columns
        if (type(value) in numeric_types and unique_value_proportion >= 0.11) or (isinstance(value, str) and len(value) >= 1 and value[0].isdigit() and "-" not in value and unique_value_proportion >= config.NUMERIC_UNIQUE):
            min_value = collection.find_one({column: { "$type": ["int", "long", "double", "decimal"], "$not": { "$eq": float('NaN') }}}, {column: 1, "_id": 0}, sort=[(column, 1)])[column]
            max_value = collection.find_one({column: { "$type": ["int", "long", "double", "decimal"], "$not": { "$eq": float('NaN') } }}, {column: 1, "_id": 0}, sort=[(column, -1)])[column]
            collection_info['numeric'][column] = {'min': min_value, 'max': max_value}

        # Date columns
        elif "date" in column.lower() or "time" in column.lower() or "year" in column.lower() or "month" in column.lower() or "day" in column.lower(): 
            earliest = collection.find_one({column: {"$type": "string"}}, {column: 1, "_id": 0}, sort=[(column, 1)])[column]
            latest = collection.find_one({column: {"$type": "string"}}, {column: 1, "_id": 0}, sort=[(column, -1)])[column]
            collection_info['date'][column] = {'earliest': earliest, 'latest': latest}

        # High proportion of unique values: classify as 'others'
        elif unique_value_proportion >= 0.46:
            collection_info['others'].append(column)

        # Categorical columns
        else:
            collection_info['categorical'][column] = {'unique_values': unique_values}
            if re.search(r'(location|store|branch)', column.lower()):
                    KNOWN_STORE_LOCATIONS.update({v.lower(): v for v in unique_values})

    return collection_info

# def gather_metrics(db, collection_name):
#     """Fetch and categorize metrics for MongoDB collections based on the first document."""
#     global FIELD_MAPPING, KNOWN_STORE_LOCATIONS

#     # Clear previous mappings
#     FIELD_MAPPING.clear()
#     KNOWN_STORE_LOCATIONS.clear()

#     # Get the collection
#     collection = db[collection_name]

#     # Retrieve only the first document for schema inference
#     sample_doc = collection.find_one()

#     if not sample_doc:
#         print(f"No documents found in collection '{collection_name}'. Returning empty schema.")
#         return {'numeric': [], 'categorical': [], 'date': [], 'others': []}

#     # Initialize structure to categorize fields
#     table_info = {'numeric': [], 'categorical': [], 'date': [], 'others': []}
#     numeric_types = (int, float)

#     for key, value in sample_doc.items():
#         # Map field names to lowercase for FIELD_MAPPING
#         FIELD_MAPPING[key.lower()] = key

#         # Detect and map special fields
#         if re.search(r'(qty|quantity|count)', key.lower()):
#             FIELD_MAPPING['quantity'] = key
#         elif re.search(r'(price_usd|price|cost|amount)', key.lower()):
#             FIELD_MAPPING['price'] = key
#         elif re.search(r'(product|model)', key.lower()):
#             FIELD_MAPPING['product'] = key
#         elif re.search(r'(name|artist)', key.lower()):
#             FIELD_MAPPING['name'] = key

#         # Categorize field types
#         if isinstance(value, numeric_types):
#             table_info['numeric'].append(key)
#         elif isinstance(value, str):
#             table_info['categorical'].append(key)

#             # Detect and map location-related fields
#             if re.search(r'(location|store|branch)', key.lower()):
#                 if key not in KNOWN_STORE_LOCATIONS:
#                     KNOWN_STORE_LOCATIONS[key.lower()] = key

#         elif isinstance(value, datetime):
#             table_info['date'].append(key)
#         else:
#             table_info['others'].append(key)

#     # Return the categorized schema
#     return table_info




