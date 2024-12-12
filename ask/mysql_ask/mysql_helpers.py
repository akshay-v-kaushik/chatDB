import re
from datetime import datetime
import mysql.connector
from ask.mysql_ask.mysql_globals import FIELD_MAPPING, KNOWN_STORE_LOCATIONS  # Ensure globals are imported for shared state
import config
def normalize_date(date_string):
    """Normalize natural language dates to SQL-compatible format."""
    # Remove ordinal suffixes (e.g., "1st" -> "1")
    date_string = re.sub(r"(\d+)(st|nd|rd|th)", r"\1", date_string)
    date_formats = ["%B %d, %Y", "%B %d %Y", "%B %d"]  # Supported formats
    for fmt in date_formats:
        try:
            parsed_date = datetime.strptime(date_string, fmt)
            # If the year is missing, use the current year
            if "%Y" not in fmt:
                parsed_date = parsed_date.replace(year=datetime.now().year)
            return parsed_date.strftime("%Y-%m-%d")
        except ValueError:
            continue
    return None


def normalize_location_from_keywords(keywords):
    """Normalize location using combined keywords."""
    combined_keywords = " ".join(keywords).lower()
    for key, normalized_location in KNOWN_STORE_LOCATIONS.items():
        if key in combined_keywords:
            return normalized_location  # Return the full location name
    return None  # Return None if no match is found


def gather_metrics(connection, table_name):
    """Fetch and categorize table metrics dynamically."""
    global FIELD_MAPPING, KNOWN_STORE_LOCATIONS  # Ensure globals are updated
    
    # Define patterns or keywords for detecting location-related columns
    location_patterns = re.compile(r"(location|branch|area|city)", re.IGNORECASE)

    try:
        cursor = connection.cursor()
        cursor.execute(
            "SELECT COLUMN_NAME, DATA_TYPE FROM information_schema.columns WHERE table_name = %s and table_schema = %s;",
            (table_name,'chatDB'))

        schema = cursor.fetchall()
        # print(f"Schema fetched: {schema}")
        # print(schema)
        cursor.execute(f"SELECT COUNT(*) FROM {table_name};")
        total_rows = cursor.fetchone()[0]
        # print(f"Total rows in table: {total_rows}")

        # Structure for table info
        table_info = {
            'numeric': {},
            'categorical': {},
            'date': {},
            'others': []
        }
        numeric_types = ['int', 'bigint', 'float', 'double', 'decimal']

        # Reset FIELD_MAPPING and KNOWN_STORE_LOCATIONS
        FIELD_MAPPING.clear()
        KNOWN_STORE_LOCATIONS.clear()

        # Process schema
        for column, data_type in schema:
            # print(f"Processing column: {column}, Data type: {data_type}")
            FIELD_MAPPING[column.lower()] = column  # Map lower-case field names
            try:
                cursor.execute(f"SELECT COUNT(DISTINCT {column}) FROM {table_name};")
                unique_values_count = cursor.fetchone()[0]
                # print(f"Unique values in column {column}: {unique_values_count}")
            except Exception as e:
                # print(f"Error fetching unique values for column {column}: {e}")
                continue

            # Identify quantity and price fields
            if re.search(r'(qty|quantity|count)', column.lower()):
                FIELD_MAPPING['quantity'] = column
            elif re.search(r'(price|cost|amount)', column.lower()) and data_type in numeric_types:
                FIELD_MAPPING['price'] = column
            elif re.search(r'(product|model)', column.lower()):
                FIELD_MAPPING['product'] = column
            elif re.search(r'(name|artist)', column.lower()):
                FIELD_MAPPING['name'] = column

            # Dynamically detect store_location-like columns
            if location_patterns.search(column.lower()):
                try:
                    cursor.execute(f"SELECT DISTINCT {column} FROM {table_name};")
                    locations = [row[0] for row in cursor.fetchall()]
                    # print(f"Locations detected in column {column}: {locations}")
                    table_info['categorical'][column] = {'unique_values': locations}
                    KNOWN_STORE_LOCATIONS.update({str(loc).lower(): str(loc) for loc in locations if isinstance(loc, str)})
                    FIELD_MAPPING['store_location'] = column  # Map the detected column for location-related queries
                except Exception as e:
                    # print(f"Error processing store_location-like column {column}: {e}")
                    continue

            # Numeric fields
            elif data_type in numeric_types:
                try:
                    cursor.execute(f"SELECT MIN({column}), MAX({column}) FROM {table_name};")
                    min_value, max_value = cursor.fetchone()
                    table_info['numeric'][column] = {'min': min_value, 'max': max_value}
                    # print(f"Numeric field {column}: min={min_value}, max={max_value}")
                except Exception as e:
                    print(f"Error processing numeric field {column}: {e}")
                    continue

            # Date fields
            elif 'date' in data_type or 'time' in data_type:
                try:
                    cursor.execute(f"SELECT MIN({column}), MAX({column}) FROM {table_name};")
                    earliest, latest = cursor.fetchone()
                    table_info['date'][column] = {'earliest': earliest, 'latest': latest}
                    # print(f"Date field {column}: earliest={earliest}, latest={latest}")
                except Exception as e:
                    print(f"Error processing date field {column}: {e}")
                    continue

            # Other categorical fields
            else:
                try:
                    cursor.execute(f"SELECT DISTINCT {column} FROM {table_name} LIMIT 100;")
                    unique_values = [row[0] for row in cursor.fetchall()]
                    table_info['categorical'][column] = {'unique_values': unique_values}
                    # print(f"Categorical field {column}: unique_values={unique_values}")
                except Exception as e:
                    print(f"Error processing categorical field {column}: {e}")
                    continue

        return table_info

    except mysql.connector.Error as err:
        print(f"Database error: {err}")
    except Exception as e:
        print(f"Unexpected error in gather_metrics: {e}")
    finally:
        if cursor:
            cursor.close()
        # print("Cursor closed.")
    # return None



def connect_to_database():
    """Establish connection to the MySQL database."""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="1234",
            database="chatDB",
        )
        # print("Connection to the database was successful.")
        return connection
    except mysql.connector.Error as err:
        print(f"Connection error: {err}")
        return None


def log_error(message):
    """Log errors to a file."""
    with open('error_log.txt', 'a') as log_file:
        log_file.write(f"{message}\n")


def log_query(query, result):
    """Log executed queries and their results."""
    with open('query_log.txt', 'a') as log_file:
        log_file.write(f"Query: {query}, Result: {result}\n")
