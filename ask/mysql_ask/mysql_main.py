from ask.mysql_ask.mysql_helpers import connect_to_database, log_query, gather_metrics
from ask.mysql_ask.mysql_query_parser import parse_query_nltk
from ask.mysql_ask.mysql_globals import FIELD_MAPPING, KNOWN_STORE_LOCATIONS
from ask.mysql_ask.mysql_patterns import initialize_patterns
import mysql.connector
def execute_query(connection, query):
    """Execute SQL query and return results."""
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        return cursor.fetchall()
    except mysql.connector.Error as err:
        print(f"SQL Execution Error: {err}")
        log_query(query, f"Error: {err}")
        return None
    except Exception as e:
        print(f"Unexpected error during query execution: {e}")
        log_query(query, f"Unexpected error: {e}")
        return None

def run_cli(table_name):
    """Run the CLI for user queries."""
    # Connect to the database
    connection = connect_to_database()
    if not connection:
        return

    # table_name = "student_data"

    # Gather metrics and initialize patterns
    table_info = gather_metrics(connection, table_name)
    # print("Field Mapping (FIELD_MAPPING):", FIELD_MAPPING)  # Debugging
    # print("Known Store Locations (KNOWN_STORE_LOCATIONS):", KNOWN_STORE_LOCATIONS)  # Debugging
    # print("Table Info:", table_info)
    # print("Initializing patterns...")
    initialize_patterns(connection, table_name, table_info)

    # print("Welcome to ChatDB CLI! Type your query or 'exit' to quit.")
    while True:
        user_input = input("Enter your query: ").strip().lower()
        if user_input in ['exit', 'quit']:
            break

        # Debugging FIELD_MAPPING state before parsing
        # print("Debug: FIELD_MAPPING before parsing:", FIELD_MAPPING)

        query, description = parse_query_nltk(user_input)
        if query:
            print(f"Generated SQL Query: {query}")
            result = execute_query(connection, query)

            if result:
                # Handle queries with multiple rows (e.g., GROUP BY or LIMIT)
                if isinstance(result, list):  # Check if result is a list of rows
                    for row in result:
                        # Print each row (e.g., for top 5 students)
                        print(f"{row[0]}: {row[1]}")
                else:
                    # Single result, handle as before
                    print(f"{result[0]}")

                # Log the query and result
                log_query(query, result)
            else:
                print("No matching records found.")
        else:
            print(description)

    connection.close()
    # print("Database connection closed.")

if __name__ == "__main__":
    run_cli()
