from ask.mongo_ask.mongo_globals_NLP import FIELD_MAPPING, KNOWN_STORE_LOCATIONS
from ask.mongo_ask.mongo_helpers_NLP import connect_to_db, gather_metrics
from ask.mongo_ask.mongo_query_parser_NLP import parse_query_nltk
from ask.mongo_ask.mongo_patterns_NLP import initialize_patterns
from pprintpp import pprint

def execute_query_mongo(collection, query):
    """Execute MongoDB query and return results."""
    try:
        if isinstance(query, list):
            return list(collection.aggregate(query))  # Aggregate pipeline queries
        elif isinstance(query, dict):
            return list(collection.find(query))  # Standard find queries
    except Exception as e:
        print(f"MongoDB Execution Error: {e}")
        return None


def run_cli(collection_name):
    """Run the CLI for MongoDB queries."""
    # print("Starting the CLI...")
    db = connect_to_db()  # Establish connection to MongoDB

    # Properly check if the database connection is None
    if db is None:
        print("Failed to connect to MongoDB. Exiting...")
        return

    # collection_name = "spotify"  # Replace with your collection name

    # Check if the collection exists
    if collection_name not in db.list_collection_names():
        print(f"Collection '{collection_name}' does not exist in the database. Exiting...")
        return

    # print(f"Connected to MongoDB. Using collection: {collection_name}")

    # Gather metrics for the collection
    table_info = gather_metrics(db, collection_name)
    # REMOVE
    pprint(KNOWN_STORE_LOCATIONS)
    pprint(table_info)
    # print(FIELD_MAPPING)
    # Initialize query patterns for MongoDB
    try:
        initialize_patterns(db, collection_name, table_info)
        # print(FIELD_MAPPING)
    except Exception as e:
        print(f"Error initializing patterns: {e}")
        return

    collection = db[collection_name]  # Access the collection
    # print("\nWelcome to ChatDB CLI for MongoDB! Type your query or 'exit' to quit.")

    while True:
        # try:
            user_input = input("Enter your query: ").strip().lower()
            if user_input in ['exit', 'quit']:
                print("Exiting CLI. Goodbye!")
                break

            # Parse the query
            query, description = parse_query_nltk(user_input, table_info)
            if query:
                print(f"Generated MongoDB Query: {query}")
                result = execute_query_mongo(collection, query)
                if result:
                    print("Query Results:")
                    for doc in result:
                        print(doc)
                else:
                    print("No matching records found.")
            else:
                print(description)
        # except Exception as e:
        #     print(f"Error processing query: {e}")


if __name__ == "__main__":
    run_cli()
