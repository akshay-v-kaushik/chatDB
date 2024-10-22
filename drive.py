import sys
from config import MYSQL_CONFIG, MONGODB_URI

# Global list of supported DBMS types
DBMS_OPTIONS = ["mysql", "mongodb"]

def upload_dataset(db_type):
    """Dummy function for dataset upload."""
    print(f"Uploading dataset to {db_type}... [Functionality to be implemented]")

def explore_database(db_type):
    """Dummy function for database exploration."""
    print(f"Exploring {db_type} databases... [Functionality to be implemented]")

def generate_random_query(db_type):
    """Dummy function for generating random queries."""
    print(f"Generating random queries for {db_type}... [Functionality to be implemented]")

def natural_language_query(db_type):
    """Dummy function for handling natural language queries."""
    print(f"Handling natural language query for {db_type}... [Functionality to be implemented]")

def get_db_type():
    """Prompt user to select between MySQL or MongoDB."""
    print("\nSelect Database System:")
    for idx, dbms in enumerate(DBMS_OPTIONS, start=1):
        print(f"{idx}. {dbms.capitalize()}")

    choice = input("Enter your choice: ")
    if choice in ["1", "2"]:
        return DBMS_OPTIONS[int(choice) - 1]
    else:
        print("Invalid selection. Defaulting to MySQL.")
        return "mysql"

def exit_program():
    """Exit the CLI program."""
    print("Exiting the program. Goodbye!")
    sys.exit()

def display_menu():
    """Display the main menu options."""
    print("\n---- ChatDB CLI ----")
    print("1. Upload Dataset")
    print("2. Explore Database")
    print("3. Generate Random SQL Queries")
    print("4. Generate Random MongoDB Queries")
    print("5. Natural Language Query")
    print("6. Exit")

def main():
    """Main function to run the CLI."""
    while True:
        display_menu()
        choice = input("Select an option: ")

        if choice == "1":
            db_type = get_db_type()
            upload_dataset(db_type)
        elif choice == "2":
            db_type = get_db_type()
            explore_database(db_type)
        elif choice == "3":
            db_type = get_db_type()
            generate_random_query(db_type)
        elif choice == "4":
            db_type = get_db_type()
            generate_random_query(db_type)  # Same logic for MongoDB query generation
        elif choice == "5":
            db_type = get_db_type()
            natural_language_query(db_type)
        elif choice == "6":
            exit_program()
        else:
            print("Invalid selection. Please try again.")

if __name__ == "__main__":
    main()
