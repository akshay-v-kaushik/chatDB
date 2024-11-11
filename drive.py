import sys
from db_pusher import DatabasePusher
from upload.upload import upload_dataset
from explore.explore import explore_database
from generate.generate_queries import generate_random_query
from utils.common import get_db_type
from utils.connect import DatabaseConnector
from drop.drop import delete_dataset

def natural_language_query(db_type):
    print(f"Handling natural language query for {db_type}... [Functionality to be implemented]")

# helper function to exit the program
def exit_program():
    print("Exiting the program. Goodbye!")
    sys.exit()

# helper function to display the main menu
def display_menu():
    print("\n---- ChatDB CLI ----")
    print("1. Upload Dataset")
    print("2. Explore Database")
    print("3. Generate Random Queries")
    print("4. Natural Language Query")
    print("5. Delete Dataset")
    print("6. Exit")

def main():
    connections = DatabaseConnector()
    connections.connect_all()
    pusher = DatabasePusher()
    while True:
        display_menu()
        choice = input("Select an option: ")

        if choice == "1":
            db_type = get_db_type()
            upload_dataset(pusher, db_type, connections.connections)
        elif choice == "2":
            db_type = get_db_type()
            explore_database(db_type, connections.connections)
        elif choice == "3":
            db_type = get_db_type()
            generate_random_query(db_type, connections.connections)
        elif choice == "4":
            db_type = get_db_type()
            natural_language_query(db_type)
        elif choice == "5":
            db_type = get_db_type()
            delete_dataset(db_type, connections.connections)
        elif choice == "6":
            exit_program()                
        else:
            print("Invalid selection. Please try again.")

if __name__ == "__main__":
    main()