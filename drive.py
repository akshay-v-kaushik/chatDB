import sys
from config import DBMS_OPTIONS
from db_pusher import DatabasePusher
from upload.upload import upload_dataset


DBMS_OPTIONS = ["mysql", "mongodb"]

def explore_database(db_type):
    print(f"Exploring {db_type} databases... [Functionality to be implemented]")

def generate_random_query(db_type):
    print(f"Generating random queries for {db_type}... [Functionality to be implemented]")

def natural_language_query(db_type):
    print(f"Handling natural language query for {db_type}... [Functionality to be implemented]")


# helper function to get the database type from the user
def get_db_type():

    print("\nSelect Database System:")
    for idx, dbms in enumerate(DBMS_OPTIONS, start=1):
        print(f"{idx}. {dbms.capitalize()}")

    choice = input("Enter your choice: ")
    if choice in ["1", "2"]:
        return DBMS_OPTIONS[int(choice) - 1]
    else:
        print("Invalid selection. Defaulting to MySQL.")
        return "mysql"

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
    print("5. Exit")

def main():
    pusher = DatabasePusher()
    while True:
        display_menu()
        choice = input("Select an option: ")
        if choice == "1":
            db_type = get_db_type()
            upload_dataset(pusher, db_type)
        elif choice == "2":
            db_type = get_db_type()
            explore_database(db_type)
        elif choice == "3":
            db_type = get_db_type()
            generate_random_query(db_type)
        elif choice == "4":
            db_type = get_db_type()
            natural_language_query(db_type)
        elif choice == "5":
            exit_program()            
        else:
            print("Invalid selection. Please try again.")

if __name__ == "__main__":
    main()
