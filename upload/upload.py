import os

def validate_dataset(dataset_path):

    if not os.path.isfile(dataset_path):
        print("Error: The specified path does not point to a file.")
        return False
    if not dataset_path.lower().endswith('.csv') and not dataset_path.lower().endswith('.json'):
        print("Error: Only CSV and JSON files are supported.")
        return False
    try:
        # Attempt to open the file to check readability
        with open(dataset_path, 'r') as file:
            pass
    except Exception as e:
        print(f"Error: Could not read the file. {e}")
        return False
    return True

def upload_dataset(pusher, db_type, connections):
    dataset_path = input("Enter the path to the dataset file: ")
    if validate_dataset(dataset_path):
        pusher.pusher.push_dataset(db_type, dataset_path, connections)
    else:
        print("Dataset validation failed.")