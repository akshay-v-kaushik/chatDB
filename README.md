# chatDB

## Steps to Set It Up

1. **Clone the Repository**:

   ```sh
   git clone https://github.com/yourusername/chatDB.git
   cd chatDB
   ```

2. **Create a Virtual Environment**:

   ```sh
   python -m venv env
   ```

3. **Activate the Virtual Environment**:

   - On **Windows**:
     ```sh
     .\env\Scripts\activate
     ```
   - On **macOS/Linux**:
     ```sh
     source env/bin/activate
     ```

4. **Install Dependencies**:

   ```sh
   pip install -r requirements.txt
   ```

5. **Set Up Configuration**:

   - Replace the placeholders in the [config.py] file in the project root directory with your actual database information:

     ```python
     MYSQL_CONFIG = {
         'user': 'your_mysql_username',
         'password': 'your_mysql_password',
         'host': 'your_mysql_host',
         'database': 'your_mysql_database'
     }

     MONGODB_URI = 'your_mongodb_uri'
     ```

## How to Run the Project

- **Run the CLI**:

  ```sh
  python drive.py
  ```

- **CLI Options**:
  - **Upload Dataset**: Upload a dataset to the selected database.
  - **Explore Database**: Explore the selected database (functionality to be implemented).
  - **Generate Random Queries**: Generate random queries for the selected database (functionality to be implemented).
  - **Natural Language Query**: Handle natural language queries for the selected database (functionality to be implemented).
  - **Exit**: Exit the CLI.

### Explanation

- **Clone the Repository**: Clone the project repository from GitHub.
- **Create a Virtual Environment**: Create a virtual environment to manage project dependencies.
- **Activate the Virtual Environment**: Activate the virtual environment to use the installed dependencies.
- **Install Dependencies**: Install the required dependencies listed in [requirements.txt](http://_vscodecontentref_/2).
- **Set Up Configuration**: Create a [config.py](http://_vscodecontentref_/3) file with the necessary database configuration details.
- **Run the CLI**: Run the [drive.py](http://_vscodecontentref_/4) script to start the CLI and interact with the project.
- **CLI Options**: Use the CLI options to upload datasets, explore databases, generate random queries, handle natural language queries, and exit the CLI.
