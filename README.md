# chatDB

## Steps to Set It Up

1. **Clone the Repository**:

   ```sh
   git clone https://github.com/akshay-v-kaushik/chatDB.git
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

   - Create a [.env] file in the project root directory with the following content, replacing the placeholders with your actual database information:

     ```plaintext
     MYSQL_USER=your_mysql_username
     MYSQL_PASSWORD=your_mysql_password
     MYSQL_HOST=your_mysql_host
     MYSQL_DATABASE=your_mysql_database

     MONGODB_URI=your_mongodb_uri
     ```

## How to Run the Project

- **Run the CLI**:

  ```sh
  python drive.py
  ```

- **CLI Options**:
  - **Upload Dataset**: Upload a dataset to the selected database.
  - **Explore Database**: Explore the selected database.
  - **Generate Random Queries**: Generate random queries for the selected database.
  - **Natural Language Query**: Handle natural language queries for the selected database.
  - **Delete Dataset**: Deletes a specific table/collection of the user's choice.
  - **Exit**: Exit the CLI.
