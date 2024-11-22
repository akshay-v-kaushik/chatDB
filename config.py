from dotenv import load_dotenv
import os

load_dotenv()

DBMS_OPTIONS = ["mysql", "mongodb"]

MYSQL_CONFIG = {
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'host': os.getenv('MYSQL_HOST'),
    'database': os.getenv('MYSQL_DATABASE')
}

MONGODB_URI = os.getenv('MONGODB_URI')

NUMERIC_UNIQUE =  0.1
OTHERS_UNQIUE = 0.46