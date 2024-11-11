from sqlalchemy import create_engine
import pymongo
import config

class DatabaseConnector:
    def __init__(self):
        self.connections = [None, None]

    def connect_mysql(self):
        if self.connections[0] is None:
            connection_string = f"mysql+pymysql://{config.MYSQL_CONFIG['user']}:{config.MYSQL_CONFIG['password']}@{config.MYSQL_CONFIG['host']}/{config.MYSQL_CONFIG['database']}"
            self.connections[0] = create_engine(connection_string)
        return

    def connect_mongodb(self):
        if self.connections[1] is None:
            self.connections[1] = pymongo.MongoClient(config.MONGODB_URI)
        return
    
    def connect_all(self):
        self.connect_mysql()
        self.connect_mongodb()
        return