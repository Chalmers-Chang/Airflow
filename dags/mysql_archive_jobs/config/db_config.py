import os

class db_config:
   
    DATABASES = {
        "POC_MySQL_account": {
            "env_username": "POC",
            "host": "host_name",
            "db": "target_db",
            "username": "user_name",
            "password": "gAAAAABnoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo==", #encrypted by using fernet
            "proxy": None,
        },
        "POC_MSSQL_account": {
            "env_username": "POC",
            "host": "host_name",
            "db": "bodb02",
            "username": "user_name",
            "password": "gAAAAABnoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo==", #encrypted by using fernet
            "proxy": None,
        },
        "PROD_MySQL_account": {
            "env_username": "PROD",
            "host": "host_name",
            "db": "db_name",
            "username": "user_name",
            "password": "gAAAAABnoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo==", #encrypted by using fernet
            "proxy": "your_proxy",
        },
        "PROD_MSSQL_account": {
            "env_username": "PROD",
            "host": "host_name",
            "db": "bodb02",
            "username": "user_name",
            "password": "gAAAAABnoooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo==", #encrypted by using fernet
            "proxy": "your_proxy",
        }
    }

    def __init__(self, db_name):
        
        if db_name not in self.DATABASES:
            raise ValueError(f"Database configuration for '{db_name}' not found.")
        
        self.config = self.DATABASES[db_name]
        self.host = self.config["host"]
        self.db = self.config["db"]
        self.username = self.config["username"]
        self.password = self.config["password"]
        self.proxy = self.config["proxy"]

    def get_connection_info(self):

        return self.config
	
