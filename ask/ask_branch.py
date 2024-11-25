from ask.mysql_ask.mysql_main import run_cli as mysql_run_cli
from ask.mongo_ask.mongo_main import run_cli as mongo_run_cli
def branch_ask(db_type, name):
    if db_type == 'mysql':
        mysql_run_cli(name)
    elif db_type == 'mongodb':
        mongo_run_cli(name)
    else:
        return 'Please retry'