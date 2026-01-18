# functions/db.py

import mysql.connector

from functions.utils import load_env
from functions.tenant import get_env

load_env()

def get_connection():
    return mysql.connector.connect(
        host=get_env("DB_HOST"),
        port=int(get_env("DB_PORT", "3306")),
        user=get_env("DB_USER"),
        password=get_env("DB_PASSWORD"),
        database=get_env("DB_NAME"),
        ssl_disabled=False
    )
