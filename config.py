import os
from dotenv import load_dotenv

load_dotenv(".env")

db_name:str=os.getenv("db_name")