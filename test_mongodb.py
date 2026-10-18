
import os
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

uri = os.getenv("MONGODB_URI")
database_name = os.getenv("MONGODB_DATABASE", "fleetpulse_dev")

if not uri:
    raise ValueError("Chua cau hinh MONGODB_URI trong file .env")

client = MongoClient(uri, serverSelectionTimeoutMS=10000)

try:
    client.admin.command("ping")
    print("Ket noi MongoDB Atlas thanh cong!")
    print(f"Database: {database_name}")
finally:
    client.close()
