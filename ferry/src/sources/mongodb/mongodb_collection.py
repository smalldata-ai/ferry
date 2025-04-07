# ferry/src/sources/mongodb/mongodb_collection.py

import pymongo

def mongodb_collection(connection_url: str, database: str, collection: str):
    """Fetch data from a MongoDB collection."""
    client = pymongo.MongoClient(connection_url)
    db = client[database]
    return db[collection].find()
