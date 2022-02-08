import pymongo
import json
from bson import ObjectId
from pymongo import MongoClient

client = pymongo.MongoClient("mongodb://localhost:27017/")


db = client["mongodatabase"]
users = db['users']
data = []

for line in open("/Users/ranadilendrasingh/Downloads/sample_mflix/theaters.json"):
    if line:
        new_data = json.loads(line)
        new_data['_id'] = ObjectId(new_data['_id']['$oid'])
        data.append(new_data)

users.insert_many(data)
