import pymongo
import json
from bson import ObjectId
from pymongo import MongoClient

client = pymongo.MongoClient("mongodb://localhost:27017/")


db = client["mongodatabase"]
theaters = db['theaters']
data = []

for line in open("/Users/ranadilendrasingh/Downloads/sample_mflix/theaters.json"):
    if line:
        new_data = json.loads(line)
        new_data['_id'] = ObjectId(new_data['_id']['$oid'])
        new_data['theaterId'] = new_data['theaterId']['$numberInt']
        new_data['location']['geo']['coordinates'] = [
            (new_data['location']['geo']['coordinates'][0]['$numberDouble']),
            (new_data['location']['geo']['coordinates'][1]['$numberDouble'])]
        data.append(new_data)

theaters.insert_many(data)
