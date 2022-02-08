import pymongo
import json
from bson import ObjectId

client = pymongo.MongoClient("mongodb://localhost:27017/")

db = client["mongodatabase"]

comments = db['comments']
data = []

for line in open("/Users/ranadilendrasingh/Downloads/sample_mflix/comments.json"):
    new_data = json.loads(line)
    new_data['_id'] = ObjectId(new_data['_id']['$oid'])
    new_data['movie_id'] = ObjectId(new_data['movie_id']['$oid'])
    new_data['date'] = new_data['date']['$date']['$numberLong']
    data.append(new_data)

comments.insert_many(data)
