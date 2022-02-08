import pymongo

client = pymongo.MongoClient("mongodb://localhost:27017/")


db = client["mongodatabase"]
comments = db['comments']
users=db['users']
movies=db['movies']
theaters=db['theaters']

def insert_comment(value):
    comments.insert_one(value)
def insert_movie(value):
    movies.insert_one(value)
def insert_theater(value):
    theaters.insert_one(value)
def insert_user(value):
    users.insert_one(value)
