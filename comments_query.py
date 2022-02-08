import pymongo

def task_a_i(comments):

    ans = comments.aggregate([
        {'$group': {'_id': '$name','total': {'$sum': 1}}},
        {'$sort': {'total': -1}},
        {'$limit': 10}
    ])
    usernames = []
    for i in ans:
        usernames.append(i['_id'])
    return usernames


def task_a_2(comments):
    new_data = comments.aggregate([
        {'$group': {'_id': '$movie_id','text': {'$sum': 1}}},
        {'$sort': {'text': -1}},
        {'$limit': 10},
    ])
    movies_name = []
    for i in new_data:
        movies_name.append(i['data']['title'])
    return movies_name

def task_a_3(comments,year):
    result = comments.aggregate([
        {"$project": {"_id": 0, "date": {"$toDate": {"$convert": {"input": "$date", "to": "long"}}}}},
        {"$group": {"_id": {"year": {"$year": "$date"}, "month": {"$month": "$date"}},"text": {"$sum": 1}}},
        {"$match": {"_id.year": {"$eq": year}}},
        {"$sort": {"_id.month": 1}}
    ])
    li = []
    for i in result:
        li.append(i)
    return li

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    db = client["mongodatabase"]
    comments = db['comments']


