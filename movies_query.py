import pymongo

# for i in movies.find({},{"imdb.rating.numberDouble":1,"_id":0}):
def task_i_1(n, movies):
    li = movies.aggregate([
        {'$project': {'title': '$title', 'rating': '$imdb.rating'}},
        {'$group': {'_id': {'rating': '$rating', 'title': '$title'}}},
        {'$sort': {'_id.rating': -1}},
        {'$limit': n}
    ])
    res = []
    for i in li:
        res.append(i)
    return res


def task_i_2(n, year, movies):
    li = movies.aggregate([
        {'$match': {'year': str(year)}},
        {'$project': {'_id': 0, 'title': 1, 'imdb.rating': 1}},
        {'$sort': {'imdb.rating': -1}},
        {'$limit': n}
    ])
    res = []
    for i in li:
        res.append(i)
    return res

def task_i_3(n, movies):
    pipeline = [
        {"$match": {"imdb.votes": {"$gt": "1000"}}},
        {"$project": {"_id": 0, "title": 1, "imdb.rating": 1}},
        {"$sort": {"imdb.rating": -1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res
def task_i_4(n, movies, pattern):
    pipeline = [
        {"$match": {"title": {"$regex": pattern}}},
        {"$project": {"_id": 0, "title": 1, "tomatoes.viewer.rating": 1}},
        {"$sort": {"tomatoes.viewer.rating": -1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res

def task_ii_1(n, movies):
    pipeline = [
        {"$unwind": "$directors"},
        {"$group": {"_id": {"director_name": "$directors"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res

def task_ii_2(n, year, movies):
    pipeline = [
        {"$unwind": "$directors"},
        {"$group": {"_id": {"directors": "$directors", "year": "$year"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.year": str(year)}},
        {"$project": {"_id.directors": 1, "no_of_films": 1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_ii_3(n, genres, movies):
    pipeline = [
        {"$unwind": "$directors"},
        {"$unwind": "$genres"},
        {"$group": {"_id": {"directors": "$directors", "genres": "$genres"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.genres": genres}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_iii_1(n, movies):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_iii_2(n, year, movies):
    pipeline = [
        {"$unwind": "$cast"},
        {"$group": {"_id": {"cast": "$cast", "year": "$year"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.year": str(year)}},
        {"$project": {"_id.year": 0}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res

def task_iii_3(n, genres, movies):
    pipeline = [
        {"$unwind": "$cast"},
        {"$unwind": "$genres"},
        {"$group": {"_id": {"cast": "$cast", "genres": "$genres"}, "no_of_films": {"$sum": 1}}},
        {"$sort": {"no_of_films": -1}},
        {"$match": {"_id.genres": genres}},
        {"$project": {"_id.genres": 0}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res


def task_iv(n, movies, genres):
    pipeline = [
        {"$unwind": "$genres"},
        {"$match": {"genres": genres, 'imdb.rating': {'$exists': True, '$ne': ''}}},
        {"$project": {"_id": 0, "title": 1, 'rating': '$imdb.rating'}},
        {'$group': {'_id': {'title': '$title', 'rating': '$rating'}}},
        {"$sort": {"_id.rating": -1}},
        {"$limit": n}
    ]
    li = movies.aggregate(pipeline)
    res = []
    for i in li:
        res.append(i)
    return res

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    db = client["mongodatabase"]
    movies = db['movies']

    inr=task_iv(10,movies,'Action')
    for i in inr:
        print(i)






