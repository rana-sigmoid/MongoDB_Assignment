import pymongo


def task_c_i(theaters):
    li = theaters.aggregate([
        {"$group": {"_id": "$location.address.city", "total_theaters": {"$sum": 1}}},
        {"$project": {"location.address.city": 1, "total_theaters": 1}},
        {"$sort": {"total_theaters": -1}},
        {"$limit": 10}
    ])
    res = []
    for i in li:
        res.append(i)
    return res


def task_c_ii(theaters, coord):

    li = theaters.aggregate([
     {"$geoNear": {"near": {"type": "Point", "coordinates": coord },"maxDistance":10*1000000,"distanceField": "dist.calculated",}},
     {"$project": {"city": "$location.address.city", "distance": "$dist.calculated"}},
     {"$sort": {"_id.distance": 1}},
     {"$limit": 10}
    ])
    res = []
    for i in li:
        res.append(i)
    return res

if __name__ == "__main__":
    client = pymongo.MongoClient("mongodb://localhost:27017/")

    db = client["mongodatabase"]
    theaters = db['theaters']

  

    co_ordinates = [-85.76461, 38.327175]
    ansss=task_c_ii(theaters,co_ordinates)
    for i in ansss:
        print(ansss)