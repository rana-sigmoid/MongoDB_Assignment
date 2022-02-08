import pymongo
import json
from bson import ObjectId
from pymongo import MongoClient

client = pymongo.MongoClient("mongodb://localhost:27017/")


db = client["mongodatabase"]
movies = db.get_collection('movies')
data = []

for line in open("/Users/ranadilendrasingh/Downloads/sample_mflix/movies.json"):
    if line:
        new_data = json.loads(line)
        if (new_data.get('_id')):
            new_data['_id'] = ObjectId(new_data['_id']['$oid'])
            if (new_data.get('year')):
                x = new_data['year']
                if type(x) != str:
                    new_data['year'] = new_data['year']['$numberInt']

            if (new_data.get('runtime')):
                new_data['runtime'] = new_data['runtime']['$numberInt']

            if (new_data.get('released')):
                new_data['released'] = new_data['released']['$date']['$numberLong']

            x = new_data['imdb']['rating']
            if type(x) != str and new_data['imdb']['rating'].get('$numberDouble'):
                new_data['imdb']['rating'] = new_data['imdb']['rating']['$numberDouble']
            elif type(x) != str:
                new_data['imdb']['rating'] = new_data['imdb']['rating']['$numberInt']

            if (new_data['imdb'].get('votes')):
                new_data['imdb']['votes'] = new_data['imdb']['votes']['$numberInt']
            new_data['imdb']['id'] = new_data['imdb']['id']['$numberInt']


            if new_data.get('tomatoes'):
                if (new_data['tomatoes'].get('viewer')):
                    if (new_data['tomatoes']['viewer'].get('rating')):
                        if new_data['tomatoes']['viewer']['rating'].get('$numberInt'):
                            new_data['tomatoes']['viewer']['rating'] = \
                            new_data['tomatoes']['viewer']['rating']['$numberInt']
                        else:
                            new_data['tomatoes']['viewer']['rating'] = \
                            new_data['tomatoes']['viewer']['rating']['$numberDouble']

                    new_data['tomatoes']['viewer']['numReviews'] = \
                    new_data['tomatoes']['viewer']['numReviews']['$numberInt']
                new_data['tomatoes']['lastUpdated'] = new_data['tomatoes']['lastUpdated']['$date'][
                    '$numberLong']

            if new_data.get('num_mflix_comments'):
                new_data['num_mflix_comments'] = new_data['num_mflix_comments']['$numberInt']

        data.append(new_data)

movies.insert_many(data)
