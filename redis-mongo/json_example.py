import json
import redis
import pymongo

# DB Connections, Mongo then Redis

# MONGO
MONGODB_URI = "mongodb://localhost:27017/"

# Connect to your MongoDB
mongoclient = pymongo.MongoClient(MONGODB_URI)

# select/create database
mongodb = mongoclient["testdb"]
# select/create collection in database
mongoCollection = mongodb["testcoll"]

# REDIS
r = redis.Redis(host="localhost", port=6379, db=0)


# TEST
testdict = {
    "hello": "world",
    "temperature": 7.0,
}

# Encode testdict to JSON
encoded = json.dumps(testdict)

# Save in Redis
r.set("myencoded", encoded)

# Load from Redis
mydecoded = json.loads(r.get("myencoded"))

print(mydecoded)

mongoCollection.insert_one(mydecoded)
