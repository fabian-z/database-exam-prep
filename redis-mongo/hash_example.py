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
    "hello": "new world",
    "temperature": 15.0,
}

# Save in Redis
r.hset("myhash", mapping=testdict)

# Load from Redis
myLoadedHash = r.hgetall("myhash")
myDocument = {}

for key in myLoadedHash:
    # Decode Key to String
    keyStr = key.decode("utf-8")
    # Decode Value to String - TODO maybe decode numbers or other datatypes?
    myDocument[keyStr] = myLoadedHash[key].decode("utf-8")

print(myDocument)

mongoCollection.insert_one(myDocument)
