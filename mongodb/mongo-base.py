import pymongo
import time
import pprint
from bson import ObjectId

# BASE

MONGODB_URI = "mongodb://localhost:27017/"

# Connect to your MongoDB
client = pymongo.MongoClient(MONGODB_URI)

# select/create database
db = client["testdb"]

# select/create collection in database
coll = db["testcoll"]

# set value
coll.insert_one({"key1": "value1", "key2": "value2"})
#coll.insert_many for array of dicts

# get value with condition key1 = value1
coll.find_one({"key1": "value1"})

# get values with condition key1 = value1 (returns an iterator)
for doc in coll.find({"key1": "value1"}):
    print(doc)

# get distinct keys
coll.distinct("key1")

coll.count_documents({"key1": "value1"})

# make update
coll.update_one(
    { "key1": "value1" },
    { "$set": { "key2": "value2" }}
)

# index information
coll.index_information()

# explain query performance
coll.find({"key1": "value1"}).explain()
