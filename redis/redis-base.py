import redis
from datetime import timedelta
import pymongo
import time
import pprint
from bson import ObjectId

# Clients
r = redis.Redis(host="localhost", port=6379, db=0)

MONGODB_URI = "mongodb://localhost:27017/"

# Connect to your MongoDB
client = pymongo.MongoClient(MONGODB_URI)

# select/create database
db = client["testdb"]

# select/create collection in database
coll = db["testcoll"]


# BASIC

# set value
r.set("key", "value")

# mset for python dict
r.mset({"key1": "value1", "key2": "value2"})

# get value
value = r.get("key")

# check if value exists
if r.exists("key"):
	print("key exists")
else:
	print("key does not exist")

# get value and decode as string from UTF-8
value = r.get("key").decode("utf-8")

# expire value
r.expire("key", timedelta(seconds=3))

# HASH

testdict = {
	"hash_key1": "One",
	"hash_key2": "Two",
}
r.hset("key3", mapping=testdict)

# Equivalent
#r.hset("key3", "hash_key1", "One")
#r.hset("key3", "hash_key2", "Two")

print(r.hget("key3", "hash_key1").decode("utf-8"))

# PUB/SUB

sub = r.pubsub()
# get messages for topic
sub.subscribe("topic")

# publish content
r.publish("topic", "message")

# first message dict has 'type': 'subscribe'
print(sub.get_message())

# get content message
print(sub.get_message())
