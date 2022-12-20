import pymongo
import time
import pprint
from bson import ObjectId
import datetime

MONGODB_URI = "mongodb://localhost:27017/"

# Connect to your MongoDB cluster:
client = pymongo.mongo_client.MongoClient(MONGODB_URI)

# Get a reference to the "sample_mflix" database:
db = client["mflix"]

# Get a reference to the "movies" collection:
movie_collection = db["movies_initial"]

stage_group_type = {
   "$group": {
         "_id": "$language",
         "count": { "$sum" : 1 }
   }
}

stage_group_year = {
   "$group": {
         "_id": "$year",
         "count": { "$sum" : 1 }
   }
}

# Match title = "A Star Is Born":
stage_match_language = {
   "$match": {
         "language": {"$regex": "^English"}
   }
}

stage_sort_count = {
   "$sortByCount": "$language"
}

# Sort by year, ascending:
stage_sort_year_ascending = {
   "$sort": { "year": pymongo.ASCENDING }
}

# Sort by year, descending:
stage_sort_year_descending = {
   "$sort": { "year": pymongo.DESCENDING }
}

# Limit to 1 document:
stage_idyear = { 
 "$project": {
         "_id": "$_id",
         "year": "$year"
   }
}

stage_match_year = {
   "$match": {
         "year": 2016
   }
}

stage_group_rating = {
   "$group": {
         "_id": "$rating",
         "count": { "$sum" : 1 }
   }
}

stage_count_sort = {
   "$sort": {"count": +1}
}

stage_match_shorts = {
   "$match": {
      "genre": {"$regex": "short", '$options' : 'i'},
      "year": {"$gte": 2000},
   }
}

stage_project_short = {
   "$project": {
       "title": "$title",
       "year": "$year"
   }
}

stage_short_sort = {
   "$sort": {"title": +1}
}

stage_short_out = {
   "$out": { "db": "ShortMovies", "coll": "2000er" }
}

pipeline = [
   stage_match_shorts,
   stage_project_short,
   stage_short_out
]

results = list(movie_collection.aggregate(pipeline))
print(len(results))

#for movie in results:
#   print(movie)

""" 
totalTimePipeline =datetime.timedelta()
totalTimeFind =datetime.timedelta()

for year in years:
   a = datetime.datetime.now()
   pipeline = [ { "$match": { "year": year } } ]
   yearResults = movie_collection.aggregate(pipeline)
   count = 0
   for x in yearResults:
      count = count + 1
   b = datetime.datetime.now()
   totalTimePipeline=totalTimePipeline+(b-a)

   a = datetime.datetime.now()
   yearResults = movie_collection.count_documents({"year": year})
   b = datetime.datetime.now()
   totalTimeFind=totalTimeFind+(b-a)

print(totalTimePipeline)
print(totalTimeFind) """

#sorted_x = sorted(results, key=lambda kv: kv["count"], reverse = True)
#pprint.pprint(sorted_x)

#for movie in results:

#   print(movie)
   #if len(str(movie["year"])) > 4:
   #   new = int(str(movie["year"])[0:4])
   #   movie_collection.update_one({ "_id": movie["_id"] }, { "$set": {"year": new}})
   
   #print(" * {title}, {first_castmember}, {year}".format(
   #      title=movie["title"],
   #      first_castmember=movie["cast"][0],
   #      year=movie["year"],
   #))
