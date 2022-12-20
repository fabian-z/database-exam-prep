import pymongo
import time
import pprint

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

pipeline = [
   stage_idyear
]

results = movie_collection.aggregate(pipeline)

#sorted_x = sorted(results, key=lambda kv: kv["count"], reverse = True)
#pprint.pprint(sorted_x)

for movie in results:
   print(movie)

   if len(str(movie["year"])) > 4:
      print(movie)
      movie_collection.update({ "_id": movie["_id"] }, {"year": str(movie["year"])[:4]})
   #print(" * {title}, {first_castmember}, {year}".format(
   #      title=movie["title"],
   #      first_castmember=movie["cast"][0],
   #      year=movie["year"],
   #))
