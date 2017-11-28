import json
import pprint
from bson.son import SON
from pymongo import MongoClient

settings = json.load(open("settings.json"))
connectionString = settings["connectionString"]

client = MongoClient(connectionString)
db = client.get_default_database()
posts = db["posts"]

pipelinePopularLangTech = [
    {"$unwind": "$tags"},
    {"$group": {"_id": "$tags", "count": {"$sum": 1}}},
    {"$sort": SON([("count", -1), ("_id", -1)])},
    {"$limit": 10}
    ]
print ("Top popular languages and technologies")    
popularLangTech = list(posts.aggregate(pipelinePopularLangTech))

pprint.pprint(popularLangTech)
print ("------------------------------------------") 

pipelineQuestionLatency = [
    {"$match": { "type": 2}},
    {"$sort": SON([("date", 1)])},
    {"$group": {"_id": "$par", "firstAnswer": {"$first": "$date"}}},
    {"$lookup": {"from": "posts", "localField": "_id", "foreignField": "_id", "as": "questions"}},
    {"$project": {"firstAnswer": 1, "_id": 0, "question": {"$arrayElemAt": ["$questions", 0]}}},
    {"$project": {"aDate": "$firstAnswer", "tags": "$question.tags", "qDate": "$question.date", "diff": {"$divide": [{"$subtract":["$firstAnswer", "$question.date"]}, 60000]}}},
    {"$unwind": "$tags"},
    {"$group": {"_id": "$tags", "avg": {"$avg": "$diff"}, "count": {"$sum": 1}}},
    {"$match": {"count": {"$gt": 50}}},
    {"$sort": {"avg": -1}},
    {"$limit": 10}
    ]   
print ("Top lowest average latency tags answer")    
topLowestTechLatency = list(posts.aggregate(pipelineQuestionLatency))

pprint.pprint(topLowestTechLatency)
print ("------------------------------------------") 
