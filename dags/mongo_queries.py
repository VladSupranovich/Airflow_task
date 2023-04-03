import pymongo
import pprint

client = pymongo.MongoClient('mongodb://localhost:27017/')
db = client.my_database
collection = db.my_collection

#collection.drop()

result_1 = db.my_collection.aggregate([
    {
        '$sort': {'thumbsUpCount': -1}
    },
    {
        '$limit': 5
    },
    {
        '$project': {
            '_id': 0,
            'reviewId': 1,
            'userName': 1,
            'userImage': 1,
            'content': 1,
            'thumbsUpCount': 1
        }
    }
])

pprint.pprint(list(result_1))


result_2 = db.my_collection.aggregate([
  {
    "$project": {
      "content": 1,
      "length": { "$strLenCP": { "$toString": "$content" } }
    }
  },
  {
    "$match": { "length": { "$lt": 5 } }
  },
  {
    "$sort": { "length": 1 }
  }
])

pprint.pprint(list(result_2))


result_3 = db.my_collection.aggregate([
    {
        '$project': {
            'date': {
                '$toDate': '$at'
            },
            'score': 1
        }
    }, {
        '$group': {
            '_id': {
                '$dateToString': {
                    'format': '%Y-%m-%d',
                    'date': '$date'
                }
            },
            'avg_score': {
                '$avg': '$score'
            }
        }
    }, {
        '$sort': {
            'avg_score': -1
        }
    }
]
)

pprint.pprint(list(result_3))