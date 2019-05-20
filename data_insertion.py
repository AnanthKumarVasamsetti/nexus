import json
import pymongo

connection = pymongo.MongoClient('mongodb://localhost')
db = connection.twitter #Selecting database
collection = db.apple_support
items = {
    'one':{
        'two':2
    },
    'two': {
        'two':2
    }
}

collection.insert(items)