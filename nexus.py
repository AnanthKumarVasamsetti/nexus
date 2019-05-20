from pymongo import MongoClient
from threading import Thread
from tf_idf_calculation import calculate
from pprint import pprint


def print_json(tf_idf_values, limit):
    counter = 0
    for key in tf_idf_values:
        if counter == limit:
            break

        print(key)
        print(tf_idf_values[key])
        counter = counter + 1
    return


class Nexus:
    def __init__(self, conn_url, database):
        print("Initialized nexus.")
        self.conn = MongoClient(conn_url)
        self.db = self.conn[database]

        self.local_database = {
            'local_tf_1': {},
            'local_tf_2': {},
            'local_tf_3': {},
            'local_tf_4': {}
        }
    
    def run_extraction(self, tf_idf_collections):
        collection_data = tf_idf_collections

        data = {
            "convo_12745": [
                "play",
                "game",
                "purchase",
                "apple",
                "store",
                "reverse",
                "naira",
                "dollar",
                "nigerian",
                "store",
                "option",
                "choose",
                "nada",
                "cool",
                [
                    "play",
                    "game"
                ],
                [
                    "game",
                    "purchase"
                ],
                [
                    "purchase",
                    "apple"
                ],
                [
                    "apple",
                    "store"
                ],
                [
                    "store",
                    "reverse"
                ],
                [
                    "reverse",
                    "naira"
                ],
                [
                    "naira",
                    "dollar"
                ],
                [
                    "dollar",
                    "nigerian"
                ],
                [
                    "nigerian",
                    "store"
                ],
                [
                    "store",
                    "option"
                ],
                [
                    "option",
                    "choose"
                ],
                [
                    "choose",
                    "nada"
                ],
                [
                    "nada",
                    "cool"
                ],
                "guy",
                "help",
                "matter",
                "naira",
                "dollar",
                "card",
                "apple",
                "app",
                "store",
                "hahba",
                [
                    "guy",
                    "help"
                ],
                [
                    "help",
                    "matter"
                ],
                [
                    "matter",
                    "naira"
                ],
                [
                    "naira",
                    "dollar"
                ],
                [
                    "dollar",
                    "card"
                ],
                [
                    "card",
                    "apple"
                ],
                [
                    "apple",
                    "app"
                ],
                [
                    "app",
                    "store"
                ],
                [
                    "store",
                    "hahba"
                ],
                "love",
                "help",
                "good",
                "bet",
                "reach",
                "itunes",
                "store",
                "team",
                [
                    "love",
                    "help"
                ],
                [
                    "help",
                    "good"
                ],
                [
                    "good",
                    "bet"
                ],
                [
                    "bet",
                    "reach"
                ],
                [
                    "reach",
                    "itunes"
                ],
                [
                    "itunes",
                    "store"
                ],
                [
                    "store",
                    "team"
                ],
                "click",
                "link",
                "send",
                [
                    "click",
                    "link"
                ],
                [
                    "link",
                    "send"
                ],
                "like",
                "look",
                "option",
                "country",
                "reply",
                "dm",
                [
                    "like",
                    "look"
                ],
                [
                    "look",
                    "option"
                ],
                [
                    "option",
                    "country"
                ],
                [
                    "country",
                    "reply"
                ],
                [
                    "reply",
                    "dm"
                ],
                "thank",
                "have",
                "send",
                "private",
                "message",
                [
                    "thank",
                    "have"
                ],
                [
                    "have",
                    "send"
                ],
                [
                    "send",
                    "private"
                ],
                [
                    "private",
                    "message"
                ]
            ],
            "convo_12746": [
                "wtf",
                "fuck",
                [
                    "wtf",
                    "fuck"
                ]
            ],
            "convo_12747": [
                "update",
                "phone",
                "hot",
                "ass",
                "mess",
                [
                    "update",
                    "phone"
                ],
                [
                    "phone",
                    "hot"
                ],
                [
                    "hot",
                    "ass"
                ],
                [
                    "ass",
                    "mess"
                ],
                "happy",
                "help",
                "dm",
                "detail",
                "experience",
                [
                    "happy",
                    "help"
                ],
                [
                    "help",
                    "dm"
                ],
                [
                    "dm",
                    "detail"
                ],
                [
                    "detail",
                    "experience"
                ]
            ],
            "convo_12748": [
                "iphone",
                "restart",
                "sound",
                "haptic",
                "turn",
                "help",
                [
                    "iphone",
                    "restart"
                ],
                [
                    "restart",
                    "sound"
                ],
                [
                    "sound",
                    "haptic"
                ],
                [
                    "haptic",
                    "turn"
                ],
                [
                    "turn",
                    "help"
                ],
                "let",
                "help",
                "happen",
                "specific",
                "app",
                "feature",
                "iphone",
                "version",
                "instal",
                [
                    "let",
                    "help"
                ],
                [
                    "help",
                    "happen"
                ],
                [
                    "happen",
                    "specific"
                ],
                [
                    "specific",
                    "app"
                ],
                [
                    "app",
                    "feature"
                ],
                [
                    "feature",
                    "iphone"
                ],
                [
                    "iphone",
                    "version"
                ],
                [
                    "version",
                    "instal"
                ],
                "happen",
                "app",
                "run",
                "1103",
                [
                    "happen",
                    "app"
                ],
                [
                    "app",
                    "run"
                ],
                [
                    "run",
                    "1103"
                ],
                "like",
                "look",
                "meet",
                "dm",
                "continue",
                [
                    "like",
                    "look"
                ],
                [
                    "look",
                    "meet"
                ],
                [
                    "meet",
                    "dm"
                ],
                [
                    "dm",
                    "continue"
                ]
            ],
            "convo_12749": [
                "condition",
                "current",
                "iphone",
                "trade",
                "new",
                "model",
                "upgrade",
                "program",
                [
                    "condition",
                    "current"
                ],
                [
                    "current",
                    "iphone"
                ],
                [
                    "iphone",
                    "trade"
                ],
                [
                    "trade",
                    "new"
                ],
                [
                    "new",
                    "model"
                ],
                [
                    "model",
                    "upgrade"
                ],
                [
                    "upgrade",
                    "program"
                ],
                "point",
                "right",
                "direction",
                "send",
                "dm",
                "country",
                "start",
                [
                    "point",
                    "right"
                ],
                [
                    "right",
                    "direction"
                ],
                [
                    "direction",
                    "send"
                ],
                [
                    "send",
                    "dm"
                ],
                [
                    "dm",
                    "country"
                ],
                [
                    "country",
                    "start"
                ]
            ],
            "convo_12750": [
                "vamo",
                "melhorar",
                "essa",
                "cagada",
                "vc",
                "to",
                "fazendo",
                "celular",
                "para",
                "travar",
                "nuncaaaaa",
                [
                    "vamo",
                    "melhorar"
                ],
                [
                    "melhorar",
                    "essa"
                ],
                [
                    "essa",
                    "cagada"
                ],
                [
                    "cagada",
                    "vc"
                ],
                [
                    "vc",
                    "to"
                ],
                [
                    "to",
                    "fazendo"
                ],
                [
                    "fazendo",
                    "celular"
                ],
                [
                    "celular",
                    "para"
                ],
                [
                    "para",
                    "travar"
                ],
                [
                    "travar",
                    "nuncaaaaa"
                ],
                "twitter",
                "support",
                "available",
                "english",
                "help",
                "join",
                [
                    "twitter",
                    "support"
                ],
                [
                    "support",
                    "available"
                ],
                [
                    "available",
                    "english"
                ],
                [
                    "english",
                    "help"
                ],
                [
                    "help",
                    "join"
                ]
            ],
            "convo_12751": [
                "enjoy",
                "destroy",
                "usability",
                "iphone",
                "app",
                "crash",
                "take",
                "time",
                "open",
                "battery",
                "die",
                [
                    "enjoy",
                    "destroy"
                ],
                [
                    "destroy",
                    "usability"
                ],
                [
                    "usability",
                    "iphone"
                ],
                [
                    "iphone",
                    "app"
                ],
                [
                    "app",
                    "crash"
                ],
                [
                    "crash",
                    "take"
                ],
                [
                    "take",
                    "time"
                ],
                [
                    "time",
                    "open"
                ],
                [
                    "open",
                    "battery"
                ],
                [
                    "battery",
                    "die"
                ],
                "help",
                "dm",
                "country",
                "region",
                "locate",
                [
                    "help",
                    "dm"
                ],
                [
                    "dm",
                    "country"
                ],
                [
                    "country",
                    "region"
                ],
                [
                    "region",
                    "locate"
                ]
            ],
            "convo_12752": [
                "new",
                "mac",
                "air",
                "make",
                "fan",
                "noise",
                "minimal",
                "work",
                "week",
                "old",
                "app",
                "run",
                "heat",
                "help",
                "bangalore",
                "india",
                [
                    "new",
                    "mac"
                ],
                [
                    "mac",
                    "air"
                ],
                [
                    "air",
                    "make"
                ],
                [
                    "make",
                    "fan"
                ],
                [
                    "fan",
                    "noise"
                ],
                [
                    "noise",
                    "minimal"
                ],
                [
                    "minimal",
                    "work"
                ],
                [
                    "work",
                    "week"
                ],
                [
                    "week",
                    "old"
                ],
                [
                    "old",
                    "app"
                ],
                [
                    "app",
                    "run"
                ],
                [
                    "run",
                    "heat"
                ],
                [
                    "heat",
                    "help"
                ],
                [
                    "help",
                    "bangalore"
                ],
                [
                    "bangalore",
                    "india"
                ],
                "smc",
                "reset",
                "help",
                "try",
                "step",
                "article",
                [
                    "smc",
                    "reset"
                ],
                [
                    "reset",
                    "help"
                ],
                [
                    "help",
                    "try"
                ],
                [
                    "try",
                    "step"
                ],
                [
                    "step",
                    "article"
                ]
            ],
            "convo_12753": [
                "software",
                "upgrade",
                "take",
                "dial",
                "age",
                [
                    "software",
                    "upgrade"
                ],
                [
                    "upgrade",
                    "take"
                ],
                [
                    "take",
                    "dial"
                ],
                [
                    "dial",
                    "age"
                ],
                "get",
                "dm",
                "connect",
                [
                    "get",
                    "dm"
                ],
                [
                    "dm",
                    "connect"
                ]
            ],
            "convo_12754": [
                "mistake",
                "choose",
                "store",
                "new",
                "iphone",
                "help",
                [
                    "mistake",
                    "choose"
                ],
                [
                    "choose",
                    "store"
                ],
                [
                    "store",
                    "new"
                ],
                [
                    "new",
                    "iphone"
                ],
                [
                    "iphone",
                    "help"
                ],
                "restore",
                "backup",
                [
                    "restore",
                    "backup"
                ]
            ],
            "convo_12755": [
                "photo",
                "app",
                "drain",
                "battery",
                "min",
                "wtf",
                "iphone7plus",
                [
                    "photo",
                    "app"
                ],
                [
                    "app",
                    "drain"
                ],
                [
                    "drain",
                    "battery"
                ],
                [
                    "battery",
                    "min"
                ],
                [
                    "min",
                    "wtf"
                ],
                [
                    "wtf",
                    "iphone7plus"
                ]
            ]
        }
        tf_idf_values = calculate(collection_data)

        self.shard_json(tf_idf_values)

    def shard_json(self, tf_idf_values):
        for key in tf_idf_values:
            # Selecting the collection number to store
            collection_number = (ord(key[0]) % 4) + 1
            self.local_database['local_tf_'+str(collection_number)].update({
                key: tf_idf_values[key]
            })


def launch_nexus():
    try:
        conn_url = "mongodb://localhost"
        database = 'twitter'
        resultant_collections = ['tokenized_conversations_1', 'tokenized_conversations_2',
                                'tokenized_conversations_3', 'tokenized_conversations_4']
        tf_idf_collections = ['tf_idf_1', 'tf_idf_2', 'tf_idf_3', 'tf_idf_4']

        print("Initializing nexus")
        nexus = Nexus(conn_url, database)

        thread_pool = []

        # Cleaning tf_idf values collections
        for col in tf_idf_collections:
            nexus.db[col].delete_many({})

        # Pooling threads for each collection of tf_idf values
        for i in range(len(tf_idf_collections)):
            collection = nexus.db[resultant_collections[i]].find()
            collection = collection[0]
            del collection['_id']
            collection = [collection]
            thread_pool.append(
                Thread(target=nexus.run_extraction, args=collection))

        print("Extracting and shradding the data for tf_idf values.")
        for thread in thread_pool:
            thread.start()

        for index in range(len(thread_pool)):
            thread_pool[index].join()

            remote_collection = tf_idf_collections[index]
            local_collection = nexus.local_database['local_tf_'+str(index+1)]

            print("Inserting into: "+remote_collection+".")
            nexus.db[remote_collection].insert(local_collection)

        print("Nexus executed successfully")
    except KeyError as err:
        print("Error occured in nexus: "+str(err))

launch_nexus()
