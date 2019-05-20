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
