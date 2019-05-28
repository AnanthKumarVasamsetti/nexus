from pymongo import MongoClient

def rank(results):
    ranked_results = {}
    result_array = []

    for res_obj in results:
        for key in res_obj:
            if key not in ranked_results:
                ranked_results[key] = 0
            ranked_results[key] = ranked_results[key] + res_obj[key]
    for key in ranked_results:
        result_array.append({'id': key, 'val': ranked_results[key]})
    
    result_array = sorted(result_array, key = lambda x : x['val'])
    return result_array[::-1][:10]

def retrive(tokens):
    conn_url = "mongodb://localhost"
    database = 'twitter'

    conn = MongoClient(conn_url)
    db = conn[database]
    collection_suffix = 'tf_idf_'
    results = []
    for key in tokens:
        # Selecting the collection number to retrive
        if(isinstance(key, str)):
            collection_number = (ord(key[0]) % 20) + 1
        elif(isinstance(key, tuple) or isinstance(key, list)):
            collection_number = (ord(key[0][0]) % 20) + 1
        
        # Once we get the collection number we retrive the token results from that resprctive collection
        collection_id = collection_suffix + str(collection_number)
        print(collection_id)
        collection = db[collection_id].find({})
        res = collection[0][str(key)]
        results.append(res)
    
    ranked_results = rank(results)
    print(ranked_results)



tokens = ['music', 'control', 'center', ('music', 'control'), ('control', 'center')]
retrive(tokens)