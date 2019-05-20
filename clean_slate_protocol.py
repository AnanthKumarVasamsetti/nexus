import json
import re
import string
import spacy
from pymongo import MongoClient
from pprint import pprint
import math
from threading import Thread

from tf_idf_calculation import calculate as tf_idf_calculator

conn_url = "mongodb://localhost"
database = 'twitter'
collection = 'apple_support'
resultant_collections = ['tokenized_conversations_1', 'tokenized_conversations_2', 'tokenized_conversations_3', 'tokenized_conversations_4']
__nlp__ = spacy.load('en_core_web_sm')

def remove_special_chars(tokens):

    url_regex = r'http[:, s:]?' # To preserve URLs
    special_chars_regex = r'[^A-Z,a-z,0-9_,\']*'
    mentioners_regex = r'@[A-Z, a-z, 0-9]*' # Mentioners are like @AppleSupport @AmazonHelp @User_id @Twitter_id
    amp_gt = ['amp', 'gt'] # Some special characters like &amp; , &gt;
    cleaned_tokens = []

    for tkn in tokens:
        isUrl = re.match(url_regex, tkn)
        if(isUrl):
            continue
            cleaned_tokens.append(tkn)
        elif(tkn in amp_gt):
            continue
        else:
            tkn = re.sub(mentioners_regex, '', tkn)
            tkn = re.sub(special_chars_regex, '', tkn)
            if(len(tkn) > 0):   # Sometimes after replacing the regex the token could be left with empty string
                cleaned_tokens.append(tkn.lower())

    return cleaned_tokens

def fliter_by_pos(dialouge):
    tokens = []
    pos = ['NOUN', 'PROPN', 'VERB','ADJ']
    doc = __nlp__(str(dialouge))

    for tkn in doc:
        if(not tkn.is_stop and tkn.pos_ in pos):
            tokens.append(tkn.lemma_)
    
    return tokens

def create_bi_grams(tokens):
    bi_grams = [b for b in zip(tokens[:-1], tokens[1:])]
    return bi_grams

def clean(conversation):
    try:
    
        tokenized_conversations = []
        for conv_obj in conversation:
            dialouge = conv_obj[1]
            #dialouge = "Hey @AppleSupport and anyone else who upgraded to ios11.1, are y\u2019all having issues with capital \u201cI\ufe0f\u201d in the Mail app? As it puts in \u201cA\u201d?"
            # Tokenizing words for a given dialouge in a conversation
            tokenized_dialouge = fliter_by_pos(dialouge)
            # Removing special characters and punctuations from tokens (urls are preserved here)
            tokenized_dialouge = remove_special_chars(tokenized_dialouge)
            # Creating bi-grams from the tokenized data
            bi_grams = create_bi_grams(tokenized_dialouge)
            # Attaching bi-grams to the tokenized dialouge
            tokenized_conversations = tokenized_conversations + tokenized_dialouge + bi_grams
        
        return tokenized_conversations
    
    except IndexError as err:
        print("----------- Error encounterd -----------")
        print(err)

def chunks(l, n):
    for i in range(0, len(l), n):
        yield (l[i:i + n][0], l[i:i + n][-1]+1)

def divide_tokens(number_of_tweets, number_of_partitions):
    
    blocks = list(chunks(range(0,number_of_tweets), math.floor(number_of_tweets/number_of_partitions)))
    
    return blocks

def start_cleaning(db, tweets, divider):
    tokenized_conversations = {}
    
    for t_id in tweets:
        tokenized_conversations[t_id] = clean(tweets[t_id])
    
    index = int(t_id.split('_')[1])
    index = int(index % divider)

    #Assigning cleaned tokens into resultant collections
    db[resultant_collections[index]].insert(tokenized_conversations)

def launch():
    #Connection establishment
    print("Intialized connection....")
    thread_pool = []
    conn = MongoClient(conn_url)
    db = conn[database] #Select the database
    col = db[collection] #Select the collection

    tweets = col.find() #Get all records
    tweets = tweets[0]

    del tweets['_id'] #Deleting unwanted data
    
    # Cleaning the collections before insertion
    print("Cleaning collections....")
    for collection_name in resultant_collections:
        db[collection_name].delete_many({})

    print("Starting to excute clean slate protocol.")
    tokenized_conversations = {}
    number_of_partitions = 4    # Number of partitions for a huge data to divide into
    number_of_tweets = len(tweets.keys())
    
    token_partitions = divide_tokens(number_of_tweets, number_of_partitions)    # Tuple with ranges for each partition
    
    tweets_partitions = {}

    for (start_index, end_index) in token_partitions:
        partition = {}
        
        for index in range(start_index, end_index):
            t_id = 'convo_'+str(index)
            partition[t_id] = tweets[t_id]

        tweets_partitions[start_index] = partition
    
    # Spawing threads for each partition and running the independently
    for partition_id in tweets_partitions:
        thread = Thread(target=start_cleaning, args=(db, tweets_partitions[partition_id], number_of_partitions))
        thread_pool.append(thread)
    
    for thread in thread_pool:
        thread.start()
    
    for thread in thread_pool:
        thread.join()
    
    print("Clean slate protocol executed successfully.")

launch()