import json
from pprint import pprint
import math


def separate_unigrams_bigrams(conversation):
    unigrams = []
    bigrams = []
    res = {}
    for tkn in conversation:
        if(isinstance(tkn, str)):
            unigrams.append(tkn)
        elif(isinstance(tkn, tuple) or isinstance(tkn, list)):
            bigrams.append(tkn)

    res = {
        'unigrams': unigrams,
        'bigrams': bigrams
    }
    return res


def calculate_tf(unigram_tokens):
    tf_set = {
        tkn: unigram_tokens.count(tkn) / len(unigram_tokens) for tkn in unigram_tokens
    }

    return tf_set


def calculate_idf(words, conversations):
    # Make sure that there are no duplicates in the colected words
    words_set = list(set(words))
    idf_set = {}
    w_appeared = 0

    for w in words_set:
        w_appeared = 0
        for convo_id in conversations:
            if w in conversations[convo_id]:
                w_appeared = w_appeared + 1

        idf_set[w] = math.log((len(conversations) / w_appeared))

    return idf_set


def order_word_to_convo(conversations_tf_idf):
    reordered_tf_idf = {}

    for convo_id in conversations_tf_idf:
        for tkn in conversations_tf_idf[convo_id]:
            if tkn not in list(reordered_tf_idf.keys()):
                reordered_tf_idf[tkn] = {}

            reordered_tf_idf[tkn][convo_id] = conversations_tf_idf[convo_id][tkn]

    return reordered_tf_idf


def calculated_tf_idf_for_unigrams(conversation_unigrams):
    tf_idf_set = {}
    idf_set = {}
    word_set = []
    for convo_id in conversation_unigrams:
        # Term frequency for each word in each conversation
        tf_idf_set[convo_id] = calculate_tf(conversation_unigrams[convo_id])
        # Gathering all tokens from each conversation for IDF calculation
        word_set = word_set + conversation_unigrams[convo_id]

    idf_set = calculate_idf(word_set, conversation_unigrams)

    for convo_id in tf_idf_set:
        for word in tf_idf_set[convo_id]:
            tf_idf_set[convo_id][word] = tf_idf_set[convo_id][word] * \
                idf_set[word]

    return tf_idf_set
    # return order_word_to_convo(tf_idf_set)


def calculate_tf_idf_for_bigrams(conversation_bigrams, unigrams_tf_idf_values):
    tf_idf_set = {}

    for convo_id in conversation_bigrams:
        sum_of_avgs = 0
        tf_idf_set[convo_id] = {}
        for bi_gram in conversation_bigrams[convo_id]:
            # As there will be only two words in a bigram
            word_1 = bi_gram[0]
            word_2 = bi_gram[1]
            
            # Averaging out the tf_idf of bigram from their respective unigram's tf_idf value
            sum_of_avgs = unigrams_tf_idf_values[convo_id][word_1] + unigrams_tf_idf_values[convo_id][word_2]
            sum_of_avgs = sum_of_avgs / 2

            tf_idf_set[convo_id][(word_1, word_2)] = sum_of_avgs
    
    return tf_idf_set
        

def calculate(conversations_data):
    conversation_unigrams = {}
    conversation_bigrams = {}
    complete_set = {}

    print("Started calculating tf-idf values.")

    for convo_id in conversations_data:
        unigrams_and_bigrams = separate_unigrams_bigrams(
            conversations_data[convo_id])
        # Seperating unigrams and bigrams for calculating tf-idf values for each conversation
        conversation_unigrams[convo_id] = unigrams_and_bigrams['unigrams']
        conversation_bigrams[convo_id] = unigrams_and_bigrams['bigrams']
    
    # Calculating tf-idf values for unigrams
    print("Calculating tf-idf for unigrams.")
    unigrams_tf_idf_values = calculated_tf_idf_for_unigrams(conversation_unigrams)
    # Calculating tf-idf values for bigrams
    print("Calculating tf-idf for bigrams.")
    bigrams_tf_idf_values = calculate_tf_idf_for_bigrams(conversation_bigrams, unigrams_tf_idf_values)
    
    # Clubbing both the unigrams and bigrams values
    complete_set = unigrams_tf_idf_values
    for convo_id in complete_set:
        complete_set[convo_id].update(bigrams_tf_idf_values[convo_id])

    # Reordering the tf-idf json
    print('Reordering the values for word to conversation.')
    complete_set = order_word_to_convo(complete_set)
    return complete_set


#calculate(data)
