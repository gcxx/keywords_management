import pymongo
from bson.son import SON
import math
import numpy
import matplotlib.pyplot as plt
import time

client1 = pymongo.MongoClient('192.168.1.80', 27017)
VionelDB = client1['VionelDB']
allmovies = VionelDB['AllMovies_copy']

client2 = pymongo.MongoClient('192.168.1.87', 27017)
db = client2['Hackathon']
movielens_user_data = db['movielens_validation_iid2_copy']
movielens_ids = db['movielens_imdbid_tmdbid']

if __name__ == '__main__':
    start=time.time()
    tfidfs={}
    count = 0
    allmovie_themes={}
    count=0

    for line in movielens_user_data.find(no_cursor_timeout=True):
        # print line 
        count+=1
        print count
        keywords_for_user={}
        num=0
        uid = line['userId']
        print uid
        # print line ['likelist']
        # if len(line['likelist'])<20:
        #     continue
        iids=[ d['imdbId'] for d in line['likelist']]
        # print iids

        ### themes
        # for data in allmovies.find({'imdbId':{'$in':iids}},no_cursor_timeout=True):
        #     try:
        #         for theme in data['allmovieFeature']['themes']:
        #             name = theme['title']
        #             if name in keywords_for_user:
        #                 keywords_for_user[name]+=1
        #             else:
        #                 keywords_for_user[name]=1
        #     except:
        #         continue
        # movielens_user_data.update({'userId': uid},{'$set': {'themes': keywords_for_user}},upsert=False, multi=False)
        ### moods
        for data in allmovies.find({'imdbId':{'$in':iids}},no_cursor_timeout=True):
            try:
                for theme in data['allmovieFeature']['moods']:
                    name = theme['title']
                    if name in keywords_for_user:
                        keywords_for_user[name]+=1
                    else:
                        keywords_for_user[name]=1
            except:
                continue
        movielens_user_data.update({'userId': uid},{'$set': {'moods': keywords_for_user}},upsert=False, multi=False)
        ### keywords
        for data in allmovies.find({'imdbId':{'$in':iids}},no_cursor_timeout=True):
            try:
                for theme in data['allmovieFeature']['keywords']:
                    name = theme['title'].replace('.','')
                    if name in keywords_for_user:
                        keywords_for_user[name]+=1
                    else:
                        keywords_for_user[name]=1
            except:
                continue
        movielens_user_data.update({'userId': uid},{'$set': {'keywords': keywords_for_user}},upsert=False, multi=False)
        ### attributes
        for data in allmovies.find({'imdbId':{'$in':iids}},no_cursor_timeout=True):
            try:
                for theme in data['allmovieFeature']['attributes']:
                    name = theme['title']
                    if name in keywords_for_user:
                        keywords_for_user[name]+=1
                    else:
                        keywords_for_user[name]=1
            except:
                continue
        movielens_user_data.update({'userId': uid},{'$set': {'attributes': keywords_for_user}},upsert=False, multi=False)

        ### subgenres
        for data in allmovies.find({'imdbId':{'$in':iids}},no_cursor_timeout=True):
            try:
                for theme in data['allmovieFeature']['subgenres']:
                    name = theme['title']
                    if name in keywords_for_user:
                        keywords_for_user[name]+=1
                    else:
                        keywords_for_user[name]=1
            except:
                continue
        movielens_user_data.update({'userId': uid},{'$set': {'subgenres': keywords_for_user}},upsert=False, multi=False)







        end = time.time()
        print end - start
        # break