import pymongo
from bson.son import SON
import math
import numpy
import matplotlib.pyplot as plt
import time
import json

client1 = pymongo.MongoClient('192.168.1.80', 27017)
VionelDB = client1['VionelDB']
allmovies = VionelDB['AllMovies_copy']

client2 = pymongo.MongoClient('192.168.1.87', 27017)
db = client2['Hackathon']
movielens_user_data = db['movielens_validation_iid2_copy']
movielens_ids = db['movielens_imdbid_tmdbid']

def create_idf():
    pipeline = [
    {'$project' : {'_id':0,'themes':'$allmovieFeature.themes.title'}},
    {'$limit' : 500000},
    {'$unwind':"$themes"},
    {'$group': { '_id' : "$themes", 'count': { '$sum': 1 }}}]
    
    keywords_count=list(allmovies.aggregate(pipeline))

    idf= lambda count,user_id:[user_id,{'idf':math.log10(30106.0/(count+1))}]
    IDFs = dict(map(idf,[line['count']for line in keywords_count],[line['_id']for line in keywords_count]))
    print IDFs
    for line in keywords_count: 
        tfidf_mean=math.log10(30106.0/(line['count']+1))*line['count']/30106
        IDFs[line['_id']]['tfidf_mean']=tfidf_mean
        IDFs[line['_id']]['count']=line['count']
    return IDFs

def TFIDF(keyword_count_per_user,movie_count_per_user,idf):
    # idf = math.log(float(sum_movie)/(sum_keyword+1))
    tf = float(keyword_count_per_user)/movie_count_per_user
    return tf*idf

def create_user_tfidf_list_for_movies(IDFs,user_num=120000):
    tfidfs={}
    allmovie_themes={}
    count_user=0
    count_kibana=0
    for line in movielens_user_data.find(no_cursor_timeout=True):
        count_user+=1
        themes = line['themes']
        uid = line['userId']
        keywords_for_user={}
        if len(line['likelist'])<20:
            continue        
        iids=[ d['imdbId'] for d in line['likelist']]
        for theme in themes:
            keywords_for_user[theme]=int(themes[theme])
        # print keywords_for_user
        if not len(keywords_for_user):
            continue
        ### remove all the keyword with only 1 count
        value_to_remove=1
        keywords_for_user={key: value for key, value in keywords_for_user.items() if value != value_to_remove}
        tfidf_user={}

        # print keywords_for_user
        for keyword in keywords_for_user:
            tfidf = TFIDF(keywords_for_user[keyword],len(line['likelist']),IDFs[keyword]['idf'])
            tfidf_user[keyword]=tfidf
            # tfidf_user['uid']=uid
            # json.dump({"index":{"_index":"themes","_type":"user_themes_tfidf","_id":count_kibana}},of)
            # of.write('\n')
            # count_kibana+=1
            # json.dump({'theme':keyword.replace(' ','_'), 'userid':uid,'tfidf':tfidf},of)
            # of.write('\n')

            if keyword in tfidfs:
                tfidfs[keyword].append(tfidf)
            else:
                tfidfs[keyword]=[]
                tfidfs[keyword].append(tfidf)
        # print tfidf_user.values()
        # print sorted(tfidf_user.items(),lambda x, y: cmp(x[1], y[1]),reverse=True)
        # tfidf_user = sorted(tfidf_user.items(),lambda x, y: cmp(x[1], y[1]),  reverse=True)[:10]
        # print tfidf_user
        if count_user >=user_num:
            print count_user 
            break
        end = time.time()
        print (end - start)/60
    return tfidfs

if __name__ == '__main__':
    start=time.time()
    IDFs=create_idf()
    tfidfs_mean={}
    # of=open('test.json','w')
    # data = create_idf()
    tfidfs = create_user_tfidf_list_for_movies(IDFs,1000)
    # print tfidfs['Heroic Mission']
    for line in tfidfs:
        ### diveded by mean_tfidf
        # print tfidfs[line]
        tfidfs_mean[line] = numpy.mean(tfidfs[line])#/IDFs[line]['tfidf_mean']

    
    # output_data['allmovie_themes']=[]
    # print sorted(tfidfs_mean.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
    print 
    print 
    for line in sorted(tfidfs_mean.items(), lambda x, y: cmp(x[1], y[1]), reverse=True):
        # print line[1]
        if not math.isnan(line[1]):
            print line, IDFs[line[0]]['tfidf_mean'],IDFs[line[0]]['count']
            # if line[0]=='Serial Killers'or line[0]=="Musician's Life":

            #     print [value/IDFs[line[0]]['tfidf_mean'] for value in tfidfs[line[0]][:200]]
            #     print numpy.mean([value/IDFs[line[0]]['tfidf_mean'] for value in tfidfs[line[0]][:200]])
        # print {line[0]:line[1]}
        
        
        output_data={line[0]:line[1]}
    # of.close()


    ### for plotting
    # tfidfs_mean=sorted(tfidfs_mean.items(), lambda x, y: cmp(x[1], y[1]), reverse=True)
    # plt.bar(range(len(tfidfs_mean)), [data[1] for data in tfidfs_mean], align='center')
    # plt.xticks(range(len(tfidfs_mean)), [data[0] for data in tfidfs_mean])
    # plt.show()
