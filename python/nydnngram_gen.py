from dask.distributed import Client, LocalCluster
import multiprocessing as mp
import numpy as np
import matplotlib.pyplot as plt
from time import sleep
import os
import glob
from dask.distributed import Client, wait
from dask_cuda import LocalCUDACluster
from dask.utils import parse_bytes
from dask import delayed
from dask import dataframe as dd
from dask import compute
from dask import persist
import cudf
import dask_cudf
import pandas as pd
import nltk
from nltk import ngrams
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import numpy as np
import numba as nb
import string
import sys
import hashlib

#nltk.download('stopwords')


## generates and store n grams
def get_all_csv_files(directory):
    return glob.glob(os.path.join(directory, '**', '*.CSV'), recursive=True)

@delayed
def generate_ngrams(x) -> int:
    #tokens = word_tokenize(text)
        return 2
    
@delayed
def myfoo(x):
    #words = r['text'].astype(str)
    if x is None or x == "":
        return ""
    myx = str(x)
    #print(myx)
    text = myx.translate(str.maketrans('', '', string.punctuation))
        # Tokenize the text
    words = nltk.word_tokenize(text.lower())

    words = [word for word in words if word not in stop_words and word.isalnum() and len(word) > 3]
    #return list(ngrams(word_tokenize(text.lower()),10))
    #print("words")
    #print(text)
    #print(words)
    #print(ngrams(word_tokenize(text.lower()),10))
    #return tuple(ngrams(word_tokenize(words.lower()),10))
    ngramlist = list(ngrams(words,5))
    hashes = [hashlib.md5(str(ngram).encode()).hexdigest() for ngram in ngramlist]
    #print(hashes)
    #print("length of hashes = " + str(len(hashes)))
    #return tuple(ngrams(words,5))
    return hashes

def myfoo_nodelay(x):
    #words = r['text'].astype(str)
    if x is None or x == "":
        return ""
    myx = str(x)
    #print(myx)
    text = myx.translate(str.maketrans('', '', string.punctuation))
        # Tokenize the text
    words = nltk.word_tokenize(text.lower())

    words = [word for word in words if word not in stop_words and word.isalnum() and len(word) > 3]
    #return list(ngrams(word_tokenize(text.lower()),10))
    #print("words")
    #print(text)
    #print(words)
    #print(ngrams(word_tokenize(text.lower()),10))
    #return tuple(ngrams(word_tokenize(words.lower()),10))
    ngramlist = list(ngrams(words,5))
    hashes = [hashlib.md5(str(ngram).encode()).hexdigest() for ngram in ngramlist]
    #print(hashes)
    #print("length of hashes = " + str(len(hashes)))
    #return tuple(ngrams(words,5))
    return hashes

@nb.njit
def tokenize_text(x):
    return x['url'].str.split()

def to_lower(text):
   return np.char.split('the quick brown fox')

@delayed
def validate_match(gram1,str2):  # Check whether two strings share enough n-grams to be a match
    
    ng1 = set(gram1)
    #print("received string unit test" + str(gram1)[0:50])
    #print(ng1)
    #ng2 = set(gram2)
    ng2 = set (myfoo(str2))
    #print(ng1)
    #print(ng2)
    if len(ng1) == 0 or len(ng2) == 0:
        return False, 0, 0
    inter = len(ng1.intersection(ng2))
    score1 = inter/len(ng1)
    score2 = inter/len(ng2)
    is_match = max(score1, score2)>0.5 and min(score1, score2)>0.2  # Here's the criteria I use to determine whether an article is a match
    return is_match, score1, score2


def validate_match_loop(x):  # Check whether two strings share enough n-grams to be a match
    #print("received string" + str(x)[0:50])
    ng1 = set(x)
    
    #print(list(ng1)[:10])
    #ng2 = set(gram2)
    ng2 = set (myfoo_nodelay(str2))
    #print(ng1)
    #print(ng2)
    if len(ng1) == 0 or len(ng2) == 0:
        return False, 0, 0
    inter = len(ng1.intersection(ng2))
    score1 = inter/len(ng1)
    score2 = inter/len(ng2)
    is_match = max(score1, score2)>0.5 and min(score1, score2)>0.2  # Here's the criteria I use to determine whether an article is a match
    if is_match:
        print("match found" + str(is_match) + str(score1) + str(score2))
    return is_match, score1, score2

if __name__ == '__main__':
    #sys.stderr = open(os.devnull, 'w')
    cluster = LocalCluster(
        n_workers=2,
        processes=True,
        threads_per_worker=2
    )
    client = Client(cluster)
    # print(client)
    #dtypes = {'digital_pub_date': str, 'print_pub_date': str, 'url': str,  'headline': str, 'is_archive_url': str, 'byline': str,'text': str, 'ngrams': str}
    nydndtypes = {'Title': str, 'URL': str,  'Copyright Owner': str, 'Copyright Registration': str, 'Registration Date': str, 
                  'Author(s)': str, 'Print Publication Date': str, 'Print Article Text': str}
    csvfiles = get_all_csv_files("/home/ubuntu/mypython/nydnsample")
    #df = read_data("/home/ubuntu/mypython/NYT_00192140.CSV")
    #df = [read_data(file) for file in csvfiles]
    for file in csvfiles:
        ddf = dd.read_csv(file,  blocksize="128MB", dtype=nydndtypes)
        ddfint = ddf.drop(columns=['Title', 'URL', 'Copyright Owner', 'Copyright Registration', 'Author(s)'], axis=1)
        #ddfsmall = ddfint.compute()
        print(ddfint.head(10))
        print("files read")
        #part = ddf.partitions[35]
        #part['ngrams'] = part['text'].apply(lambda x: myfoo(x), meta=('ngrams', 'str'))
        #df = dd.from_delayed(df, meta={'digital_pub_date': str, 'print_pub_date': str, 'url': str,  'headline': str, 'is_archive_url': str, 'byline': str,'text': str})
        #part = df.partitions[0]
        #print(part.head(20))
        #print(part.loc[4].compute())
        #print(df.describe().compute())
        # A = client.map(square, range(2))
        # B = client.map(neg, A)
        # total = client.submit(sum, B)
        # print(total.result())
        #print(part.loc[1].compute())
        #print(df.map_partitions(len).compute())

            
        #print(part.loc[1:100]['ngrams'].compute())
        #print(part.loc[1])
        #str1 = """herein are fictitious and bear abivable. For seldom, if ever, have actors been required to sail through the old story of the scapegrace American millionaire and the shy little alpine maid who fall in love with less of a script to convey them. It is downright painful to behold an expert cast of generally fun-loving cut-ups toil and struggle under the"""
        str2 = """Mary GordonBob . . . . . John WarburtonSuco . . . . . Claud AllisterBertie . . . . . Will StantonGovernor's Aide . . . . . Edgar NortonTess Bailey . . . . . Margaret RoachDuffy . . . . . Billy BevanIt was probably inevitable that the first film starring Annabella to be released following her romantic union with Tyrone Power should bear a title such as ""Bridal Suite,"" which opened yesterday at the Capitol. But don't be misled by the label; the characters depicted herein are fictitious and bear absolutely no similarity to actual persons, living or dead.In fact, they bear little resemblance to anything conceivable. For seldom, if ever, have actors been required to sail through the old story of the scapegrace American millionaire and the shy little alpine maid who fall in love with less of a script to convey them. It is downright painful to behold an expert cast of generally fun-loving cut-ups toil and struggle under the load of joyless dialogue and trite situation which is mercilessly heaped upon them.At the CapitolBRIDAL SUITE, screen play by Samuel Hoffenstein based on a story by Gottfried Reinhardt and Virginia Faulkner; directed by William Thiele; produced by Edgar Selwyn for Metro-Goldwyn-Mayer.Luise Anzengruber . . . . . AnnabellaNeil McGill . . . . . Robert YoungDoctor Grauer . . . . . Walter ConnollySir Horace Bragdon . . . . . Reginald OwenCornelius McGill . . . . . Gene LockhartLord Helfer . . . . . Arthur TreacherMrs. McGill . . . . . Billie BurkeAbbie Bragdon . . . . . Virginia FieldMaxl . . . . . Felix Bressart"""
        stop_words = set(stopwords.words('english'))
        
        #############
        lazy_results = []
        counter = 0
        for part in ddfint.partitions:
        #part = ddf.partitions[1:35]
            part['ngrams'] = part['Print Article Text'].apply(lambda x: myfoo_nodelay(x), meta=('ngrams', 'str'))
            part['lazyresults'] = part['ngrams'].apply(lambda x: validate_match_loop(x), meta=('lazyresults', 'object'))
            print(part.head(10))
            #validate_match(part.loc[0]['ngrams'],str2))
            #print(part.loc[0]['lazyresults'].compute()) 
            #lazy_results.append(validate_match(part.loc[0]['ngrams'],str2))
                # Check if the counter is modulo 5
            # if counter % 5 == 0:
            #     # Compute the results of the last 5 tasks
            #     futures = persist(*lazy_results)
            #     #print("computing results after futures")
            #     results = compute(*futures)
            #     # Print the results
            #     print(results)
            #     # Clear the lazy_results list
            #     lazy_results = []
            #     del results
            #     del futures
            #     del part
        #     print(validate_match(part.loc[1]['ngrams'],str2))
        #     print(validate_match(part.loc[2]['ngrams'], str2))
        #    counter += 1
            #df = client.persist(part['lazyresults'])
            #client.persist(df)
            #df = client.persist(ddfsub.loc[:10]['lazyresults'])
            #df = client.persist(part.loc[:100]['lazyresults'])
            #print("results in df")

            #result = compute(*df[:10])
            #result = compute(*df)
            #result = client.compute(df)
            #print(part.loc[3]['ngrams'].compute())
            #zvhngrams = part['ngrams'].compute()
            #print(zvhngrams.head(10))
            print ("memory used in Bytes by zvhngrams " + str(sys.getsizeof(part)))
            #part['ngrams_length'] = part['ngrams'].apply(len, meta=('ngrams_length', 'int'))
            #zvhtext = part['text'].compute()
            #print(zvhtext)
            #print ("memory used in Bytes by text " + str(sys.getsizeof(zvhtext)))
            part['ngrams_length'] = part['ngrams'].apply(len, meta=('ngrams_length', 'int'))
            #dflen = client.persist(part.loc[:10['ngrams_length'])
            #dflen = client.persist(part['ngrams_length'])
            #print(compute(*dflen))
            #client.compute(dflen)
            #average_length = dflen.mean().compute()
            #print("average length of ngram " + str(average_length))
            #print("total number of ngrams " + str(dflen.sum().compute()))
    
            #print(dflen.head(10))
            print ("memory used in Bytes by ngrams " + str(part['ngrams'].memory_usage(deep=True).compute()))
            print ("memory used in Bytes by frame " + str(part.memory_usage(deep=True).compute()))
            #print(result)
            #del dflen
            #del result
            #del part
            #del df
            #del ddfsub   
        
        #del ddf
        #del ddfint    
        # futures = persist(*lazy_results)
        # ddf['ngrams'] = ddf['text'].apply(lambda x: myfoo(x), meta=('ngrams', 'str'))
        # ddf['lazyresults'] = ddf['ngrams'].apply(lambda x: validate_match_loop(x), meta=('lazyresults', 'object'))
        # print("computing results after futures")
        # print( ddf.head(10)['ngrams'])
        # #futures = persist(ddf.loc[0:10]['lazyresults'])
        # #results = compute(*futures)
        # #print(results)
        # ddfsub = ddf.get_partition(2)
        # ddfsub = ddf.loc[:1]
        # print(ddfsub.head(10))
        # ddfsub['ngrams'] = ddfsub['text'].apply(lambda x: myfoo(x), meta=('ngrams', 'str'))
        # ddfsub['lazyresults'] = ddfsub['ngrams'].apply(lambda x: validate_match_loop(x), meta=('lazyresults', 'object'))
        #######
        
        
        # for part in ddf.partitions:
        #     part['ngrams'] = part['text'].apply(lambda x: myfoo_nodelay(x), meta=('ngrams', 'str'))
        #     part['lazyresults'] = part['ngrams'].apply(lambda x: validate_match_loop(x), meta=('lazyresults', 'object'))

        #     #df = client.persist(part['lazyresults'])
        #     #print(part.loc[2]['text'])
        # #df = client.persist(ddfsub.loc[:10]['lazyresults'])
        #     df = client.persist(part.loc[:100]['lazyresults'])
        #     #print("results in df")
        
        # #     result = compute(*df[:10])
        #     result = compute(*df)
        #     #print("printing part")
        #     #print(part.head(10))
        #     print(result)
        #     part['ngrams_length'] = part['ngrams'].apply(len, meta=('ngrams_length', 'int'))
        #     dflen = client.persist(part['ngrams_length'])
        #     print(compute(*dflen))
        #     average_length = dflen.mean().compute()
        #     print("average length of ngram " + str(average_length))
        #     print("total number of ngrams " + str(dflen.sum().compute()))
        #     print ("memory used in Bytes by ngrams " + str(part['ngrams'].memory_usage(deep=True).compute()))
        #         # print(ddf.head(10))
        #     dfout = part['ngrams']
        #     #print(part.head(10))
        #     #dfout.to_csv('/home/ubuntu/mypython/nytout_ngram/exportnew-*.csv') 
        #     #ddf.to_parquet("/home/ubuntu/mypython/nytout_ngram/", engine="pyarrow")
        #     del df
        #     del result
        #     del part
        #     #del ddfsub
        #######
        # #results = compute(df.loc[0]['lazyresults'])
        # #newfutures = persist(results)
        # #newresults = compute(*newfutures)
    
        
        #compute(print(*lazy_results)) 
        #print(*lazy_results)
        #print(part.loc[0]['ngrams'].compute())
        # for singleresults in results[1:3]:
        #     print(singleresults)

    #    sleep(10)
    #    cluster.close()
        # print(ddf.head(10))
        # dfout = ddf['ngrams']
        # print(dfout.head(10))
        # dfout.to_csv('/home/ubuntu/mypython/nytout_ngram/export-*.csv')  
        #print(ddf.head(10))
        #ddf.to_parquet("/home/ubuntu/mypython/nytout_ngram/", engine="pyarrow")
        print('Execution complete!')
    # print('unit test' )
    # asciigram = myfoo_nodelay(str1)
    # print(asciigram)
    # print("length of asciigram = " + str(len(asciigram)))
    #print(ngrams(word_tokenize("this is a fox"),10))
    #unittest = validate_match(myfoo(str1), str2).compute()
    #print(unittest)
#    sleep(2)
    print('Execution cluster close!')
    cluster.close()
