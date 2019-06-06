#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
from pymongo import MongoClient, DESCENDING, ASCENDING
import yaml

if __name__ == '__main__':

    conf = "config.yaml"
    config = yaml.load(open(conf))
    mongo_conn = config['mongodb']['conn_str']
    dest_mongo_conn = config['mongodb']['dest_conn_str']

    mongo_client = MongoClient(mongo_conn)
    dest_mongo_client = MongoClient(dest_mongo_conn)

    mongo_db = mongo_client['tacloud']
    dest_mongo_db = dest_mongo_client['tacloud']

    #获取源数据库中 所有的collections 并遍历
    collection_names = mongo_db.collection_names()

    for collection_name in collection_names:
        print("Start get the indexex of Collection [" + collection_name + "]")
        if collection_name == 'system.profile':
            continue
        
        collection = mongo_db[collection_name]
        dest_collection = dest_mongo_db[collection_name]
        #获取源数据库中 该表的所有索引 并遍历
        indexes = collection.list_indexes()
        for index in indexes:
            
            keys = index['key'].keys()
            index = index['key']

            dest_indexes = []
            for key in keys:
                if key == "_id":
                    continue
                #print(key + "    " + str(index[key]) )
                if index[key] == 1.0:
                    dest_indexes.append((key, ASCENDING))
                else:
                    dest_indexes.append((key, DESCENDING))
            
            if len(dest_indexes) == 0:
                continue
            print("Collection [" + collection_name+"]'s index is:" + str(dest_indexes) )
            #在目标数据库中 创建索引
            dest_collection.create_index(dest_indexes)
            #my_collection.create_index([("mike", pymongo.DESCENDING),("eliot", pymongo.ASCENDING)])

