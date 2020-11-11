import argparse
import io
import os
import sys
import random
from datetime import datetime
from elasticsearch import Elasticsearch, helpers




class elasticSearchHandler:
    ElasticSearchConnection=''
    Host='null'
    Port=0
    connectionEstablished=False
    indexNames={}
    dataTypes={}
    bulkStructure=[]
    bulkCount = 0
    def __init__(self, Host='localhost', Port=9200,user=None,password=None):
        self.Host=Host
        self.Port=Port

        if (user != None):
            self.ElasticSearchConnection = Elasticsearch([{'host': Host, 'port': Port}], http_auth = (user, password), timeout=60, max_retries=200,retry_on_timeout=True)
        else:
            self.ElasticSearchConnection = Elasticsearch([{'host':Host,'port':Port}],timeout=60,max_retries=200, retry_on_timeout=True)

        if not self.ElasticSearchConnection.ping():
            self.connectionEstablished = False
        else:
            self.connectionEstablished = True
        self.bulkStructure = []
        for i in range(30):
            self.bulkStructure.append([])

    def isConnected(self):
        return self.connectionEstablished


    def createIndex(self,idx,mapping=None):
        if self.connectionEstablished == False:
            return False

        if self.ElasticSearchConnection.indices.exists(idx):
            return False
        res=""
        if (mapping == None):
            res = self.ElasticSearchConnection.indices.create(index=idx, ignore=400)
        else:
            res=self.ElasticSearchConnection.indices.create(index=idx, ignore=400, body=mapping)

        if "error" in res and res['error'] and res['error']['type'] != 'resource_already_exists_exception':
            print(res)
            return False
        return True
    
    def dropIndex(self,idx):
        if self.connectionEstablished == False:
            return False
        res = self.ElasticSearchConnection.indices.delete(index=idx, ignore=[400, 404])
        if "error" in res:

            if res['error']['type'] == 'index_not_found_exception':
                return True
            return False
        return True

    def insert(self,idx,doctype,dataInJSON,id=None):
        if self.connectionEstablished == False:
            return False
        res=""
        if (id == None):
            res = self.ElasticSearchConnection.index(index=idx, doc_type=doctype, body=dataInJSON)
        else:
            res = self.ElasticSearchConnection.index(index=idx, doc_type=doctype, id=id, body=dataInJSON)

        if "error" in res:
            return False
        #self.ElasticSearchConnection.indices.refresh(idx)

        return True

    def commit(self,idx):
        if self.connectionEstablished == False:
            return False
        self.ElasticSearchConnection.indices.refresh(idx)


    def get(self,idx,doctype,id):
        if self.connectionEstablished == False:
            return False
        res = self.ElasticSearchConnection.get(index=idx, doc_type=doctype, id=id, ignore=404)
        if "error" in res:
            return None
        return res

    def search(self,idx,query):
            if self.connectionEstablished == False:
                return False
            res=self.ElasticSearchConnection.search(index=idx,body=query,size=1000)
            return res

    def count(self, idx):
        if self.connectionEstablished == False:
            return False
        res = self.ElasticSearchConnection.count(index=idx, body={"query": {"match_all": {}}})
        return res

    def randomTuple2(self, idx, query):
        antall=self.count(idx)

        nr=random.randrange(int(antall['count'])) % 10001
        res = self.ElasticSearchConnection.search(index=idx, body=query, size=nr)
        #print(res['hits']['hits'][-1])
        return(res['hits']['hits'][-1])

    def randomTuple(self, idx, query):
        seed=random.randrange(1477072619038)
        randomquery='{"query": {"function_score": {"functions": [{"random_score": {"seed":' + str(seed) + '}}]}}}'

        res = self.ElasticSearchConnection.search(index=idx, body=randomquery, size=1)
        return (res['hits']['hits'][-1])

    def randomfields(self, idx,fieldname,ant):
        seed=random.randrange(1477072619038)
        randomquery='{"query": {"function_score": {"functions": [{"random_score": {"seed":' + str(seed) + '}}]}}}'

        res = self.ElasticSearchConnection.search(index=idx, body=randomquery, size=ant+2)
        randomFields=[]
        cnt=1
        for cnt in range(ant):
            print(res['hits']['hits'][cnt]["_source"][fieldname])
            randomFields.append(res['hits']['hits'][cnt]["_source"][fieldname])
        #print("ZZZZ")
        print(randomFields)

        return (randomFields)




    def listAllIndexes(self):
        if self.connectionEstablished == False:
            return False
        for index in self.ElasticSearchConnection.indices.get('*'):
            print(index)

    def snapshot(self):
        if self.connectionEstablished == False:
            return False
        self.ElasticSearchConnection.snapshot()
        for index in self.ElasticSearchConnection.indices.get('*'):
            print(index)

    def bulkInsert(self):
        if self.bulkCount == 0:
            return
        insertList = []
        for idx in self.indexNames:
            indexNo = self.indexNames[idx]
            for i in self.bulkStructure[indexNo]:
                data = {}
                data["_index"] = idx
                data["_type"] = self.dataTypes[idx]
                data["_source"] = i
                insertList.append(data)
        helpers.bulk(self.ElasticSearchConnection, insertList)
        self.indexNames = {}
        self.dataTypes = {}
        self.bulkStructure = []
        self.bulkCount = 0
        for i in range(30):
            self.bulkStructure.append([])

    def paralellBulkInsert(self):
        if self.bulkCount == 0:
            return
        insertList = []
        for idx in self.indexNames:
            indexNo = self.indexNames[idx]
            for i in self.bulkStructure[indexNo]:
                data = {}
                data["_index"] = idx
                data["_type"] = self.dataTypes[idx]
                data["_source"] = i
                insertList.append(data)
        for success,info in helpers.parallel_bulk(self.ElasticSearchConnection, insertList):
            if not success: print("doc failed", info)
        self.indexNames = {}
        self.dataTypes = {}
        self.bulkStructure = []
        self.bulkCount = 0
        for i in range(30):
            self.bulkStructure.append([])


    def addToBulk(self,idx,datatype,dataInJson):
        self.bulkCount+=1
        if idx in self.indexNames:
            indexNo=self.indexNames[idx]
            self.bulkStructure[indexNo].append(dataInJson)
        else:
            newindex=len(self.indexNames)
            self.indexNames[idx]=newindex
            self.dataTypes[idx]=datatype
            indexNo = newindex
            self.bulkStructure[indexNo].append(dataInJson)



    def printBulkBuffers(self):

        for idx in self.indexNames:
            indexNo = self.indexNames[idx]
            print("index:" + idx + "(" + str(indexNo) + ")" + " datatype: " + self.dataTypes[idx])
            for i in self.bulkStructure[indexNo]:
                print("Data:" + str(i))
