import sys
import os
import xmlHandler
import elasticsearchHandler
import argparse
import json
import glob



from elasticsearchHandler import elasticSearchHandler
from sandboxLogger import SandboxLogger

from xmlHandler import xmlHandler
s=SandboxLogger("myLogger","logging_config.config")

globalElasticHandler = None
def insertMytest(server,port):
    global globalElasticHandler

    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
    else:
        elasticHandler = elasticSearchHandler(server, port)
        globalElasticHandler = elasticHandler
    data = {}
    data['Filnavn'] = 'Testing 1 and 2'
    data['Dato'] = "01012011"
    data['Frame'] = float("55.7507178")
    data['Sannsynlighet'] = float("37.6176606")
    data['Tall'] = int("29")

    json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
    elasticHandler.insert("mytest", "Mytest", json_data)
    print(json_data)

    data = {}
    data['Filnavn'] = 'Testing 1 and 2'
    data['Dato'] = "01022011"
    data['Frame'] = float("55.7507178")
    data['Sannsynlighet'] = float("37.6176606")
    data['Tall'] = int("29")
    json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
    elasticHandler.insert("mytest", "Mytest", json_data)
    print(json_data)
    data = {}
    data['Filnavn'] = 'Testing 1 and 2'
    data['Dato'] = "01022011 184500"
    data['Frame'] = float("55.7507178")
    data['Sannsynlighet'] = float("37.6176606")
    data['Tall'] = int("29")
    json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
    elasticHandler.insert("mytest", "Mytest", json_data)
    print(json_data)
    data = {}
    data['Filnavn'] = 'Testing 1 and 2'
    data['Dato'] = "01022011 194500"
    data['Frame'] = float("55.7507178")
    data['Sannsynlighet'] = float("37.6176606")
    data['Tall'] = int("29")
    json_data = json.dumps(data, ensure_ascii=False).encode('utf8')
    elasticHandler.insert("mytest", "Mytest", json_data)
    print(json_data)

    elasticHandler.commit("mytest")

def createDataStructure(server, port, idx, MappingsFile):
    global globalElasticHandler
    elasticHandler = None
    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
    else:
        elasticHandler = elasticSearchHandler(server, port)

    # elasticHandler = elasticSearchHandler(server,port)
    settingsfile = open(MappingsFile, "r")
    settings = settingsfile.read()
    elasticHandler.createIndex(idx, settings)


parser = argparse.ArgumentParser()
        parser.add_argument('server', help='Ipaddress or logicalname for elastic server')
        parser.add_argument('port', help='portnr for server')
        parser.add_argument('mappingsDir', help='Directory with all mapping files')
        parser.add_argument('masterfileName', help='File with all data for insertion')
        args = parser.parse_args()
        mappingsString=args.mappingsDir + "/"+ "mappings.mytest"

        indexName = mappingsString.split(".")[1]
        dropIndex(args.server, args.port, indexName)
        createDataStructure(args.server, args.port, indexName, mappingsString)

        globalElasticHandler = elasticSearchHandler(args.server, args.port)
        insertMytest(args.server, args.port)