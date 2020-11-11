import sys
import os
import elasticsearchHandler
import argparse
import json
import glob

from elasticsearchHandler import elasticSearchHandler


def createDataStructures(server, port, idx, MappingsFile, xmlFile):
    global globalElasticHandler
    elasticHandler = None
    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
    else:
        elasticHandler = elasticSearchHandler(server, port)
        globalElasticHandler = elasticHandler

    # elasticHandler = elasticSearchHandler(server,port)
    settingsfile = open(MappingsFile, "r")
    settings = settingsfile.read()

    elasticHandler.createIndex(idx, settings)

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


def dropIndex(server, port, idx):
    global globalElasticHandler
    elasticHandler = None
    if globalElasticHandler != None:
        elasticHandler = globalElasticHandler
    else:
        elasticHandler = elasticSearchHandler(server, port)
        globalElasticHandler = elasticHandler
    # print("drop")
    # elasticHandler = elasticSearchHandler(server, port)
    elasticHandler.dropIndex(idx)
    # print("drop")


if __name__ == '__main__':

        parser = argparse.ArgumentParser()
        parser.add_argument('server', help='Ipaddress or logicalname for elastic server')
        parser.add_argument('port', help='portnr for server')
        parser.add_argument('mappingsDir', help='Directory with all mapping files')

        args = parser.parse_args()
        mappingsString=args.mappingsDir + "/"+ "mappings.*"
        for filenow in glob.glob(mappingsString):
            indexName = filenow.split(".")[1]
            dropIndex(args.server, args.port, indexName)


        for filenow in glob.glob(mappingsString):
                indexName=filenow.split(".")[1]
                createDataStructure(args.server, args.port, indexName, filenow)
