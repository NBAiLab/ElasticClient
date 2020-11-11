import sys
import os
import xmlHandler
import elasticsearchHandler
import argparse
import json
import glob



from elasticsearchHandler import elasticSearchHandler


def createDataStructure(server, port, idx, MappingsFile):

    elasticHandler = elasticSearchHandler(server, port)

    # elasticHandler = elasticSearchHandler(server,port)
    settingsfile = open(MappingsFile, "r")
    settings = settingsfile.read()
    elasticHandler.createIndex(idx, settings)

if __name__ == '__main__':

        parser = argparse.ArgumentParser()
        parser.add_argument('server', help='Ipaddress or logicalname for elastic server')
        parser.add_argument('port', help='portnr for server')
        parser.add_argument('indeksName', help='Navn på indeks')
        parser.add_argument('definisjonsfil', help='Fil med definisjoner for layout')
        args = parser.parse_args()

        createDataStructure(args.server, args.port, args.indeksName, args.definisjonsfil)