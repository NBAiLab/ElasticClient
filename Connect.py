from datetime import datetime
from elasticsearch import Elasticsearch
import sys


es = Elasticsearch([{'host':'localhost','port':9200}])
print(es)

if not es.ping():
    raise ValueError("Connection failed")