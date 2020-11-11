

from elasticsearch import Elasticsearch
import sys


if (len(sys.argv)) != 2:
    print("Missing argument index name")
    exit()
print("Deleting index:" + sys.argv[1])
es = Elasticsearch()

# ignore 404 and 400
res=es.indices.delete(index=sys.argv[1], ignore=[400, 404])
print(res)

if "error" in res:
    print("juhu")