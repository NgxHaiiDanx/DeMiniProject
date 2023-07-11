import json
import pymongo
from urllib.request import urlopen


def __ingest_to_collections(collection, data):
    myclient = pymongo.MongoClient("mongodb://haidannx:haidannx@miniproject-mongo-1:27017/")
    mydb = myclient["local"]
    if collection == "meta":
        mycol = mydb["meta"]
        mycol.insert_one(data)
    else:
        mycol = mydb["data"]
        mycol.insert_one(data)


def get_data_from_json(url):
    response = urlopen(url)
    data = json.loads(response.read())
    print(data)
    __ingest_to_collections("meta", data["meta"])
    for data in data["data"]:
        __ingest_to_collections("data", data)
    return True
