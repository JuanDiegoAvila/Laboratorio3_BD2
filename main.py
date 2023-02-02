import pymongo
import csv
import json
import pandas as pd

client = pymongo.MongoClient('mongodb+srv://avilin:pilin@mondongo.lnml9zm.mongodb.net/?retryWrites=true&w=majority')
db = client['Mondongo']
collection = db['winemag-data']

with open("winemag-data.csv", "r", encoding='utf-8') as file:
    reader = csv.DictReader(file)
    data = list(reader)



for i in range(0, len(data), 20000):
    chunk = data[i:i+20000]
    collection.insert_many(chunk)
    print('subi chunk')
    