from flask import Flask,current_app, jsonify, request, abort
from flask_pymongo import PyMongo, ObjectId
from flask_apscheduler import APScheduler
import pandas as pd
import urllib
from bson import json_util
import json
import numpy as np

app = Flask(__name__, static_url_path='', static_folder='static')


app.config["MONGO_URI"] = "mongodb+srv://linto_mongodb:BtAUQm3cbhms28WO@cluster0.x3jziue.mongodb.net/flask_db?retryWrites=true&w=majority"
mongo = PyMongo(app)

listings = mongo.db.listings
scheduler = APScheduler()
scheduler.init_app(app)


@scheduler.task('interval', hours=24)
def downloadFileAndInsert():
    print("Downloading data")
    urllib.request.urlretrieve("http://data.insideairbnb.com/canada/on/toronto/2022-09-07/visualisations/listings.csv", "data.csv")
    print("Inserting data start")
    df = pd.DataFrame()
    for buffer in pd.read_csv("data.csv", chunksize=1000):
        df = pd.concat([df, buffer], ignore_index=True)
    listings.delete_many({})
    batchSize = 1000
    tempList = []
    for index, row in df.iterrows():
        tempList.append(row.to_dict())
        if index > 0 and index % batchSize == 0:
            listings.insert_many(tempList)   
            tempList = []     
    
    if len(tempList) > 0:
        listings.insert_many(tempList)
    print("Inserting data completed")

scheduler.start()

# downloadFileAndInsert()


@app.route('/')
def indexPage():
    return current_app.send_static_file('index.htm')

@app.route("/api/listings/")
def getAll():
    skipString = request.args.get('skip', '')
    skip = 0
    try: 
        skip = int(skipString)
    except:
        print("Error converting to int")
    allLisiting = listings.find({}).limit(10).skip(skip)
    return json.loads(json_util.dumps(allLisiting))

@app.route("/api/listings/<path:id>")
def getItemById(id):
    try:
        item = listings.find_one({"_id" : ObjectId(id)})
        if item == None:
            return {"error": "Item not found"}
        return json.loads(json_util.dumps(item))
    except:
         return {"error": "Please enter valid objectID"}

@app.route("/api/listings/count")
def listingCount():
    return {"count": listings.count_documents({})}

@app.route("/api/listings/license")
def listingLicense():
    hasLicense =listings.count_documents({"license":{"$ne":np.nan}})
    noLicense =listings.count_documents({"license":np.nan})
    return {"hasLicense": hasLicense, "noLicense": noLicense}

@app.route("/api/listings/roomtype")
def listingRoomType():
    roomTypesCount =listings.aggregate([
      {
      "$group" :
        {
          "_id" : "$room_type",
          "count": { "$sum": 1 }
        },
      },
      {"$project": { 
            "_id": 0,
            "name": "$_id",
            "count": 1,
            "sum": 1
        }}
    ])
    return json.loads(json_util.dumps(roomTypesCount))

@app.route("/api/listings/neighbourhood")
def listingNeighbourhoodPrice():
    neighbourhoodCount =listings.aggregate([
      {
      "$group" :
        {
          "_id" : "$neighbourhood",
          "count": { "$sum": 1 }
        },
      },
       { "$sort" : { "count" : -1 } },
        { "$limit" : 10},
      {"$project": { 
            "_id": 0,
            "name": "$_id",
            "count": 1,
            "sum": 1
        }},
        
    ])
    return json.loads(json_util.dumps(neighbourhoodCount))

