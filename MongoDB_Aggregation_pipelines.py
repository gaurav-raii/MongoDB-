# -*- coding: utf-8 -*-
"""
Created on Mon Apr 29 13:04:07 2019

@author: gaurav rai

All these queries are run against a mongoDB instance provided with the "Data Wrangling with MongoDB" course at Udacity

An example Document from the cities collection that we are investigating here.
{
    "_id" : ObjectId("52fe1d364b5ab856eea75ebc"),
    "elevation" : 1855,
    "name" : "Kud",
    "country" : "India",
    "lon" : 75.28,
    "lat" : 33.08,
    "isPartOf" : [
        "Jammu and Kashmir",
        "Udhampur district"
    ],
    "timeZone" : [
        "Indian Standard Time"
    ],
    "population" : 1140
}
"""

"""we have to find the average regional city population for all countries in the cities collection.
we will first calculate the average city population for each region in a country
and then calculate the average of all the regional averages for a country"""

from pymongo import MongoClient
client = MongoClient('localhost:27017')
db = client.examples



pipeline = [{"$unwind":"$isPartOf"},
            {"$group":{"_id":{"country": "$country","region":"$isPartOf"},"Avg_pop":{"$avg":"$population"}}},
            {"$group":{"_id":"$_id.country", "avgRegionalPopulation":{"$avg":"$Avg_pop"}}}]
    

print(db.cities.aggregate(pipeline))


"""next we will find Which Region in India
that has the largest number of cities with longitude between 75 and 80"""

pipeline = [{"$match":{"country":"India", "lon":{"$gt":75}, "lon":{"$lt":80}}},
            {"$unwind":"$isPartOf"},
            {"$group":{"_id":"$isPartOf", "count":{"$sum":1}}},
            {"$sort":{"count":-1}},
            {"$limit":1}
                
print(db.cities.aggregate(pipeline))

"""the average regional city population for all countries in the cities collection."""

pipeline = [{"$unwind":"$isPartOf"},
                {"$group":{"_id":{"country": "$country","region":"$isPartOf"}, "Avg_pop":{"$avg":"$population"}}},
                {"$group":{"_id":"$_id.country", "avgRegionalPopulation":{"$avg":"$Avg_pop"}}}]

print(db.cities.aggregate(pipeline))

