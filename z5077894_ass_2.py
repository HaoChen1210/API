# coding:utf-8
#use postman to test
import uuid
import time
from operator import itemgetter, attrgetter

from flask import Flask
from flask import jsonify
from flask import request
from flask import abort

import requests
import pymongo
from pymongo import MongoClient


app = Flask(__name__)
url = "mongodb://Hao:c951210@ds145752.mlab.com:45752/my-database"

def connectToDb(MONGOURL):
	client = MongoClient(MONGOURL)
	return client.get_database()

db = connectToDb(url)

@app.route("/<collections>", methods=['POST'])
def import_data(collections):
	params = request.get_json(force=True)
	indicator = params['indicator_id']
	document = db[collections].find_one({"indicator": params['indicator_id']})
	if document is None:
		doc = {}
		collection_id = str(uuid.uuid1())
		doc['collection_id'] = collection_id
		location = "/{}/{}".format(collections, collection_id)

		create_time = time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time()))
		doc['creation_time'] = create_time
		doc['indicator'] = indicator
		doc['indicator_value'] = 'GDP (current US$)'
		entries = []
		for i in range(1,3):
			url = "http://api.worldbank.org/v2/countries/all/indicators/{}?date=2012:2017&format=json&page={}".format(indicator, i)
			if len(requests.get(url).json()) == 1:
				abort(404)
			data = requests.get(url).json()
			for item in data[1]:
				data = {
					"country": item['country']['value'],
					"date": item['date'],
					"value": item['value']
				}
				entries.append(data)

		doc['entries'] = entries
		db[collections].insert_one(doc)
		return jsonify({
			"location": location,
			"collection_id": collection_id,
			"creation_time": time.strftime('%Y-%m-%d %H:%M:%S',time.localtime(time.time())),
			"indicator": params['indicator_id']
		}), 201
	else:
		return jsonify({
			"location": "/{}/{}".format(collections, document['collection_id']),
			"collection_id": document['collection_id'],
			"creation_time": document['creation_time'],
			"indicator": document['indicator']
		}), 200


@app.route("/<collections>/<collection_id>", methods=['DELETE'])
def delete(collections, collection_id):
	document = db[collections].find_one({"collection_id": collection_id})
	if document == None :
		mesg = {"message":"Collection = {} is not exit!".format(collection_id)}
		return jsonify(mesg),404
	db[collections].delete_one({"collection_id": collection_id})
	mesg = {"message":"Collection = {} is removed from the database!".format(collection_id)}
	return jsonify(mesg)


@app.route("/<collections>", methods=['GET'])
def get(collections):
	docs = db[collections].find()



	ret = []
	for doc in docs:
		ret.append({
			"location": "/{}/{}".format(collections, doc['collection_id']),
			"collection_id": doc['collection_id'],
			"creation_time": doc['creation_time'],
			"indicator": doc['indicator']
		})
	if not ret:
		mesg = {"message":"Collection  is not exit!"}
		return jsonify(mesg),404
	return jsonify(ret)


@app.route("/<collections>/<collection_id>", methods=['GET'])
def retrieval(collections, collection_id):
	docs = db[collections].find({"collection_id":collection_id})

	if docs == None :
		mesg = {"message":"Collection = {} is not exit!".format(collection_id)}
		return jsonify(mesg)

	ret=[]
	for doc in docs:
		ret.append({
			"location":"/{}/{}".format(collections, doc['collection_id']),
			"collection_id": doc['collection_id'],
			"creation_time": doc['creation_time'],
			"indicator": doc['indicator'],
			"entries": doc['entries']
		})
	if not ret:
		mesg = {"message":"Collection = {} is not exit!".format(collection_id)}
		return jsonify(mesg),404
	return jsonify(ret)


@app.route("/<collections>/<collection_id>/<year>/<country>", methods=['GET'])
def retrieval_by_country_and_year(collections, collection_id,year,country):
	doc = db[collections].find_one({"collection_id":collection_id})
	ret = {}
	ret['collection_id'] = collection_id
	ret['indicator'] = doc['indicator']
	ret['country'] = country
	ret['year'] = year

	for entry in doc['entries']:
		if entry['date'] == year and entry['country'] == country:
			ret['value'] = entry['value']
			break
	if not ret:
		mesg = {"message":"Collection  is not exit!"}
		return jsonify(mesg),404
	return jsonify(ret)


@app.route("/<collections>/<collection_id>/<year>", methods=['GET'])
def query(collections, collection_id, year):

	q = request.args['q']

	doc = db[collections].find_one({"collection_id": collection_id})

	ret = {}
	indicator = doc['indicator']
	indicator_value = doc['indicator_value']

	ret['indicator'] = indicator
	ret['indicator_value'] = indicator_value

	entries = doc['entries']
	sorted_list = []
	for entry in doc['entries']:
		if entry['date'] == year:
			sorted_list.append(entry)

	sorted_result = sorted(sorted_list, key=lambda v:v['value'], reverse=True)

	if q.startswith("top"):
		n = int(q[3:])
		ret['entries'] = sorted_result[:n]
	elif q.startswith("bottom"):
		n = int(q[6:])
		ret['entries'] = sorted_result[-n:]
	if not ret:
		mesg = {"message":"Collection  is not exit!"}
		return jsonify(mesg),404
	return jsonify(ret)


if __name__ == '__main__':

	app.run(debug=True)