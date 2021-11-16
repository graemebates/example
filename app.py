import os

import boto3
from flask import Flask, jsonify, make_response, request
import dynamodbgeo
import uuid

app = Flask(__name__)

USERS_TABLE = os.environ['USERS_TABLE']
GEO_TABLE = 'geo_table'
dynamodb_client = boto3.client(
    'dynamodb', region_name='localhost', endpoint_url='http://localhost:8000',
      aws_access_key_id='DEFAULT_ACCESS_KEY', aws_secret_access_key='DEFAULT_SECRET'
)

# Set up geospatial client
config = dynamodbgeo.GeoDataManagerConfiguration(dynamodb_client, GEO_TABLE)
geoDataManager = dynamodbgeo.GeoDataManager(config)

# Pick a hashKeyLength appropriate to your usage
config.hashKeyLength = 3

# Use GeoTableUtil to help construct a CreateTableInput.
table_util = dynamodbgeo.GeoTableUtil(config)
create_table_input=table_util.getCreateTableRequest()

#tweaking the base table parameters as a dict
create_table_input["ProvisionedThroughput"]['ReadCapacityUnits']=5

# Use GeoTableUtil to create the table
table_util.create_table(create_table_input)

@app.route('/users/<string:username>')
def get_user(username):
    result = dynamodb_client.get_item(
        TableName=USERS_TABLE, Key={'username': {'S': username}}
    )
    item = result.get('Item')
    if not item:
        return jsonify({'error': 'Could not find user with provided "username"'}), 404

    return jsonify(
        {'username': item.get('username').get('S'), 'first_name': item.get('first_name').get('S'), 'last_name': item.get('last_name').get('S')}
    )


@app.route('/users', methods=['POST'])
def create_user():
    username = request.json.get('username')
    first_name = request.json.get('first_name')
    last_name = request.json.get('last_name')

    if not username or not first_name or not last_name:
        return jsonify({'error': 'Please provide "username", "first_name" and "last_name"'}), 400

    dynamodb_client.put_item(
        TableName=USERS_TABLE, Item={'username': {'S': username}, 'first_name': {'S': first_name}, 'last_name': {'S': last_name}}
    )

    return jsonify({'username': username, 'first_name': first_name, 'last_name': last_name})


@app.route('/properties', methods=['POST'])
def create_property():
    username = request.json.get('username')
    property_name = request.json.get('property_name')
    city = request.json.get('city')
    province = request.json.get('province')
    description = request.json.get('description')
    longitude = request.json.get('longitude')
    latitude = request.json.get('latitude')

    if not username or not property_name:
        return jsonify({'error': 'Please provide "username", "property_name"'}), 400

    # TODO: Check if invoked from user and username exists in user table

    item = {'Item': {'username': {'S': username}, 'property_name': {'S': property_name}, 'city': {'S': city}, 'province': {'S': province}, 'description': {'S': description}}}
    

    geoDataManager.put_Point(dynamodbgeo.PutPointInput(
        dynamodbgeo.GeoPoint(latitude, longitude),
         str( uuid.uuid4()), # Ensure uniqueness of hash and range
         item # rest of item
        ))

    return jsonify({'username': username, 'property_name': property_name})

@app.route('/properties/radius', methods=['GET'])
def find_property():
    longitude = request.json.get('longitude')
    latitude = request.json.get('latitude')
    radius = request.json.get('radius')
    province = request.json.get('province')

    if not longitude or not latitude or not radius:
        return jsonify({'error': 'Please provide "longitude", "latitude", "radius"'}), 400


    QueryRadiusInput = {
            "FilterExpression": "province = :val1",
            "ExpressionAttributeValues": {
                ":val1": {"S": province},
            }
        }

    query_radius_result=geoDataManager.queryRadius(
        dynamodbgeo.QueryRadiusRequest(
            dynamodbgeo.GeoPoint(latitude, longitude), # center point
            radius*2, QueryRadiusInput, sort = True)) # diameter
    print(query_radius_result)
    return jsonify({'properties': query_radius_result})


@app.errorhandler(404)
def resource_not_found(e):
    return make_response(jsonify(error='Not found!'), 404)
