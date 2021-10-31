from flask import Flask, request
import sqlite3
import requests
from tqdm import tqdm
import json 
import numpy as np
import pandas as pd

app = Flask(__name__) 

@app.route('/')
def home():
    return 'Hello World'

@app.route('/stations/')
def route_all_stations():
    conn = make_connection()
    stations = get_all_stations(conn)
    return stations.to_json()

@app.route('/stations/<station_id>')
def route_stations_id(station_id):
    conn = make_connection()
    station = get_station_id(station_id, conn)
    return station.to_json()

@app.route('/stations/add', methods=['POST']) 
def route_add_station():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_stations(data, conn)
    return result

@app.route('/trips/')
def route_all_trips():
    conn = make_connection()
    trips = get_all_trips(conn)
    return trips.to_json()

@app.route('/trips/<trip_id>')
def route_trips_id(trip_id):
    conn = make_connection()
    trips = get_trip_id(trip_id, conn)
    return trips.to_json()

@app.route('/trips/add', methods=['POST']) 
def route_add_trips():
    # parse and transform incoming data into a tuple as we need 
    data = pd.Series(eval(request.get_json(force=True)))
    data = tuple(data.fillna('').values)
    
    conn = make_connection()
    result = insert_into_trips(data, conn)
    return result
    
@app.route('/json', methods=['POST'])
def json_example():
    
    req = request.get_json(force=True) # Parse the incoming json data as Dictionary
    
    name = req['name']
    age = req['age']
    address = req['address']
    
    return (f'''Hello {name}, your age is {age}, and your address in {address}
            ''')

@app.route('/trips/longest_duration') # Static Endpoints (Mendapatkan durasi terpanjang berdasarkan bikeid)
def route_trips_longdur():
    conn = make_connection()
    trips = get_trips_longdur(conn)
    return trips.to_json()

@app.route('/trips/longest_duration/<bike_id>') # Dynamic Endpoints (Mendapatkan durasi terpanjang berdasarkan bikeid)
def route_trips_longdur_id(bike_id):
    conn = make_connection()
    trips = get_trip_longdur_id(bike_id, conn)
    return trips.to_json()

@app.route('/stations/period', methods=['POST']) # POST Endpoints (Mendapatkan traffic stations pada periode tertentu)
def route_trips_period():
    req = request.get_json(force=True)
    period = req['period']
    station = req['station']
    conn = make_connection()
    result = get_trips_period(period, station, conn)
    return result.to_json()

########## Functions ##########

def get_all_stations(conn):
    query = f"""SELECT * FROM stations"""
    result = pd.read_sql_query(query, conn)
    return result

def get_station_id(station_id, conn):
    query = f"""SELECT * FROM stations WHERE station_id = {station_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_stations(data, conn):
    query = f"""INSERT INTO stations values {data}"""
    try:
        conn.execute(query)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_all_trips(conn):
    query = f"""SELECT * FROM trips"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_id(trip_id, conn):
    query = f"""SELECT * FROM trips WHERE id = {trip_id}"""
    result = pd.read_sql_query(query, conn)
    return result 

def insert_into_trips(datatrips, conn):
    querytrips = f"""INSERT INTO trips VALUES {datatrips}"""
    try:
        conn.execute(querytrips)
    except:
        return 'Error'
    conn.commit()
    return 'OK'

def get_trips_longdur(conn):  # Static Endpoints (Mendapatkan durasi terpanjang berdasarkan bikeid)
    query = f"""SELECT bikeid, MAX(duration_minutes) AS Max_Duration FROM trips GROUP BY bikeid ORDER BY Max_Duration DESC"""
    result = pd.read_sql_query(query, conn)
    return result

def get_trip_longdur_id(bike_id, conn): # Dynamic Endpoints (Mendapatkan durasi terpanjang berdasarkan bikeid)
    query = f"""SELECT * FROM trips WHERE bikeid = {bike_id} ORDER BY duration_minutes DESC"""
    result = pd.read_sql_query(query, conn)
    return result 

def get_trips_period(period, station, conn): # POST Endpoints (Mendapatkan traffic stations pada periode tertentu)
    query = f"""SELECT COUNT(bikeid),AVG(duration_minutes) FROM trips WHERE start_time like '{period}%' AND start_station_id = {station}"""
    result = pd.read_sql_query(query, conn)
    return result 

def make_connection():
    connection = sqlite3.connect('austin_bikeshare.db')
    return connection   

if __name__ == '__main__':
    app.run(debug=True, port=5000)