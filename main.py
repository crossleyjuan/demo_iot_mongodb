from pymongo import MongoClient, UpdateOne, InsertOne, ASCENDING, DESCENDING
from fake_wrapper import FakeWrapper, cacharro
from jinja2 import Template
import json
from datetime import datetime
import time
from dateutil import parser
import configparser

config = configparser.ConfigParser()
config.read("config.ini")

URI = config["DEFAULT"]["MongoDBURI"]
MACHINES = int(config["DEFAULT"]["Machines"])
TIMEINMINS = int(config["DEFAULT"]["TimeInMinutes"])
DB_NAME = config["DEFAULT"]["DB_NAME"]
COL_NAME = config["DEFAULT"]["COL_NAME"]

client = MongoClient(URI)

col = client[DB_NAME][COL_NAME]

def initialize_db():
    col.drop()
    col.create_index([ ( "date_from", DESCENDING ), ("edificio", ASCENDING), ("id", ASCENDING) ])
    col.create_index([ ( "location", "2dsphere" )])
#    col.create_index([ ( "date_from", DESCENDING ), ("data.vars.id", ASCENDING), ("data.vars.value", ASCENDING) ])

def get_template():
    with open("template.json") as f:
        return f.read()

def simple_1(loops):
    fake = FakeWrapper()

    template = Template(get_template())
    start_time = datetime.now()
    for x in range(0, loops):
        data = []
        for i in range(0, MACHINES):
            fake.current_cacharro(i)
            result = json.loads(template.render(data=fake))
            result["edificio"] = fake["building"]
            result["location"] = fake["location"]
            data.append(result)

        col.insert_many(data, ordered=False)
    end_time = datetime.now()
    print(end_time - start_time)

def test_render():
    fake = FakeWrapper()

    for x in range(0, 10):
        for i in range(0, 2):
            fake.current_cacharro(i)

            template = Template(get_template())
            result = template.render(data=fake)
            print(result)


def simple_2(loops):
    fake = FakeWrapper()

    template = Template(get_template())
    edificio = fake["building"]
    location = fake["location"]

    for x in range(0, loops):
        data = []
        requests = []
        start_time = datetime.now()
        for i in range(0, MACHINES):
            fake.current_cacharro(i)
            result = json.loads(template.render(data=fake))
            date_from = datetime.now().replace(second=0, microsecond=0)
            filter = { "edificio": edificio, "id": result["id"], "date_from": date_from }
            update = { 
                "$set": { "location": location },
                "$inc": { "count": 1 },
                "$push": { "data": { "date": datetime.now(), "vars": result["vars"] } } 
            }
            requests.append(UpdateOne(filter=filter, update=update, upsert=True))

        col.bulk_write(requests, ordered=False)

        end_time = datetime.now()
        timespan = end_time - start_time
        print(timespan)

        time.sleep((1000000 - timespan.microseconds) / 1000000)
        end_time2 = datetime.now()
#        print(end_time2 - start_time)


#Q1 Obtener el valor para un sensor y timestamp determinado
def test_q1():
    timestamp = datetime(2020, 12, 4, 15, 53, 34, 11000)
    id = "Maquina 0"
    building = "Medina, Webb and West"
    sensor = "sensor2"

    date_from = timestamp.replace(second=0, microsecond=0)
    start_time = datetime.now()
    filter = { "$match": { "edificio": building, "id": id, "date_from": date_from  } }
    unwind = { "$unwind": "$data" }
    filter_record = { "$match": { "data.date": timestamp } }
    unwind_sensors = { "$unwind": "$data.vars" }
    filter_sensor = { "$match": { "data.vars.id": sensor } }
    pipeline = [ filter, unwind, filter_record, unwind_sensors, filter_sensor ]

    print(pipeline)
    for doc in col.aggregate(pipeline):
        print(doc)

    end_time = datetime.now()
    timespan = end_time - start_time
    print(timespan)

#Q1a: Obtener los valores de un sensor de una maquina en un rango de tiempo
def test_q1a():
    q1_starttime = datetime(2020, 12, 4, 13, 0, 00)
    q1_endtime = datetime(2020, 12, 4, 16, 0, 00)
    id = "Maquina 0"
    building = "Medina, Webb and West"
    sensor = "sensor4"

    start_time = datetime.now()
    filter = { "$match": { "edificio": building, "id": id, "date_from": { "$gt": q1_starttime }  } }
    unwind = { "$unwind": "$data" }
    unwind_sensors = { "$unwind": "$data.vars" }
    filter_sensor = { "$match": { "data.vars.id": sensor } }
    project = { "$project": {  "_id": 0, "data": { "date": "$data.date", "val": "$data.vars.value" } } }
    pipeline = [ filter, unwind, unwind_sensors, filter_sensor, project ]

    for doc in col.aggregate(pipeline):
        print(doc)

    end_time = datetime.now()
    timespan = end_time - start_time
    print(pipeline)
    print(timespan)

#Q2: Obtener el promedio/max/min para un sensor a lo largo de una semana
def test_q2():
    q1_starttime = datetime(2020, 12, 4, 0, 0, 00)
    q1_endtime = datetime(2020, 12, 5, 0, 0, 00)
    id = "Maquina 0"
    building = "Medina, Webb and West"
    sensor = "sensor4"

    start_time = datetime.now()
    filter = { "$match": { "edificio": building, "id": id, "date_from": { "$gt": q1_starttime, "$lt": q1_endtime }  } }
    unwind = { "$unwind": "$data" }
    unwind_sensors = { "$unwind": "$data.vars" }
    filter_sensor = { "$match": { "data.vars.id": sensor } }
    group = { 
        "$group": { 
            "_id": "$data.vars.id", 
            "max": { "$max": "$data.vars.value" },
            "avg": { "$avg": "$data.vars.value" },
            "min": { "$min": "$data.vars.value" } 
        } 
    }
    pipeline = [ filter, unwind, unwind_sensors, filter_sensor, group ]

    for doc in col.aggregate(pipeline):
        print(doc)

    end_time = datetime.now()
    timespan = end_time - start_time
    print(pipeline)
    print(timespan)

#Q3: Obtener el máximo valor para todos los sensores de una tienda en un periodo de tiempo
def test_q3():
    q1_starttime = datetime(2020, 12, 4, 0, 0, 00)
    q1_endtime = datetime(2020, 12, 5, 0, 0, 00)
    id = "Maquina 0"
    building = "Medina, Webb and West"
    sensor = "sensor4"

    start_time = datetime.now()
    filter = { "$match": { "edificio": building, "id": id, "date_from": { "$gt": q1_starttime, "$lt": q1_endtime }  } }
    unwind = { "$unwind": "$data" }
    unwind_sensors = { "$unwind": "$data.vars" }
    group = { 
        "$group": { 
            "_id": "$data.vars.id", 
            "max": { "$max": "$data.vars.value" },
            "avg": { "$avg": "$data.vars.value" },
            "min": { "$min": "$data.vars.value" } 
        } 
    }
    pipeline = [ filter, unwind, unwind_sensors, group ]

    for doc in col.aggregate(pipeline):
        print(doc)

    end_time = datetime.now()
    timespan = end_time - start_time
    print(pipeline)
    print(timespan)

#Q4: Obtener el mínimo valor para todos los sensores geolocalizados dentro de un polígono en un periodo de tiempo
def test_q4():
    id = "Maquina 0"
    sensor = "sensor4"

    start_time = datetime.now()
    coordinates = [
            [
              40.12849105685408,
              -4.4384765625
            ],
            [
              39.65645604812829,
              -3.8671874999999996
            ],
            [
              39.99395569397331,
              -2.26318359375
            ],
            [
              40.9964840143779,
              -2.373046875
            ],
            [
              41.04621681452063,
              -4.15283203125
            ],
            [
              40.12849105685408,
              -4.4384765625
            ]
    ]
    #filter = { "$match": { "location": { "$geoWithin": { "$geometry": { "type": "Polygon", "coordinates": [ [ [ 43.31667, -2.68333 ], [ 40.812334, -3.148568 ], [ 40.072047, -3.088387 ], [ 43.31667, -2.68333 ] ] ] } } } } } 
    filter = { "$match": { "location": { "$geoWithin": { "$geometry": { "type": "Polygon", "coordinates": [ coordinates ] } } } } } 
    unwind = { "$unwind": "$data" }
    unwind_sensors = { "$unwind": "$data.vars" }
    group = { 
        "$group": { 
            "_id": "$data.vars.id", 
            "max": { "$max": "$data.vars.value" },
            "avg": { "$avg": "$data.vars.value" },
            "min": { "$min": "$data.vars.value" } 
        } 
    }
    pipeline = [ filter, unwind, unwind_sensors, group ]

    for doc in col.aggregate(pipeline):
        print(doc)

    end_time = datetime.now()
    timespan = end_time - start_time
    print(pipeline)
    print(timespan)

# Obtener todos los sensores para los que un valor esté por encima de un umbral en un intervalo de tiempo
def test_q5():
    q1_starttime = datetime(2020, 12, 4, 0, 0, 00)
    q1_endtime = datetime(2020, 12, 5, 0, 0, 00)
    sensor = "sensor4"
    threshold = 225

    start_time = datetime.now()
    filter = { "$match": { "date_from": { "$gt": q1_starttime }, "data.vars": { "$elemMatch": { "id": sensor, "value": { "$gte": threshold } } }  } }
    unwind = { "$unwind": "$data" }
    unwind_sensors = { "$unwind": "$data.vars" }
    filter_threshold = { "$match": { "data.vars.id": sensor, "data.vars.value": { "$gte": threshold }}}
    group = { "$group": { "_id": "$data.vars.id", "count": { "$sum": 1 }, "max": { "$max": "$data.vars.value" } }}
    project = { "$project": { "sensor": "$_id", "max": "$max", "count": "$count" } }
    pipeline = [ filter, unwind, unwind_sensors, filter_threshold, group, project ]

    for doc in col.aggregate(pipeline):
        print(doc)

    end_time = datetime.now()
    timespan = end_time - start_time
    print(pipeline)
    print(timespan)

loops = 60*TIMEINMINS
initialize_db()
#simple_1()
simple_2(loops)
#test_render()
#test_q1() # un sensor en un momento de tiempo
#test_q1a()
#test_q2()
#test_q3()
#test_q4()
#test_q5()
