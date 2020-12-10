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

edificios = []

def initialize_db():
    col.drop()
    col.create_index([ ( "date_from", DESCENDING ), ("edificio", ASCENDING), ("id", ASCENDING), ("sensor", ASCENDING) ])
    col.create_index([ ( "location", "2dsphere" )]) #    col.create_index([ ( "date_from", DESCENDING ), ("data.vars.id", ASCENDING), ("data.vars.value", ASCENDING) ])

# this will recover some values from the current database to use them
# as filters
def init_defaults():
    global edificios
    if len(edificios) == 0:
        edificios = [ r for r in col.distinct("edificio") ]

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
            date_from = datetime.now().replace(minute = 0,second=0, microsecond=0)
            for sensor in result["vars"]:
                filter = { "edificio": edificio, "id": result["id"], "sensor": sensor["id"], "date_from": date_from }
                update = { 
                    "$set": { "location": location },
                    "$inc": { "count": 1, "total_value_bucket": sensor["value"] },
                    "$min": { "min_value": sensor["value"] },
                    "$max": { "max_value": sensor["value"] },
                    "$push": { "data": { "date": datetime.now(), "vars": sensor["value"] } } 
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
    init_defaults()

    timestamp = datetime(2020, 12, 4, 15, 53, 34, 11000)
    id = "Maquina 0"
    building = edificios[0]
    sensor = "sensor2"

    date_from = timestamp.replace(second=0, microsecond=0)
    start_time = datetime.now()
    filter = { "$match": { "edificio": building, "id": id, "sensor": sensor, "date_from": date_from  } }
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
    init_defaults()

    q1_starttime = datetime(2020, 12, 10, 0, 0, 00)
    q1_endtime = datetime(2020, 12, 11, 23, 59, 00)
    id = "Maquina 0"
    building = edificios[0]
    sensor = "sensor4"

    start_time = datetime.now()
    filter = { "$match": { "edificio": building, "id": id, "sensor": sensor, "date_from": { "$gt": q1_starttime, "$lt": q1_endtime } } }
 #   unwind = { "$unwind": "$data" }
 #   unwind_sensors = { "$unwind": "$data.vars" }
 #   filter_sensor = { "$match": { "data.vars.id": sensor } }
 #   project = { "$project": {  "_id": 0, "data": { "date": "$data.date", "val": "$data.vars.value" } } }
 #   pipeline = [ filter, unwind, unwind_sensors, filter_sensor, project ]
    pipeline = [ filter ]

    for doc in col.aggregate(pipeline):
        print(doc)

    end_time = datetime.now()
    timespan = end_time - start_time
    print(pipeline)
    print(timespan)

#Q2: Obtener el promedio/max/min para un sensor a lo largo de una semana
def test_q2():
    init_defaults()

    q1_starttime = datetime(2020, 12, 1, 0, 0, 00)
    q1_endtime = datetime(2020, 12, 31, 0, 0, 00)
    id = "Maquina 0"
    building = edificios[0]
    sensor = "sensor4"

    start_time = datetime.now()
    filter = { "$match": { "edificio": building, "id": id, "sensor": sensor, "date_from": { "$gt": q1_starttime, "$lt": q1_endtime }  } }
    group = { 
        "$group": { 
            "_id": "$sensor", 
            "max": { "$max": "$max_value" },
            "total": { "$sum": "$total_value_bucket" },
            "total_count": { "$sum": "$count" },
            "min": { "$min": "$min_value" } 
        } 
    }
    project = { "$project": { "sensor": "$_id", "max": 1, "min": 1, "avg": { "$divide": [ "$total", "$total_count" ] } } }
    pipeline = [ filter, group, project ]

    for doc in col.aggregate(pipeline):
        print(doc)

    end_time = datetime.now()
    timespan = end_time - start_time
    print(pipeline)
    print(timespan)

#Q3: Obtener el máximo valor para todos los sensores de una tienda en un periodo de tiempo
def test_q3():
    init_defaults()

    q1_starttime = datetime(2020, 12, 1, 0, 0, 00)
    q1_endtime = datetime(2020, 12, 31, 0, 0, 00)
    id = "Maquina 0"
    building = edificios[0]

    start_time = datetime.now()
    filter = { "$match": { "edificio": building, "id": id, "date_from": { "$gt": q1_starttime, "$lt": q1_endtime }  } }
    group = { 
        "$group": { 
            "_id": "$sensor", 
            "max": { "$max": "$max_value" }
        } 
    }
    project = { "$project": { "_id": 0, "sensor": "$_id", "max": 1 } } 
    sort = { "$sort": { "sensor": 1 } }
    pipeline = [ filter, group, project, sort ]

    for doc in col.aggregate(pipeline):
        print(doc)

    end_time = datetime.now()
    timespan = end_time - start_time
    print(pipeline)
    print(timespan)

#Q4: Obtener el mínimo valor para todos los sensores geolocalizados dentro de un polígono en un periodo de tiempo
def test_q4():
    init_defaults()

    id = "Maquina 0"

    start_time = datetime.now()

    coordinates = [ [ -6.129370939621824, 37.89739938242827 ], [ -6.579652878076198, 37.62376882394451 ], [ -6.5357229328611215, 37.04717259965015 ], [ -5.964633645065341, 36.86278337906422 ], [ -5.431983059332746, 37.04278756973934 ], [ -5.388053114117691, 37.562824653938456 ], [ -6.129370939621824, 37.89739938242827 ] ] 

    filter = { "$match": { "location": { "$geoWithin": { "$geometry": { "type": "Polygon", "coordinates": [ coordinates ] } } } } } 
    group = { 
        "$group": { 
            "_id": "$sensor", 
            "min": { "$min": "$min_value" } 
        } 
    }
    project = { "$project": { "_id": 0, "sensor": "$_id", "min": "$min" } }
    sort = { "$sort": { "sensor": 1 }}
    pipeline = [ filter, group, project, sort ]

    for doc in col.aggregate(pipeline):
        print(doc)

    end_time = datetime.now()
    timespan = end_time - start_time
    print(pipeline)
    print(timespan)

# Obtener todos los sensores para los que un valor esté por encima de un umbral en un intervalo de tiempo
def test_q5():
    init_defaults()

    q1_starttime = datetime(2020, 12, 1, 0, 0, 00)
    q1_endtime = datetime(2020, 12, 31, 0, 0, 00)
    sensor = "sensor4"
    threshold = 200

    start_time = datetime.now()
    filter = { "$match": { "date_from": { "$gt": q1_starttime, "$lt": q1_endtime }, "sensor": sensor, "max_value": { "$gte": threshold } } }
    unwind = { "$unwind": "$data" }
    unwind_sensors = { "$unwind": "$data.vars" }
    filter_threshold = { "$match": { "data.vars": { "$gte": threshold }}}
    group = { "$group": { "_id": "$sensor", "max": { "$max": "$data.vars" } } }
    project = { "$project": { "sensor": "$_id", "max": "$max", "_id": 0 } }
    pipeline = [ filter, unwind, unwind_sensors, filter_threshold, group, project ]

    for doc in col.aggregate(pipeline):
        print(doc)

    end_time = datetime.now()
    timespan = end_time - start_time
    print(pipeline)
    print(timespan)

loops = 60*TIMEINMINS
#initialize_db()
#simple_2(loops)
test_q1() # un sensor en un momento de tiempo
test_q1a()
test_q2()
test_q3()
test_q4()
test_q5()
