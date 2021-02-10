from pymongo import MongoClient, UpdateOne, InsertOne, ASCENDING, DESCENDING
from fake_wrapper import FakeWrapper, cacharro
from jinja2 import Template
import json
from datetime import datetime, timedelta
import time
from dateutil import parser
import configparser
import os
import random
import logging
import time
from logtime import log_time
from bson import json_util
import json

URI = os.environ["URI"]
EXECUTION_TIME = int(os.environ["EXECUTION_TIME"])
DB_NAME = os.environ["DATABASE"]
COL_NAME = os.environ["COLLECTION"]
TEST_CASE = os.environ["TEST_CASE"]
TIME_IN_MILIS_BETWEEN_EXECUTIONS = int(os.environ["TIME_IN_MILIS_BETWEEN_EXECUTIONS"]) / 1000

client = MongoClient(URI)

buildings = []
random_times = []

col = client[DB_NAME][COL_NAME]

root = logging.getLogger(None)

logger = logging.getLogger(__name__)
console = logging.StreamHandler()

logger.setLevel(logging.DEBUG)
console.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
console.setFormatter(formatter)
logger.addHandler(console)


# this will recover some values from the current database to use them
# as filters
def init_defaults():
    global buildings
    global random_times
    if len(buildings) == 0:
        buildings = [ r for r in col.distinct("building") ]

    # Get Random sensor dates
    random_times = [n for n in col.aggregate([{"$sample": { "size": 100 }}])]
    logger.debug(len(random_times))

def get_random_sensor_data():
    global random_times
    rnd = int(random.uniform(0, len(random_times)-1))
    sensor = random_times[rnd]
    result = { "building": sensor["building"], "sensor": sensor["sensor"], "cacharro": sensor["cacharro"] }

    data = sensor["data"][int(random.uniform(0, sensor["count"]))]
    result["date"] = data["date"]
    result["value"] = data["value"]

    return result

def execute(pipeline):
    results = []
    for doc in col.aggregate(pipeline):
        results.append(doc)

    if len(results) == 0:
        logger.error("Pipeline: %s is not returning data" % pipeline)

    #logger.debug("Results: %d, pipeline: %s, results: %s" % (len(results), pipeline, results))
    return results

#Q1 Obtener el valor para un sensor y timestamp determinado
@log_time()
def test_q1():
    sensor_test_data = get_random_sensor_data()
    timestamp = sensor_test_data["date"]
    cacharro = sensor_test_data["cacharro"]
    sensor = sensor_test_data["sensor"]
    building = sensor_test_data["building"]
    date_from = timestamp.replace(second=0, microsecond=0)

    filter = { "$match": { "building": building, "cacharro": cacharro, "sensor": sensor, "date_from": date_from  } }
    unwind = { "$unwind": "$data" }
    filter_record = { "$match": { "data.date": timestamp } }
    pipeline = [ filter, unwind, filter_record ]

    execute(pipeline)

#Q1a: Obtener los valores de un sensor de una maquina en un rango de tiempo
@log_time()
def test_q1a():
    sensor_test_data = get_random_sensor_data()
    timestamp = sensor_test_data["date"]

    q1_starttime = timestamp - timedelta(minutes=3)
    q1_endtime = timestamp + timedelta(minutes=3)
    cacharro = sensor_test_data["cacharro"]
    sensor = sensor_test_data["sensor"]
    building = sensor_test_data["building"]

    start_time = datetime.now()
    filter = { "$match": { "building": building, "cacharro": cacharro, "sensor": sensor, "date_from": { "$gt": q1_starttime, "$lt": q1_endtime } } }
 #   unwind = { "$unwind": "$data" }
 #   unwind_sensors = { "$unwind": "$data.vars" }
 #   filter_sensor = { "$match": { "data.vars.id": sensor } }
 #   project = { "$project": {  "_id": 0, "data": { "date": "$data.date", "val": "$data.vars.value" } } }
 #   pipeline = [ filter, unwind, unwind_sensors, filter_sensor, project ]
    pipeline = [ filter ]

    execute(pipeline)

#Q2: Obtener el promedio/max/min para un sensor a lo largo de una semana
@log_time()
def test_q2():
    sensor_test_data = get_random_sensor_data()
    timestamp = sensor_test_data["date"]

    q1_starttime = timestamp - timedelta(days=3)
    q1_endtime = timestamp + timedelta(days=4)
    cacharro = sensor_test_data["cacharro"]
    sensor = sensor_test_data["sensor"]
    building = sensor_test_data["building"]

    start_time = datetime.now()
    filter = { "$match": { "building": building, "cacharro": cacharro, "sensor": sensor, "date_from": { "$gt": q1_starttime, "$lt": q1_endtime }  } }
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

    execute(pipeline)

#Q3: Obtener el máximo valor para todos los sensores de una tienda en un periodo de tiempo
@log_time()
def test_q3():
    sensor_test_data = get_random_sensor_data()
    timestamp = sensor_test_data["date"]

    q1_starttime = timestamp - timedelta(days=15)
    q1_endtime = timestamp + timedelta(days=15)
    cacharro = sensor_test_data["cacharro"]
    building = sensor_test_data["building"]

    start_time = datetime.now()
    filter = { "$match": { "building": building, "cacharro": cacharro, "date_from": { "$gt": q1_starttime, "$lt": q1_endtime }  } }
    group = { 
        "$group": { 
            "_id": "$sensor", 
            "max": { "$max": "$max_value" }
        } 
    }
    project = { "$project": { "_id": 0, "sensor": "$_id", "max": 1 } } 
    sort = { "$sort": { "sensor": 1 } }
    pipeline = [ filter, group, project, sort ]

    results = execute(pipeline)
    logger.debug(results)

#Q4: Obtener el mínimo valor para todos los sensores geolocalizados dentro de un polígono en un periodo de tiempo
@log_time()
def test_q4():
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

    results = execute(pipeline)
    logger.debug(results)

# Obtener todos los sensores para los que un valor esté por encima de un umbral en un intervalo de tiempo
@log_time()
def test_q5():
    sensor_test_data = get_random_sensor_data()
    timestamp = sensor_test_data["date"]
    building = sensor_test_data["building"]
    cacharro = sensor_test_data["cacharro"]

    q1_starttime = timestamp - timedelta(days=15)
    q1_endtime = timestamp + timedelta(days=15)
    sensor = sensor_test_data["sensor"]

    threshold = 29

    start_time = datetime.now()
    filter = { "$match": { "building": building, "cacharro": cacharro, "sensor": sensor, "date_from": { "$gt": q1_starttime, "$lt": q1_endtime }, "max_value": { "$gte": threshold } } }
    unwind = { "$unwind": "$data" }
    filter_threshold = { "$match": { "data.value": { "$gte": threshold }}}
    group = { "$group": { "_id": "$sensor", "max": { "$max": "$data.value" } } }
    project = { "$project": { "sensor": "$_id", "max": "$max", "_id": 0 } }
    pipeline = [ filter, unwind, filter_threshold, group, project ]

    results = execute(pipeline)
    logger.debug(results)

@log_time()
def testmethod():
    import time
    time.sleep(2)

init_defaults()

start_time = datetime.now()
end_time = datetime.now() + timedelta(minutes=EXECUTION_TIME)

logger.debug("Starting Test Case %s" % TEST_CASE)
while (True):
    if TEST_CASE == "1":
        test_q1()
    if TEST_CASE == "1a":
        test_q1a()
    if TEST_CASE == "2":
        test_q2()
    if TEST_CASE == "3":
        test_q3()
    if TEST_CASE == "4":
        test_q4()
    if TEST_CASE == "5":
        test_q5()

    if datetime.now() > end_time:
        break
    time.sleep(TIME_IN_MILIS_BETWEEN_EXECUTIONS)

logger.debug("Test Completed %s" % TEST_CASE)
