# Setup

Normally I would run everything under a sandbox environment like virtualenv:

```bash
python3 -m virtualenv venv
. venv/bin/activate
```

## Install the required libraries

```bash
pip install -r requirements.txt
```


## Configure the parameters

The code provides an example configuration file "config.ini.sample" you will need to rename this as "config.ini" and adjust the variables accordingly to your environment:

```
[DEFAULT]
MongoDBURI = mongodb+srv://user:pass@cluster.mongodb.net/testdb?retryWrites=true&w=majority
Machines = 500
TimeInMinutes = 10
DB_NAME=testdb
COL_NAME=testcol
```
# Run the application

This is a simple test and it contains different scenarios, to run the needed scenario you will need to uncomment one or many of the following:

```python
initialize_db()
simple_2(loops)
#test_render()
#test_q1() # un sensor en un momento de tiempo
#test_q1a()
#test_q2()
#test_q3()
#test_q4()
#test_q5()
```
