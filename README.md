# BatchTests

Simple test to measure basic capabilities of a database. For the first time I'm using the "Faker" library for data generation. It's a little slow so I may revert to hand crafted approach in the future.


### Installation
You'll need Python 3.6 or higher installed on the plaform I recommend using a virtual env to create the needed environment.

Install the following python libs using the following command. You may need to update pip to install ```psycopg2-binary``` successfully 

```
pip install faker colorama halo psycopg2-binary
```

NOTE : You'll also need a user in the target database to run the test against. The database and user can be named anything you want.

### Running Batchtests

The command only takes the details of the target database, the size of the data sets you want to create and the number of threads used. A size of 1 (Default) will generate a 1GB data file. It will ultimately generate this dataset 3 times, meaning that a ```-s 1``` will create 3GB of data and 3 indexes so make sure you have enough storage to support any data set you create. The files it uses to load data into the database will be deleted during the run.  

```
BatchTests 0.1
usage: BatchTests.py [-h] -u USER -p PASSWORD -ho HOSTNAME -d DATABASE [-tc THREADS] -s SIZE [--debug]

Run simple batch like tests

optional arguments:
  -h, --help            show this help message and exit
  -u USER, --user USER  sys username
  -p PASSWORD, --password PASSWORD
                        sys password
  -ho HOSTNAME, --hostname HOSTNAME
                        hostmname of target database
  -d DATABASE, --database DATABASE
                        name of the database/service to run transactions against
  -tc THREADS, --threads THREADS
                        the number of threads used to simulate users running trasactions
  -s SIZE, --size SIZE  size of dataset i.e. 1 equivalent to 1GB
  --debug               enable debug
  ```
  
  For example the following command will generate a 1GB file and use 4 threads to process it where needed.
  
  ```
  $ python BatchTests.py -u soe -p soe -d soe -ho localhost -s 1 -tc 10
BatchTests 0.1
Generated serial data in 00:00:27
Concated files in 00:00:00
Created table in 00:00:00
Loaded data serially in 00:00:03
generated data parallel in 00:00:29
Loaded data parallel in 00:00:02
Created indexes in 00:00:11
generated data parallel in 00:00:29
Loaded data parallel in with indexes 00:00:08
Updated rows in 00:00:00
Scanned Data in 00:00:00
```
The scale (```-s```) can be a floating point number i.e. ```-s 0.1``` will create 100MB data file to load.