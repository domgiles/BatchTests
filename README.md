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

The command only takes the details of the target database, the size of the data sets you want to create and the number of threads used. A size of 1 (Default) will generate a 1GB data file. It will ultimately generate this dataset 3 times, meaning that a ```-s 1``` will create 3GB of data and 4 indexes (1 unique index for the primary key) so make sure you have enough storage to support any data set you create. The files it uses to load data into the database will be deleted during the run.  

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

The scale (```-s```) can be a floating point number i.e. ```-s 0.1``` will create 100MB data file to load.
  
For example the following command will generate a 5GB file and use 20 threads to process it where needed.
  
  ```
  $ python BatchTests.py -u soe -p soe -d soe -ho localhost -s 5 -tc 20
BatchTests 0.1
Generated seed data in 00:00:03
Written serial datafile to filesystem in 00:01:07
Concated files in 00:00:19
Created table in 00:00:03
Loaded data to database serially in 00:02:31
Written parallel datafiles to filesystem in 00:01:07
Loaded data to database in parallel in 00:02:15
Created indexes in 00:10:31
Written parallel datafiles to filesystem in 00:01:07
Loaded data to database in parallel in with indexes 00:05:03
Updated rows in 00:03:25
Scanned Data in 00:00:23
```
This will result in a 5GB file being generated to the file system. This file will then be loaded serially into the target database. Then 20 250MB files will be created and loaded into the same table. The python script will then create a primary key and 3 non unique indexes on the table and load the 20 250MB files into the table. This results in a 14GB table and 4 indexes (roughly 6GB in size). The script then updates a portion of the data set (non indexed column). Finally it does a quick scan. All of the files generated for data loading are deleted after their use. The data in the database isn't and currently you'll need to manually remove the generated table.

The output will be color coded. Only the numbers in red indicate are relevant to the performance of the database.