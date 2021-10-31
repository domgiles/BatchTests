import argparse
import csv
import datetime
import logging
import os
import shutil
import subprocess
import time
from concurrent.futures.process import ProcessPoolExecutor
from datetime import timedelta
from random import random

import cx_Oracle
import mysql.connector
import psycopg2
from colorama import Fore, Style
from faker import Faker


class TransactionBench:
    drop_table_p = """drop table if exists customers_test"""
    drop_table_o = """drop table customers_test purge"""

    table_defintion_p = """create table customers_test(
                            Id numeric,
                            Email varchar(50),
                            Prefix varchar(50),
                            Name varchar(50),
                            Birth_Date date,
                            Phone_Number varchar(50),
                            Additional_Email varchar(50),
                            Address varchar(200),
                            Postcode varchar(50),
                            City varchar(50),
                            County varchar(50),
                            Country varchar(50),
                            Yearjoined numeric,
                            Timejoined time,
                            Link varchar(200),
                            Comments varchar(50),
                            Occupation varchar(100),
                            Bank varchar(20),
                            Password varchar(50)
                            )"""

    table_defintion_o = """create table customers_test(
                            Id number,
                            Email varchar(50),
                            Prefix varchar(50),
                            Name varchar(50),
                            Birth_Date date,
                            Phone_Number varchar(50),
                            Additional_Email varchar(50),
                            Address varchar(200),
                            Postcode varchar(50),
                            City varchar(50),
                            County varchar(50),
                            Country varchar(50),
                            Yearjoined number,
                            Timejoined timestamp,
                            Link varchar(200),
                            Comments varchar(50),
                            Occupation varchar(100),
                            Bank varchar(20),
                            Password varchar(50)
                            )"""

    control_file = """LOAD DATA APPEND INTO TABLE CUSTOMERS_TEST FIELDS TERMINATED BY "|"
                    (id,
                    email,
                    prefix,
                    name,
                    birth_date DATE "DD-MM-YYYY",
                    phone_number,
                    additional_email,
                    address,
                    postcode,
                    city,
                    county,
                    country,
                    yearjoined,
                    timejoined TIMESTAMP "HH24:mi:ss",
                    link,
                    comments,
                    occupation,
                    bank,
                    password)"""

    create_pk = """ALTER TABLE customers_test ADD PRIMARY KEY (Id)"""

    create_index_1 = """CREATE INDEX CUST_INDEX_1 ON customers_test(Email)"""

    create_index_2 = """CREATE INDEX CUST_INDEX_2 ON customers_test(Birth_Date)"""

    create_index_3 = """CREATE INDEX CUST_INDEX_3 ON customers_test(City)"""

    update_statement = """UPDATE customers_test set Comments = 'Ive been updated' WHERE Occupation = 'Firefighter'"""

    select_statement = """select count(1) from customers_test where county in ('Surrey', 'Shropshire')"""

    set_date_format = '''SET datestyle = "ISO, DMY"'''

    # Probably don't need to make this a class. Everything is done in the init method.... but just in case.
    def __init__(self, args):

        if args.debug:
            logging.basicConfig(level=logging.DEBUG)
            logging.getLogger("faker.factory").disabled = True
            self.debugging = True
        else:
            logger = logging.getLogger()
            logger.disabled = True
            self.debugging = False

        self.username = args.user
        self.password = args.password
        self.hostname = args.hostname
        self.database = args.database
        self.target = args.target
        self.connection_string = args.connectionstring
        self.size = args.size
        self.threads = args.threads
        self.seed_data = []
        self.seed_data_size = 10000
        self.delete_gen_file = not args.dontdelete



        records = int(3342227 * self.size)
        file_name = f'People_data_1_{records}.csv'
        total_time = 0

        start = time.time()
        # I'm rounding everything up to seconds. This is supposed to be a long running test and microseconds is likely a rounding error
        self.generate_seed_data()
        print(f'{Fore.LIGHTBLACK_EX}Generated seed data in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        start = time.time()
        all_files = self.generate_parallel(0)
        print(f'{Fore.LIGHTBLACK_EX}Written serial datafile to filesystem in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        start = time.time()
        if len(all_files) > 1:
            all_files = self.concat_files(file_name, records, all_files)
        print(f'{Fore.LIGHTBLACK_EX}Concated files in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        start = time.time()
        self.create_table()
        print(f'{Fore.LIGHTBLACK_EX}Created table in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        start = time.time()
        self.load_data(all_files, False)
        total_time += (time.time() - start)
        print(f'Loaded data to database serially in {Style.BRIGHT}{Fore.RED}{time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        self.delete_files(all_files)
        start = time.time()
        all_files = self.generate_parallel(records + 1)
        print(f'{Fore.LIGHTBLACK_EX}Written parallel datafiles to filesystem in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        start = time.time()
        self.load_data(all_files, False)
        total_time += (time.time() - start)
        print(f'Loaded data to database in parallel in {Style.BRIGHT}{Fore.RED}{time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        self.delete_files(all_files)
        start = time.time()
        self.create_indexes()
        total_time += (time.time() - start)
        print(f'Created indexes in {Style.BRIGHT}{Fore.RED}{time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        start = time.time()
        all_files = self.generate_parallel(records * 2 + 1)
        print(f'{Fore.LIGHTBLACK_EX}Written parallel datafiles to filesystem in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        start = time.time()
        self.load_data(all_files, True)
        total_time += (time.time() - start)
        print(f'Loaded data to database in parallel in with indexes {Style.BRIGHT}{Fore.RED}{time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        self.delete_files(all_files)
        start = time.time()
        self.update_data()
        total_time += (time.time() - start)
        print(f'Updated rows in {Style.BRIGHT}{Fore.RED}{time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        start = time.time()
        self.scan_data()
        total_time += (time.time() - start)
        time_delta = timedelta(seconds=total_time)
        print(f'Scanned Data in {Style.BRIGHT}{Fore.RED}{time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}{Style.RESET_ALL}')
        print(f"Total time taken for key tests {Style.BRIGHT}{Fore.RED}{str(time_delta).split('.')[0]}{Style.RESET_ALL}")

    @staticmethod
    def concat_files(target_file, row_count, all_files, delete_orginals=True):
        with open(target_file, 'wb') as wfd:
            for f in [file[1] for file in all_files]:
                with open(f, 'rb') as fd:
                    shutil.copyfileobj(fd, wfd)
        if delete_orginals:
            for f in [file[1] for file in all_files]:
                os.remove(f)
        return [[all_files[0][0], target_file, row_count]]

    def delete_files(self, all_files):
        if self.delete_gen_file:
            for f in [file[1] for file in all_files]:
                os.remove(f)
            if self.target == 'Oracle':
                os.remove('t1.ctl')
                os.remove('t1.log')

    def generate_parallel(self, starting_id):
        all_files = []
        records = int((3342227 * self.size) / self.threads)
        for thread_id in range(1, self.threads + 1):
            file_name = f'People_data_{thread_id}_{records}.csv'
            file_details = ['customers_test', file_name, records, int((thread_id - 1) * records) + starting_id]
            all_files.append(file_details)
        with ProcessPoolExecutor(max_workers=self.threads) as executor:
            executor.map(self.output_generated_data, all_files)
        return all_files

    def create_table(self):
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                if self.target == "PostgreSQL":
                    cur.execute(self.drop_table_p)
                    logging.debug(f"Statement executed : {self.drop_table_p}")
                    cur.execute(self.table_defintion_p)
                    logging.debug(f"Statement executed : {self.drop_table_p}")
                elif self.target == "Oracle":
                    try:
                        cur.execute(self.drop_table_o)
                        logging.debug(f"Statement executed : {self.drop_table_o}")
                    except Exception as e:
                        print(f"{Fore.LIGHTBLACK_EX}Table probably already existed : {e}{Fore.RESET}")
                    cur.execute(self.table_defintion_o)
                    logging.debug(f"Statement executed : {self.table_defintion_o}")
                # connection.commit()

                connection.commit()

    def generate_seed_data(self) -> []:
        fake = Faker('en_GB', use_weighting=False)
        for i in range(self.seed_data_size):
            full_name = fake.name()
            FLname = full_name.split(" ")
            Fname = FLname[0]
            Lname = FLname[1]
            domain_name = "@gmail.com"
            userId = Fname + "." + Lname + domain_name

            self.seed_data.append({
                "Id": i,
                "EmailId": userId,
                "Prefix": fake.prefix(),
                "CustomerName": full_name,
                "BirthDate": fake.date(pattern="%d-%m-%Y", end_datetime=datetime.date(2000, 1, 1)),
                "PhoneNumber": fake.phone_number(),
                "AdditionalEmailId": userId,
                "Address": fake.address().replace('\n', " "),
                "ZipCode": fake.postcode(),
                "City": fake.city(),
                "State": fake.county(),
                "Country": "United Kingdom",
                "YearJoined": int(fake.year()),
                "TimeJoined": fake.time(),
                "Link": fake.url(),
                "CustomerComments": fake.word(),
                "Occupation": fake.job(),
                "Bank": fake.aba(),
                "Password": fake.password()
            })

    def output_generated_data(self, file_details: []):
        records = file_details[2]
        headers = ["Id", "EmailId", "Prefix", "CustomerName", "BirthDate", "PhoneNumber", "AdditionalEmailId", "Address", "ZipCode", "City", "State", "Country", "YearJoined", "TimeJoined", "Link", "CustomerComments", "Occupation", "Bank", "Password"]
        with open(file_details[1], 'wt', encoding='utf-8') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames=headers, delimiter="|", quoting=csv.QUOTE_NONE, )
            for i in range(file_details[3], file_details[3] + records):
                rand_pointer = int(random() * self.seed_data_size)
                data = self.seed_data[rand_pointer]
                data["Id"] = i
                writer.writerow(data)

    # def get_connection(self):
    #     return psycopg2.connect(f"host={self.hostname} dbname={self.database} user={self.username} password={self.password}")

    def load_data(self, file_details, loading_with_indexes):
        with ProcessPoolExecutor(max_workers=self.threads) as executor:
            executor.map(self.load_data_task, file_details, (loading_with_indexes,))

    def load_data_task(self, file_details, loading_with_indexes):
        try:
            with open(os.path.join(os.getcwd(), file_details[1]), 'r') as data_file:
                next(data_file)
                if self.target == 'PostgreSQL':
                    with self.get_connection() as connection:
                        with connection.cursor() as cur:
                            cur.execute(self.set_date_format)
                            cur.copy_from(data_file, file_details[0], sep='|')
                            connection.commit()
                            # print(f"Loaded file {file_details[1]} with {file_details[2]} rows")
                elif self.target == 'Oracle':
                    oh = os.getenv('ORACLE_HOME')
                    if oh is None:
                        oh = ""
                    else:
                        oh = oh + "/"
                    fd = file_details[1]
                    with open('t1.ctl', 'w') as cfd:
                        cfd.write(self.control_file)
                    cf = 't1.ctl'
                    if self.connection_string is None:
                        cs = f"//{self.hostname}/{self.database}"
                    else:
                        cs = self.connection_string
                    if loading_with_indexes:
                        direct_load_string = ''
                    else:
                        direct_load_string = 'direct=true'
                    sqlldr_command = f"{oh}sqlldr userid={self.username}/{self.password}@'{cs}' data={fd} control={os.path.join(os.getcwd())}/{cf} silent=all direct_path_lock_wait=true {direct_load_string} parallel=true"
                    result = subprocess.run([sqlldr_command], stdout=subprocess.PIPE, cwd=os.getcwd(), shell=True)
                    if self.debugging:
                        print(f"{Fore.LIGHTRED_EX}DEBUG:root:Statement executed : {sqlldr_command}{Fore.RESET}")
                    if result.returncode != 0:
                        print(f"Command failed run sqlldr command : {sqlldr_command}")
        except Exception as e:
            print(f"Got unexpected exception : {e}")

    def get_connection(self):
        if self.target == 'PostgreSQL':
            return psycopg2.connect(f"host={self.hostname} dbname={self.database} user={self.username} password={self.password}")
        elif self.target == 'MySQL':
            return mysql.connector.connect(user=self.username, password=self.password, host=self.hostname, database=self.database)
        elif self.target == 'Oracle':
            if self.connection_string is None:
                return cx_Oracle.connect(self.username, self.password, f'//{self.hostname}/{self.database}')
            else:
                return cx_Oracle.connect(self.username, self.password, self.connection_string)

    def create_indexes(self):
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(self.create_pk)
                logging.debug(f"Statement executed : {self.create_pk}")
                cur.execute(self.create_index_1)
                logging.debug(f"Statement executed : {self.create_index_1}")
                cur.execute(self.create_index_2)
                logging.debug(f"Statement executed : {self.create_index_2}")
                cur.execute(self.create_index_3)
                logging.debug(f"Statement executed : {self.create_index_3}")
                connection.commit()

    def update_data(self):
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(self.update_statement)
                logging.debug(f"Statement executed : {self.update_statement}")
                connection.commit()

    def scan_data(self):
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(self.select_statement)
                logging.debug(f"Statement executed : {self.select_statement}")
                rows = cur.fetchall()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Run simple batch like tests')
    # group = parser.add_mutually_exclusive_group(required=False)
    parser.add_argument('-u', '--user', help='sys username', required=True)
    parser.add_argument('-p', '--password', help='sys password', required=True)
    parser.add_argument('-ho', '--hostname', help='hostmname of target database', required=False)
    parser.add_argument('-d', '--database', help='name of the database/service to run transactions against', required=False)
    parser.add_argument('-cs', '--connectionstring', help='a full connection string rather than using hostname and database', required=False)
    parser.add_argument('-tc', '--threads', help='the number of threads used to simulate users running trasactions', default=1, type=int)
    parser.add_argument('-t', '--target', help='PostgreSQL,MySQL,Oracle', required=True, choices=['MySQL', 'PostgreSQL', 'Oracle'])
    parser.add_argument('-s', '--size', help='size of dataset i.e. 1 equivalent to 1GB', default=1.0, required=True, type=float)
    parser.add_argument('-dd', '--dontdelete', help="dont delete generated files after run", required=False, action='store_true')
    parser.add_argument('--debug', help='enable debug', required=False, action='store_true')

    args = parser.parse_args()

    print(f"{Style.BRIGHT}{Fore.LIGHTRED_EX}BatchTests 0.2 running against {args.target} with scale {args.size}{Style.RESET_ALL}")
    print(f"{Style.DIM}{Fore.LIGHTRED_EX}Test started at {datetime.datetime.now():%Y-%m-%d %H:%M:%S}{Style.RESET_ALL}")

    tb = TransactionBench(args)
