import argparse
import csv
import datetime
import os
import shutil
import time
from concurrent.futures.process import ProcessPoolExecutor

import psycopg2
from colorama import Fore, Style
from faker import Faker


class TransactionBench:
    drop_table = """drop table if exists customers_test"""

    table_defintion = """create table customers_test(
                            Id numeric,
                            Email varchar(50),
                            Prefix varchar(50),
                            Name varchar(50),
                            Birth_Date date,
                            Phone_Number varchar(50),
                            Additional_Email varchar(50),
                            Address varchar(200),
                            Zip_Code varchar(50),
                            City varchar(50),
                            State varchar(50),
                            Country varchar(50),
                            Yearjoined numeric,
                            Timejoined time,
                            Link varchar(200),
                            Comments varchar(50),
                            Occupation varchar(100),
                            Bank varchar(20),
                            Password varchar(50)
                            )"""

    create_pk = """ALTER TABLE customers_test ADD PRIMARY KEY (Id)"""

    create_index_1 = """CREATE INDEX CUST_INDEX_1 ON customers_test(Email)"""

    create_index_2 = """CREATE INDEX CUST_INDEX_2 ON customers_test(Birth_Date)"""

    create_index_3 = """CREATE INDEX CUST_INDEX_3 ON customers_test(City)"""

    update_statement = """UPDATE customers_test set Comments = 'Ive been updated' WHERE Occupation = 'Firefighter'"""

    select_statement = """select count(1) from customers_test where state in ('Alaska', 'Hawaii')"""

    # Probably don't need to make this a class. Everything is done in the init method.... but just in case.
    def __init__(self, args):
        self.username = args.user
        self.password = args.password
        self.hostname = args.hostname
        self.database = args.database
        self.size = args.size
        self.threads = args.threads

        records = int(3342227 * self.size)
        file_name = f'People_data_1_{records}.csv'

        start = time.time()
        all_files = self.generate_parallel(0)
        print(f'Generated serial data in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}')
        start = time.time()
        if len(all_files) > 1:
            all_files = self.concat_files(file_name, records, all_files)
        print(f'Concated files in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}')
        start = time.time()
        self.create_table()
        print(f'Created table in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}')
        start = time.time()
        self.load_data(all_files)
        print(f'Loaded data serially in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}')
        self.delete_files(all_files)
        start = time.time()
        all_files = self.generate_parallel(records + 1)
        print(f'generated data parallel in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}')
        start = time.time()
        self.load_data(all_files)
        print(f'Loaded data parallel in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}')
        self.delete_files(all_files)
        start = time.time()
        self.create_indexes()
        print(f'Created indexes in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}')
        start = time.time()
        all_files = self.generate_parallel(records * 2 + 1)
        print(f'generated data parallel in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}')
        start = time.time()
        self.load_data(all_files)
        print(f'Loaded data parallel in with indexes {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}')
        self.delete_files(all_files)
        start = time.time()
        self.update_data()
        print(f'Updated rows in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}')
        start = time.time()
        self.scan_data()
        print(f'Scanned Data in {time.strftime("%H:%M:%S", time.gmtime(time.time() - start))}')

    def concat_files(self, target_file, row_count, all_files, delete_orginals=True):
        with open(target_file, 'wb') as wfd:
            for f in [file[1] for file in all_files]:
                with open(f, 'rb') as fd:
                    shutil.copyfileobj(fd, wfd)
        if delete_orginals:
            for f in [file[1] for file in all_files]:
                os.remove(f)
        return [[all_files[0][0], target_file, row_count]]

    def delete_files(self, all_files):
        for f in [file[1] for file in all_files]:
            os.remove(f)

    def generate_parallel(self, starting_id):
        all_files = []
        records = int((3342227 * self.size) / self.threads)
        for thread_id in range(1, self.threads + 1):
            file_name = f'People_data_{thread_id}_{records}.csv'
            file_details = ['customers_test', file_name, records, int((thread_id - 1) * records) + starting_id]
            all_files.append(file_details)
        # with ThreadPoolExecutor(max_workers=self.threads) as executor:
        with ProcessPoolExecutor(max_workers=self.threads) as executor:
            executor.map(self.generate_dummy_data, all_files)
        return all_files

    def create_table(self):
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(self.drop_table)
                # connection.commit()
                cur.execute(self.table_defintion)
                connection.commit()

    def generate_dummy_data(self, file_details):
        start = time.time()
        records = file_details[2]
        headers = ["Id", "EmailId", "Prefix", "CustomerName", "BirthDate", "PhoneNumber", "AdditionalEmailId", "Address", "ZipCode", "City", "State", "Country", "YearJoined", "TimeJoined", "Link", "CustomerComments", "Occupation", "Bank", "Password"]
        fake = Faker('en_US', use_weighting=False)
        with open(file_details[1], 'wt', encoding='utf-8') as csvFile:
            writer = csv.DictWriter(csvFile, fieldnames=headers, delimiter="|", quoting=csv.QUOTE_NONE, )
            # writer = csv.DictWriter(csvFile)
            # writer.writeheader()
            for i in range(file_details[3], file_details[3] + records):
                full_name = fake.name()
                FLname = full_name.split(" ")
                Fname = FLname[0]
                Lname = FLname[1]
                domain_name = "@gmail.com"
                userId = Fname + "." + Lname + domain_name

                writer.writerow({
                    "Id": i,
                    "EmailId": userId,
                    "Prefix": fake.prefix(),
                    "CustomerName": full_name,
                    "BirthDate": fake.date(pattern="%d-%m-%Y", end_datetime=datetime.date(2000, 1, 1)),
                    "PhoneNumber": fake.phone_number(),
                    "AdditionalEmailId": userId,
                    "Address": fake.address().replace('\n', " "),
                    "ZipCode": fake.zipcode(),
                    "City": fake.city(),
                    "State": fake.state(),
                    "Country": "United States",
                    "YearJoined": int(fake.year()),
                    "TimeJoined": fake.time(),
                    "Link": fake.url(),
                    "CustomerComments": fake.word(),
                    "Occupation": fake.job(),
                    "Bank": fake.aba(),
                    "Password": fake.password()
                })

        end = time.time()
        # return int((end - start) * 1000)

    def get_connection(self):
        return psycopg2.connect(f"host={self.hostname} dbname={self.database} user={self.username} password={self.password}")

    def load_data(self, file_details):
        with ProcessPoolExecutor(max_workers=self.threads) as executor:
            executor.map(self.load_data_task, file_details)

    def load_data_task(self, file_details):
        try:
            with open(os.path.join(os.getcwd(), file_details[1]), 'r') as data_file:
                next(data_file)
                with psycopg2.connect(
                        f"host={self.hostname} dbname={self.database} user={self.username} password={self.password}") as connection:
                    with connection.cursor() as cur:
                        cur.copy_from(data_file, file_details[0], sep='|')
                        connection.commit()
                        # print(f"Loaded file {file_details[1]} with {file_details[2]} rows")
        except Exception as e:
            print(f"Got unexpected exception : {e}")

    def create_indexes(self):
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(self.create_pk)
                cur.execute(self.create_index_1)
                cur.execute(self.create_index_2)
                cur.execute(self.create_index_3)
                connection.commit()

    def update_data(self):
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(self.update_statement)
                connection.commit()

    def scan_data(self):
        with self.get_connection() as connection:
            with connection.cursor() as cur:
                cur.execute(self.select_statement)
                rows = cur.fetchall()


if __name__ == '__main__':
    print(f"{Style.BRIGHT}{Fore.RED}BatchTests 0.1{Style.RESET_ALL}")

    parser = argparse.ArgumentParser(description='Run simple batch like tests')
    # group = parser.add_mutually_exclusive_group(required=False)
    parser.add_argument('-u', '--user', help='sys username', required=True)
    parser.add_argument('-p', '--password', help='sys password', required=True)
    parser.add_argument('-ho', '--hostname', help='hostmname of target database', required=True)
    parser.add_argument('-d', '--database', help='name of the database/service to run transactions against', required=True)
    parser.add_argument('-tc', '--threads', help='the number of threads used to simulate users running trasactions', default=1, type=int)
    parser.add_argument('-s', '--size', help='size of dataset i.e. 1 equivalent to 1GB', default=1.0, required=True, type=float)
    parser.add_argument('--debug', help='enable debug', required=False, action='store_true')

    args = parser.parse_args()

    tb = TransactionBench(args)
