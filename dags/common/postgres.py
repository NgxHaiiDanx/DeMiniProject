import psycopg2 as ps
from zipfile import ZipFile
import os
import requests


class PostgresExecutor:

    COPY_SQL = """
           COPY %s FROM stdin WITH CSV HEADER
           DELIMITER as ','
           """

    def __init__(self):
        self.conn = ps.connect(host='postgres',
                               database='postgres',
                               user='airflow',
                               password='airflow',
                               port=5432)
        self.cur = self.conn.cursor()

    def initialize_tables(self):
        self.cur.execute("""
        CREATE TABLE stations (
            id integer PRIMARY KEY,
            "name" varchar
        )""")
        self.cur.execute("""
        CREATE TABLE trips (
            trip_id bigint PRIMARY KEY,
            start_time timestamp,
            end_time timestamp,
            bikeid int,
            tripduration varchar,
            from_station_id int,
            from_station_name varchar,
            to_station_id int,
            to_station_name varchar,
            usertype varchar,
            gender varchar,
            birthyear int
        )""")
        self.conn.commit()
        print("Created 02 tables")

    def publish_jdbc(self, csv_uri, destination_table):
        with open(csv_uri, 'r') as f:
            # Skip the header row.
            next(f)
            self.cur.copy_from(f, destination_table, sep=',')
            self.conn.commit()

    def close_conn(self):
        self.conn.close()

    def ingest_fact_table_process(self, csv_uri, destination_table):
        self.initialize_tables()
        self.publish_jdbc(csv_uri, destination_table)
        self.close_conn()

    def create_dim_table_process(self):
        self.cur.execute("""
            insert into stations
            select distinct from_station_id as id, from_station_name as name
            from trips
        """)
        self.conn.commit()
        print("Finished insert to dim table")
        self.close_conn()


def get_file_name(url):
    li = url.rsplit("/", 1)[1]
    file_name = li.split(".")[0]
    return file_name


def process_table(uris):
    directory = "data_download"
    if os.path.exists(directory) is False:
        os.mkdir(directory)
    os.chdir(directory)

    for uri in uris:
        response = requests.get(uri)
        file_name = get_file_name(uri)
        open(file_name + '.zip', 'wb').write(response.content)
        with ZipFile(file_name + '.zip', 'r') as zipObject:
            list_files_name = zipObject.namelist()
            for fileName in list_files_name:
                if fileName.endswith('.csv'):
                    data = zipObject.extract(file_name + '.csv')
        if os.path.exists(file_name + '.zip'):
            os.remove(file_name + '.zip')
        print("File done: ", data)
        PostgresExecutor.ingest_fact_table_process(csv_uri=data, destination_table='trips')

    return True
