import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    
    """load_staging_tables is used to
       extract the soucrce data from
       S3 bucket to staging tables.
       copy_table_queries - copy commands
       for staging tables."""
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """insert_tables is used to 
       insert data in final target
       tables from staging tables.
       insert_table_queries - INSERT commands
       for facts/dimensions."""
    
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)   #function call
    insert_tables(cur, conn)         #function call
    
    print("ETL script executed successfully.")

    conn.close()


if __name__ == "__main__":
    main()