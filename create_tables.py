import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):

    """drop_tables function is used to
       flush out the data from previous'
       process, if any and drop the
       database tables.
       Iterates over drop_table_queries -
       Queries to drop facts/dimensions."""
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    
    """create_tables function is used to
       create fact and dimension tables
       for the data model.
       Iterates over create_table_queries -
       Queries to create facts/dimensions."""
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['DWH'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()