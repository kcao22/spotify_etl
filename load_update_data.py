import pandas as pd
import psycopg2
from sqlalchemy import create_engine


def load_update_tables(play_log_fact_table, track_dim_table, artist_dim_table, album_dim_table, hostname, database, username, pwd, port_id):
    '''
    Loads track, artist, and album DataFrames to specified PostgreSQL database. New data is appended to the respective tables and data is updated for non-static fields (e.g. popularity fields).

    ARGUMENTS:
        play_log_fact_table: Dataframe of recent tracks played data.
        track_dim_table: DataFrame of track data from recent tracks played.
        artist_dim_table: DataFrame of artist data from recent tracks played.
        album_dim_table: DataFrame of album data from recent tracks played.
        hostname: Host name credential for connecting to PostgreSQL.
        database: Name of database containing artist, album, track tables in PostgreSQL.
        username: Username credential for connecting to desired database in PostgreSQL.
        pwd: Password credential for connecting to desired database in PostgreSQL.
        port_id: Port number for connecting to desired database in PostgreSQL.
    '''
    # Connecting to postgreSQL database

    # Instantiating connection as None to avoid errors with close if script executes incorrectly.
    conn = None

    # Create SQLAlchemy Engine for insert into database tables
    engine = create_engine(f'postgresql://{username}:{pwd}@{hostname}:{port_id}/{database}')

    # Creating connection object, opens database connection
    try:
        with psycopg2.connect(
            host=hostname,
            dbname=database,
            user=username,
            password=pwd,
            port=port_id
        ) as conn:
            # cursor for storing return values
            with conn.cursor() as cur:  # Cursor closes at end of with statement block
                # SQL injection for each table
                artist_sql =  {
                    'dataframe': artist_dim_table,
                    'data_table': 'artists',
                    'staging_table': 'staging_artists', 
                    'primary_key': 'artist_id',
                    'sql_injection_updates': 'artist_followers = S.artist_followers, artist_popularity = S.artist_popularity' 
                }
                album_sql =  {
                    'dataframe': album_dim_table,
                    'data_table': 'albums',
                    'staging_table': 'staging_albums', 
                    'primary_key': 'album_id',
                    'sql_injection_updates': 'album_popularity = S.album_popularity, album_total_tracks = S.album_total_tracks' 
                }
                track_sql = {
                    'dataframe': track_dim_table,
                    'data_table': 'tracks',
                    'staging_table': 'staging_tracks',
                    'primary_key': 'track_id',
                    'sql_injection_updates': 'track_popularity = S.track_popularity'
                }
                log_sql = {
                    'dataframe': play_log_fact_table,
                    'data_table': 'play_log',
                    'staging_table': 'play_log_staging',
                    'primary_key': 'time_track_key',
                }
                # For each table, execute queries to update
                for table in [artist_sql, album_sql, track_sql, log_sql]:
                    # Append new data to staging table (to_sql creates table if not exists)
                    table['dataframe'].to_sql(
                        name=table['staging_table'],
                        con=engine,
                        if_exists='append',
                        index=False
                    )
                    # Insert new data into main data table
                    cur.execute(
                    f'''
                        INSERT INTO
                            {table['data_table']}
                        SELECT
                            T.*
                        FROM
                            {table['staging_table']} T
                            LEFT JOIN {table['data_table']} S ON
                                T.{table['primary_key']} = S.{table['primary_key']}
                        WHERE 
                            S.{table['primary_key']} IS NULL
                        '''
                    )
                    # Update non-static fields
                    if table != log_sql:  # Log table has no dynamic fields
                        cur.execute(
                        f'''
                            UPDATE
                                {table['data_table']} M
                            SET
                                {table['sql_injection_updates']}
                            FROM
                                {table['staging_table']} S
                            WHERE
                                M.{table['primary_key']} = S.{table['primary_key']}
                            '''
                        )
                    else:
                        pass
                    # Truncate staging tables
                    cur.execute(
                    f'''
                        TRUNCATE TABLE {table['staging_table']}
                        '''
                    )                
    except Exception as error:
        print(error)

    finally:  # Always executes as part of try except.
        if conn is not None:  # Close if not none
        # Exit connection
            conn.close()
        else:
            pass