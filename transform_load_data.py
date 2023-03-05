import dateutil.parser as dp
import extract_helper_functions as ex
import pandas as pd
import psycopg2
import psycopg2.extras
import pytz
from datetime import datetime
from sqlalchemy import create_engine


def create_dataframes(recent_tracks):
    '''
    Returns artist, album, and track Pandas DataFrames.
    Takes JSON object of recent tracks as returned by Spotify API request and extracts track attributes from JSON object.
    Cleans data (dates in particular) and deletes duplicates in artists and albums tables.

    ARGUMENTS:
        recent_tracks: JSON object returned by API request for user's recent songs played from Spotify.
    '''
    # Instantiate list for list of lists data collection
    # Collect datetime today
    track_fact_data = []
    artist_dim_data = []
    album_dim_data = []
    curr_date = datetime.today().date()

    # Traversing tracks and storing data in PySpark DataFrames
    # Performing transformations on timezones, date formats during data collection
    for i in range(len(recent_tracks['items'])):
        # Fact table data
        track_id = recent_tracks['items'][i]['track']['id']
        artist_id = recent_tracks['items'][i]['track']['album']['artists'][0]['id']
        album_id = recent_tracks['items'][i]['track']['album']['id']
        track_name = recent_tracks['items'][i]['track']['name']
        track_url = recent_tracks['items'][i]['track']['external_urls']['spotify']
        track_length_ms = recent_tracks['items'][i]['track']['duration_ms']
        track_popularity = recent_tracks['items'][i]['track']['popularity']
        played_at = pytz.utc.localize(datetime.strptime(recent_tracks['items'][i]['played_at'], '%Y-%m-%dT%H:%M:%S.%fZ')).astimezone(pytz.timezone('US/Pacific')).date()
        played_at_unix = str(dp.parse(recent_tracks['items'][i]['played_at']).timestamp()).replace('.', '')  # Conversion from Standard ISO 8610 datetime to UNIX seconds
        unique_id = played_at_unix + track_id

        # Artist data
        artist_name = recent_tracks['items'][i]['track']['album']['artists'][0]['name']
        artist_url = recent_tracks['items'][i]['track']['album']['artists'][0]['external_urls']['spotify']
        
        # Album data
        album_name = recent_tracks['items'][i]['track']['album']['name']
        album_url = recent_tracks['items'][i]['track']['album']['external_urls']['spotify']
        
        # Additional artist and album data
        token = ex.get_access_token()
        artist_followers, artist_popularity, album_popularity, album_total_tracks, album_release_date = ex.additional_info(token, artist_id=artist_id, album_id=album_id)

        # Tract fact table row
        track_fact_data.append(
            {
                'time_track_key': unique_id, 
                'track_id': track_id, 
                'artist_id': artist_id, 
                'album_id': album_id, 
                'track_name': track_name, 
                'track_url': track_url, 
                'track_length_ms': track_length_ms, 
                'track_popularity': track_popularity, 
                'played_at': played_at, 
                'curr_date': curr_date
            }
        )
        
        # Artist dim table row
        artist_dim_data.append(
            {
                'artist_id': artist_id,
                'artist_name': artist_name,
                'artist_url': artist_url,
                'artist_followers': artist_followers,
                'artist_popularity': artist_popularity
            }
        )

        # Album dum table row
        album_dim_data.append(
            {
                'album_id': album_id,
                'album_name': album_name,
                'album_url': album_url,
                'album_popularity': album_popularity,
                'album_total_tracks': album_total_tracks,
                'album_release_date': album_release_date
            }
        )

    # Creating DataFrames

    # Track fact table
    track_fact_table = pd.DataFrame(track_fact_data)

    # Artist dim table
    artist_dim_table = pd.DataFrame(artist_dim_data)

    # Album dim table
    album_dim_table = pd.DataFrame(album_dim_data)
    album_dim_table['album_release_date'] = album_dim_table['album_release_date'].astype('datetime64[ns]')

    # Delete duplicate artists and albums from most recent 50 artists / albums
    for table in [track_fact_table, artist_dim_table, album_dim_table]:
        table.drop_duplicates(inplace=True)

    return track_fact_table, artist_dim_table, album_dim_table
    

def load_update_tables(track_fact_table, artist_dim_table, album_dim_table, hostname, database, username, pwd, port_id):
    '''
    Loads track, artist, and album DataFrames to specified PostgreSQL database. New data is appended to the respective tables and data is updated for non-static fields (e.g. popularity fields).

    ARGUMENTS:
        track_fact_table: DataFrame of recent tracks played data.
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
                    'dataframe': track_fact_table,
                    'data_table': 'tracks',
                    'staging_table': 'staging_tracks',
                    'primary_key': 'time_track_key',
                    'sql_injection_updates': 'track_popularity = S.track_popularity'
                }
                # For each table, execute queries to update
                for table in [artist_sql, album_sql, track_sql]:
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