import dateutil.parser as dp
import extract_helper_functions as ex
import pandas as pd
import pytz
from datetime import datetime


def create_dataframes(recent_tracks):
    '''
    Returns play log, artist, album, and track Pandas DataFrames.
    Takes JSON object of recent tracks as returned by Spotify API request and extracts track attributes from JSON object.
    Cleans data (dates in particular) and deletes duplicates in tracks, artists, and albums tables.

    ARGUMENTS:
        recent_tracks: JSON object returned by API request for user's recent songs played from Spotify.
    '''
    # Instantiate list for list of lists data collection
    # Collect datetime today
    play_log_fact_data = []
    track_dim_data = []
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

        # Play log fact table row
        play_log_fact_data.append(
            {
                'time_track_key': unique_id, 
                'track_id': track_id,
                'artist_id': artist_id,
                'album_id': album_id,
                'played_at': played_at, 
                'date_appended': curr_date
            }
        )

        # Track dim table row
        track_dim_data.append(
            {
                'track_id': track_id, 
                'track_name': track_name, 
                'track_url': track_url, 
                'track_length_ms': track_length_ms, 
                'track_popularity': track_popularity, 
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

    # Play log fact table 
    play_log_fact_table = pd.DataFrame(play_log_fact_data)

    # Track dim  table
    track_dim_table = pd.DataFrame(track_dim_data)

    # Artist dim table
    artist_dim_table = pd.DataFrame(artist_dim_data)

    # Album dim table
    album_dim_table = pd.DataFrame(album_dim_data)
    album_dim_table['album_release_date'] = album_dim_table['album_release_date'].astype('datetime64[ns]')

    # Delete duplicate artists and albums from most recent 50 artists / albums
    for table in [track_dim_table, artist_dim_table, album_dim_table]:
        table.drop_duplicates(inplace=True)

    return play_log_fact_table, track_dim_table, artist_dim_table, album_dim_table
