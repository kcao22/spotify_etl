import pandas as pd
import requests
import spotipy
from spotipy.oauth2 import SpotifyOAuth

# Create spotipy client object
def create_spotipy_client(client_id, client_secret, redirect_uri, scope):
    '''
    Creates spotipy client object with authorization access to user's designated Spotipy application. Returns spotipy client object.

    ARGUMENTS:
        client_id: User's app client_id as seen via Spotify dashboard.
        client_secret: User's app client_secret as seen via Spotify dashboard.
        redirect_uri: Redirect URL used for acquiring authorization code.
        scope: Scope of accessibility for user's account.
    '''
    sp = spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=client_id,
            client_secret=client_secret,
            redirect_uri=redirect_uri,
            scope=scope
        )
    )
    return sp


# Request user's most recently played songs
def get_recent_played_tracks(spotipy_client, limit):
    '''
    Gets 50 most recently played songs from user's Spotify account. Spotipy will automatically grab authorization code, swap for access token, and contains a method that returns 50 most recently played songs from user's account. Returns user's most recently played tracks in JSON format.

    ARGUMENTS:    
        spotipy_client: Spotipy client object.
        limit: The number of songs recently played that the user would like to return.
    '''
    return spotipy_client.current_user_recently_played(limit=limit)
    

# Directly acquire access token for direct Spotify Web API functionality. For this project, this is used for additional artist and album details such as popularity.
def get_access_token():
    '''
    Uses Spotipy customized cache handler to acquire access token directly. Returns access token.

    ARGUMENTS:
        None
    '''
    handler = spotipy.CacheFileHandler()
    return handler.get_cached_token()['access_token']


# Access additional details of specified track's artist and album
def additional_info(token, artist_id, album_id):
    '''
    Interacts directly with Spotify Web API using access token associated with spotipy client object to acquire additional information for albums.
    Returns artist followers, artist popularity, album popularity, album total tracks, and album release date.

    ARGUMENTS:
        token: Access token required to access user's Spotify app.
        album_id: Unique Spotify ID associated with album of most recent song played.
        artist_id: Unique Spotify ID associated with artist of most recent song played.
    '''
    # Authentican headers
    auth_headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {token}'
    }

    # Concatenate specific artist and album IDs to get request URL
    get_artist_info_url = f'https://api.spotify.com/v1/artists/{artist_id}'
    get_album_info_url = f'https://api.spotify.com/v1/albums/{album_id}'

    # Returned JSON data on specified artist and album
    artist_info = requests.get(get_artist_info_url, headers=auth_headers).json()
    album_info = requests.get(get_album_info_url, headers=auth_headers).json()
    
    # Additional artist metrics
    artist_followers = artist_info['followers']['total']
    artist_popularity = artist_info['popularity']

    # Additional album metrics
    album_popularity = album_info['popularity']
    album_total_tracks = album_info['total_tracks']
    album_release_date = album_info['release_date']

    # Returned values
    return artist_followers, artist_popularity, album_popularity, album_total_tracks, album_release_date