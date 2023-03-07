-- Check if database exists. If so, then delete. Drop check because PostgreSQL does not support CREATE DATABASE IF NOT EXISTS functionality.
DROP DATABASE IF EXISTS spotify_project;

-- Create database with 1 limited connection for security purposes.
CREATE DATABASE spotify_project
	WITH
	CONNECTION LIMIT = 1;

-- Create table for artists
CREATE TABLE IF NOT EXISTS artists (
	artist_id CHAR(22) PRIMARY KEY,
	artist_name TEXT,
	artist_url TEXT, 
	artist_followers INT,
	artist_popularity SMALLINT
);

-- Create table for albums
CREATE TABLE IF NOT EXISTS albums (
	album_id CHAR(22) PRIMARY KEY,
	album_name TEXT,
	album_url TEXT,  
	album_popularity SMALLINT,
	album_total_tracks SMALLINT,
	album_release_date DATE
);

CREATE TABLE IF NOT EXISTS tracks (
	track_id CHAR(22) PRIMARY KEY,
	track_name TEXT,
	track_url TEXT,
	track_length_ms INT,
	track_popularity SMALLINT
);

-- Create table for tracks
CREATE TABLE IF NOT EXISTS play_log (
	time_track_key TEXT PRIMARY KEY,
	track_id CHAR(22) REFERENCES tracks (track_id),
	artist_id CHAR(22) REFERENCES artists (artist_id),
	album_id CHAR(22) REFERENCES albums (album_id),
	played_at DATE,
	date_appended DATE
);
