-- Total time listening to Spotify
CREATE OR REPLACE FUNCTION weekly_total_time_played()
RETURNS TABLE (hours_played DECIMAL)
AS 
$$
	SELECT 
		ROUND(SUM(CAST(T.track_length_ms AS DECIMAL) / 3600000), 2)
	FROM 
		play_log P
		JOIN tracks T ON
			P.track_id = T.track_id
	WHERE 
		P.played_at >= CURRENT_DATE - 7;
$$ LANGUAGE SQL;



-- Most popular songs played by track popularity
CREATE FUNCTION top_5_most_popular_songs()
RETURNS TABLE(song_artist_names TEXT)
AS 
$$
	SELECT CONCAT(T.track_name, ' by ', A.artist_name)
	FROM 
		play_log P
		JOIN tracks T
			ON P.track_id = T.track_id
		JOIN artists A
			ON P.artist_id = A.artist_id
	WHERE 
		P.played_at >= CURRENT_DATE - 7
	ORDER BY track_popularity DESC
	LIMIT 5;
$$ LANGUAGE SQL;

-- Most popular artists by popularity
CREATE OR REPLACE FUNCTION most_popular_artists()
RETURNS TABLE (artist_names TEXT)
AS 
$$
	SELECT A.artist_name
	FROM 
		(SELECT DISTINCT 
		 	artist_id 
		 FROM play_log
		 WHERE 
			played_at >= CURRENT_DATE - 7) P
		JOIN artists A ON
			P.artist_id = A.artist_id
	ORDER BY A.artist_popularity DESC
	LIMIT 5;
$$ LANGUAGE SQL;

-- Most popular albums by popularity
CREATE OR REPLACE FUNCTION most_popular_albums()
RETURNS TABLE (album_names TEXT)
AS
$$
	SELECT
		A.album_name
	FROM
	   (SELECT DISTINCT album_id
		FROM 
			play_log
		WHERE
			played_at >= CURRENT_DATE - 7) P
	JOIN albums A ON
		P.album_id = A.album_id
	ORDER BY A.album_popularity DESC
	LIMIT 5
;
$$ LANGUAGE SQL;

-- Most frequently played songs
CREATE OR REPLACE FUNCTION most_played_songs()
RETURNS TABLE(song_name TEXT, times_played SMALLINT)
AS
$$
	WITH cte AS (
		SELECT track_id, COUNT(*) times_played
		FROM
			play_log
		WHERE 
			played_at >= CURRENT_DATE - 7 
		GROUP BY 
			track_id
	)
	SELECT T.track_name, times_played
	FROM 
		cte C
		JOIN tracks T ON
		C.track_id = T.track_id
	ORDER BY C.times_played DESC, T.track_popularity DESC
	LIMIT 5
;
$$ LANGUAGE SQL;

SELECT * FROM most_played_songs()

-- Most frequently played artist
CREATE OR REPLACE FUNCTION most_frequently_played_artist()
RETURNS TABLE (artist_name TEXT, times_played SMALLINT)
AS
$$
	WITH cte AS (
		SELECT artist_id, COUNT(*) times_played
		FROM 
			play_log 
		WHERE 
			played_at >= CURRENT_DATE - 7
		GROUP BY 
			artist_id
	)
	SELECT A.artist_name, cte.times_played
	FROM 
		artists A
		JOIN cte ON
		A.artist_id = cte.artist_id
	ORDER BY cte.times_played DESC
	LIMIT 1
;
$$ LANGUAGE SQL;

-- Most frequently played album
CREATE OR REPLACE FUNCTION most_frequently_played_album()
RETURNS TABLE (album_name TEXT, times_played SMALLINT)
AS 
$$
	WITH cte AS (
		SELECT album_id, COUNT(*) times_played
		FROM 
			play_log
		WHERE 
			played_at >= CURRENT_DATE - 7
		GROUP BY 
			album_id
	)
	SELECT A.album_name, cte.times_played
	FROM 
		cte 
		JOIN albums A ON
		cte.album_id = A.album_id
	ORDER BY cte.times_played DESC
	LIMIT 1
;
$$ LANGUAGE SQL;

-- Artist played with the most followers
CREATE OR REPLACE FUNCTION artist_with_most_followers()
RETURNS TABLE (artist TEXT, artist_followers TEXT)
AS 
$$
	SELECT A.artist_name, TO_CHAR(A.artist_followers, 'FM9,999,999,999')
	FROM 
		play_log P
		JOIN artists A ON 
			P.artist_id = A.artist_id
	WHERE 
		P.played_at >= CURRENT_DATE - 7
	ORDER BY A.artist_followers DESC
	LIMIT 1
;
$$ LANGUAGE SQL;

-- Day of the week with most songs played or just number of songs played by day
CREATE OR REPLACE FUNCTION most_songs_played_in_week()
RETURNS TABLE (played_at DATE, songs_played SMALLINT)
AS
$$
	SELECT played_at, COUNT(*) times_played
	FROM 
		play_log
	WHERE played_at >= CURRENT_DATE - 7
	GROUP BY 
		played_at
	ORDER BY COUNT(*) DESC
	LIMIT 1
;
$$ LANGUAGE SQL;

-- Top 5 weekly songs by duration time
CREATE OR REPLACE FUNCTION longest_songs()
RETURNS TABLE (song_name TEXT, track_length_min DECIMAL)
AS 
$$
	SELECT 
		DISTINCT T.track_name, ROUND(CAST(T.track_length_ms AS DECIMAL) / 60000, 2)
	FROM 
		play_log P
		JOIN tracks T ON
		P.track_id = T.track_id
	WHERE 
		P.played_at >= CURRENT_DATE - 7
	ORDER BY ROUND(CAST(T.track_length_ms AS DECIMAL) / 60000, 2) DESC
	LIMIT 5
;
$$ LANGUAGE SQL;

-- Number of songs played by decade of release date
CREATE OR REPLACE FUNCTION songs_played_by_decade()
RETURNS TABLE (decade TEXT, songs_played SMALLINT)
AS
$$
	SELECT 
		CASE
			WHEN DATE_PART('Year', A.album_release_date) >= 2020 THEN '2020s'
			WHEN DATE_PART('Year', A.album_release_date) >= 2010 THEN '2010s'
			WHEN DATE_PART('Year', A.album_release_date) >= 2000 THEN '2000s'
			WHEN DATE_PART('Year', A.album_release_date) >= 1990 THEN '1990s'
			WHEN DATE_PART('Year', A.album_release_date) >= 1980 THEN '1980s'
			WHEN DATE_PART('Year', A.album_release_date) >= 1970 THEN '1970s'
			WHEN DATE_PART('Year', A.album_release_date) >= 1960 THEN '1960s'
			WHEN DATE_PART('Year', A.album_release_date) >= 1950 THEN '1950s'
			ELSE '1940s or older'
		END album_decade
		, COUNT(*) songs_played
	FROM 
		play_log P
		JOIN albums A ON
			P.album_id = A.album_id
	WHERE 	
		P.played_at >= CURRENT_DATE - 7
	GROUP BY
		CASE
			WHEN DATE_PART('Year', A.album_release_date) >= 2020 THEN '2020s'
			WHEN DATE_PART('Year', A.album_release_date) >= 2010 THEN '2010s'
			WHEN DATE_PART('Year', A.album_release_date) >= 2000 THEN '2000s'
			WHEN DATE_PART('Year', A.album_release_date) >= 1990 THEN '1990s'
			WHEN DATE_PART('Year', A.album_release_date) >= 1980 THEN '1980s'
			WHEN DATE_PART('Year', A.album_release_date) >= 1970 THEN '1970s'
			WHEN DATE_PART('Year', A.album_release_date) >= 1960 THEN '1960s'
			WHEN DATE_PART('Year', A.album_release_date) >= 1950 THEN '1950s'
			ELSE '1940s or older'
		END
;
$$ LANGUAGE SQL;
