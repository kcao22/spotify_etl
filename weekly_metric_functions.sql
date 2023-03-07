-- Total time listening to Spotify
CREATE FUNCTION weekly_total_time_played()
RETURNS DECIMAL
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
RETURNS TABLE(song_name TEXT, times_played INT)
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

