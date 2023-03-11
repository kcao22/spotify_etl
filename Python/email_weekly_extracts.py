import datetime
import pandas as pd
import psycopg2
import smtplib
import tabulate

from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP


def query_to_df(cur, sql_fxn, cols):
    '''
    Automate execution of SQL function that returns a table and creates a Pandas DataFrame. 

    ARGUMENTS:
        cur: Psycopg2 cursor
        sql_fxns: Name of SQL function to execute.
        cols: Name of columns for Pandas DataFrame creation of returned results.
    '''
    data = []
    cur.callproc(sql_fxn)
    for row in cur.fetchall():
        # Convert value to string to avoid SQL type constraints returned along with value
        data.append([str(val) for val in row])
    return pd.DataFrame(data, columns=cols)


def generate_weekly_email(sender, recipient, email_username, email_pwd, hostname, database, username, pwd, port_id):
    '''
    Calls SQL functions in database to generate weekly summary statistics for Spotify listening activities. Sends email message with SMTP protocol via Gmail.

    ARGUMENTS:
        sender: Email used for sending Spotify Weekly Recap statistics.
        recipient: Email for receiving Spotify Weekly Recap statistics.
        email_username: Username for SMTP access to email.
        email_pwd: Password for SMTP access to email.
        hostname: Host name credential for connecting to PostgreSQL.
        database: Name of database containing artist, album, track tables in PostgreSQL.
        username: Username credential for connecting to desired database in PostgreSQL.
        pwd: Password credential for connecting to desired database in PostgreSQL.
        port_id: Port number for connecting to desired database in PostgreSQL.
    '''

    # Connecting to postgreSQL database

    # Instantiating connection as None to avoid errors with close if script executes incorrectly.
    conn = None

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
                # Getting weekly metrics, tables

                # Total weekly listening time
                cur.callproc('weekly_total_time_played')
                weekly_total_time = float(cur.fetchone()[0])

                # Day with most songs played
                cur.callproc('most_songs_played_in_week')
                row = cur.fetchone()
                day_most_played = row[0]
                day_most_played_count = row[1]

                # For weekly listening information in tabular form (pd DF)
                # Dictionary with function name and cols
                tabular_info = {
                    'most_played_songs': ['Song Name', 'Times Played'],
                    'top_5_most_popular_songs': ['Song Name'],
                    'longest_songs': ['Song Name', 'Track Length (Minutes)'],
                    'songs_played_by_decade': ['Decade', 'Songs Played'],
                    'most_popular_artists': ['Artist Name'],
                    'most_frequently_played_artist': ['Artist Name', 'Times Played'],
                    'artist_with_most_followers': ['Artist Name', 'Followers'],
                    'most_popular_albums': ['Album Name'],
                    'most_frequently_played_album': ['Album Name', 'Times Played']
                }
                return_dfs = []
                for function in tabular_info.keys():
                    return_dfs.append(query_to_df(cur, function, tabular_info[function]))
                most_played_songs_df, top_5_pop_songs_df, longest_songs_df, decade_dist_df, popular_artists_df, freq_played_artist_df, most_followed_artist_df, most_popular_albums_df, most_freq_played_album_df = return_dfs

    except Exception as error:
        print(error)
    
    # Establishing week string
    week = f'{(datetime.datetime.today().date() - datetime.timedelta(days=6)).strftime("%m-%d-%Y")} - {datetime.datetime.today().date().strftime("%m-%d-%Y")}'
    
    # Email content
    msg = MIMEMultipart()
    msg['Subject'] = f'Your Spotify Weekly Recap ({week})'
    msg['From'] = sender



    # Plain text body
    text = f'''\
    Here is your weekly Spotify activity recap for {week}!

    You spent a total of {weekly_total_time} hours listening to Spotify.
    You listened to the most songs on {day_most_played}, with a total of {day_most_played_count} songs played.

    Below are tables of your weekly listening activities.
    '''
    # HTML Body
    html = f'''\
    <html>
    <body>
        <h4>
        Your most played songs were:
        </h4>
        <p>
        {tabulate.tabulate(most_played_songs_df, headers='keys', tablefmt='html', showindex=False)}
        </p>
        <h4>
        The top 5 most popular songs you played were:
        </h4>
        <p>
        {tabulate.tabulate(top_5_pop_songs_df, headers='keys', tablefmt='html', showindex=False)}
        </p>
        <h4>
        The longest songs you listened to were:
        </h4>
        <p>
        {tabulate.tabulate(longest_songs_df, headers='keys', tablefmt='html', showindex=False)}
        </p>
        <h4>
        Your song release date distributed by decade looks like:
        </h4>
        <p>
        {tabulate.tabulate(decade_dist_df, headers='keys', tablefmt='html', showindex=False)}
        </p>
        <h4>
        The most popular artists you listened to were:
        </h4>
        <p>
        {tabulate.tabulate(popular_artists_df, headers='keys', tablefmt='html', showindex=False)}
        </p>
        <h4>
        Your most played artists were:
        </h4>
        <p>
        {tabulate.tabulate(freq_played_artist_df, headers='keys', tablefmt='html', showindex=False)}
        </p>
        <h4>
        The artist you listened to with the most followers was:
        </h4>
        <p>
        {tabulate.tabulate(most_followed_artist_df, headers='keys', tablefmt='html', showindex=False)}
        </p>
        <h4>
        The most popular albums you listened to were:
        </h4>
        <p>
        {tabulate.tabulate(most_popular_albums_df, headers='keys', tablefmt='html', showindex=False)}
        </p>
        <h4>
        Your most played album was:
        </h4>
        <p>
        {tabulate.tabulate(most_freq_played_album_df, headers='keys', tablefmt='html', showindex=False)}
        </p>
    </body>
    </html>
    '''

    # Defining multipart message portions
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Attaching message portions to message multipart object
    msg.attach(part1)
    msg.attach(part2)

    # Email protocols and credentials
    mail = smtplib.SMTP('smtp.gmail.com', 587)
    mail.ehlo()
    mail.starttls()
    mail.login(email_username, email_pwd)
    mail.sendmail(sender, recipient, msg.as_string())
    mail.quit()