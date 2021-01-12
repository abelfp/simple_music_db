import os
import glob

import pandas as pd
import psycopg2

import sql_queries as sq


def execute_sql(cursor, query, action_msg):
    "Executes query, if error occurs, it prints a message"
    try:
        cursor.execute(query)
    except psycopg2.Error as e:
        print(f"Error: {action_msg}")
        print(e)


def get_files(filepath):
    "Returns list of files under filepath"
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files:
            all_files.append(os.path.abspath(f))

    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    return all_files


def process_song_file(cur, conn, filepath):
    "Processes and loads data in filepath for songs and artists tables"
    # open song file
    song_list_df = []
    for f in get_files(filepath):
        song_list_df.append(pd.read_json(f, lines=True))
    df = pd.concat(song_list_df)

    # insert song record
    song_df = df[["song_id", "title", "artist_id", "year", "duration"]]
    song_df = song_df.drop_duplicates(subset=["song_id"])
    song_df.to_csv("/tmp/songs_dump.csv", index=False)
    execute_sql(cur, sq.song_table_copy, "Copying songs data")
    conn.commit()
    print('Processed songs data.')

    # insert artist record
    artist_df = df[['artist_id', 'artist_name', 'artist_location',
                    'artist_latitude', 'artist_longitude']]
    artist_df = artist_df.drop_duplicates(subset=["artist_id"])
    artist_df.to_csv("/tmp/artists_dump.csv", index=False)
    execute_sql(cur, sq.artist_table_copy, "Copying artists data")
    conn.commit()
    print('Processed artists data.')


def process_log_file(cur, conn, filepath):
    "Processes and loads data in filepath for users, time and songplays tables"
    log_list_df = []
    for f in get_files(filepath):
        log_list_df.append(pd.read_json(f, lines=True))
    df = pd.concat(log_list_df)

    # filter by NextSong action
    df = df[df["page"] == "NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit="ms")

    # copy time data records
    time_dict = {
        "start_time": t,
        "hour": t.dt.hour,
        "day": t.dt.day,
        "week": t.dt.week,
        "month": t.dt.month,
        "year": t.dt.year,
        "weekday": t.dt.weekday
    }
    time_df = pd.DataFrame(time_dict)
    time_df = time_df.drop_duplicates(subset=["start_time"])
    time_df.to_csv("/tmp/time_dump.csv", index=False)
    execute_sql(cur, sq.time_table_copy, "Copying time data")
    conn.commit()
    print('Processed time data.')

    # load user table
    user_df = df[["userId", "firstName", "lastName", "gender", "level"]].copy()
    # remove duplicates
    user_df['userId'] = user_df["userId"].astype(int)
    user_df = user_df.drop_duplicates(subset=["userId"])

    user_df.to_csv("/tmp/users_dump.csv", index=False)
    execute_sql(cur, sq.user_table_copy, "Copying users data")
    conn.commit()
    print('Processed users data.')

    # insert songplay records
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(sq.song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (pd.to_datetime(row.ts, unit="ms"), row.userId,
                         row.level, songid, artistid, row.sessionId,
                         row.location, row.userAgent)
        cur.execute(sq.songplay_table_insert, songplay_data)
    conn.commit()
    print('Processed songplay data.')


def main():
    "Executes complete ETL"
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    # process and load data
    process_song_file(cur, conn, filepath='data/song_data')
    process_log_file(cur, conn, filepath='data/log_data')

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
