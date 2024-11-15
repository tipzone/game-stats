import io
import json
import logging
import os
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# Set up logging with both console and file output
logging.basicConfig(
    level=logging.INFO,  # Set level to INFO for general messages
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),  # Print to console
        logging.FileHandler("app.log")  # Log to a file
    ]
)

# Load environment variables from .env file
load_dotenv()

# Retrieve PostgreSQL credentials
db_password = os.getenv("POSTGRES_PASSWORD")
db_user = os.getenv("POSTGRES_USER")
db_name = os.getenv("POSTGRES_DB")
db_host = os.getenv("POSTGRES_HOST")
db_port = os.getenv("POSTGRES_PORT")

# Use the credentials in your connection
import psycopg2
conn = psycopg2.connect(
    host=db_host,
    port=db_port,
    dbname=db_name,
    user=db_user,
    password=db_password
)


# Path to the service account credentials JSON file
SERVICE_ACCOUNT_FILE = '/app/credentials.json'

# Scopes for Google Drive API
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]



def insert_games(data, conn):
    """Insert JSON data into a PostgreSQL table for 'games'."""
    # Convert JSON data to a DataFrame
    df = pd.json_normalize(data['games']).astype(str)
    
    # Write DataFrame to PostgreSQL
    with conn.cursor() as cursor:
        # Truncate table before loading
        cursor.execute("TRUNCATE TABLE landing_zone.games")
        # Insert new data
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO landing_zone.games (
                    uuid, id, name, modificationdate, cooperative, highestWins, noPoints, usesTeams,
                    urlThumb, urlImage, bggName, bggYear, bggId, designers, isBaseGame, isExpansion, rating, minPlayerCount,
                    maxPlayerCount, minPlayTime, maxPlayTime, minAge, preferredImage, previouslyPlayedAmount, load_date
                ) 
                VALUES (%(uuid)s, %(id)s, %(name)s, %(modificationDate)s, %(cooperative)s, %(highestWins)s, %(noPoints)s, 
                    %(usesTeams)s, %(urlThumb)s, %(urlImage)s, %(bggName)s, %(bggYear)s, %(bggId)s, %(designers)s, 
                    %(isBaseGame)s, %(isExpansion)s, %(rating)s, %(minPlayerCount)s, %(maxPlayerCount)s, %(minPlayTime)s, 
                    %(maxPlayTime)s, %(minAge)s, %(preferredImage)s, %(previouslyPlayedAmount)s, CURRENT_TIMESTAMP)
                """,
                row.to_dict()  # Pass each row as a dictionary
            )
        conn.commit()

def insert_players(data, conn):
    """Insert JSON data into a PostgreSQL table for 'players'."""
    # Convert JSON data to a DataFrame
    df = pd.json_normalize(data['players']).astype(str)
    
    # Write DataFrame to PostgreSQL
    with conn.cursor() as cursor:
        # Truncate table before loading
        cursor.execute("TRUNCATE TABLE landing_zone.players")
        # Insert new data
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO landing_zone.players (uuid, id, name, isAnonymous, modificationDate, bggUsername, load_date) 
                VALUES (%(uuid)s, %(id)s, %(name)s, %(isAnonymous)s, %(modificationDate)s, %(bggUsername)s, CURRENT_TIMESTAMP)
                """,
                row.to_dict()  # Pass each row as a dictionary
            )
        conn.commit()


def insert_locations(data, conn):
    """Insert JSON data into a PostgreSQL table."""
    # Convert JSON data to a DataFrame (if it's structured)
    df = pd.json_normalize(data['locations'])
    
    # Write DataFrame to PostgreSQL
    with conn.cursor() as cursor:
        # Truncate table before loading
        cursor.execute("TRUNCATE TABLE landing_zone.locations")
        # Insert new data
        for _, row in df.iterrows():
            cursor.execute(
                """
                INSERT INTO landing_zone.locations (uuid, id, name, modificationDate, load_date) 
                VALUES (%(uuid)s, %(id)s, %(name)s, %(modificationDate)s, CURRENT_TIMESTAMP)
                """,
                row.to_dict()
            )
        conn.commit()

def insert_plays(data, conn):
    """Insert JSON data into a PostgreSQL table."""
    # Convert JSON data to a DataFrame (if it's structured)
    df_plays = pd.json_normalize(data['plays']).astype(str)

    # Player scores are nested within the plays json, so we need to prepare player scores as separate data
    player_scores_data = []
    for play in data['plays']:
        play_uuid = play.get("uuid")
        for player_score in play.get("playerScores", []):
            player_score['play_uuid'] = play_uuid  # Add play UUID for relational link
            player_scores_data.append(player_score)

    # Convert 'playerScores' to DataFrame
    df_player_scores = pd.DataFrame(player_scores_data).astype(str)
    
    # Write DataFrame to PostgreSQL
    with conn.cursor() as cursor:
        # Insert into plays table
        cursor.execute("TRUNCATE TABLE landing_zone.plays")
        for _, row in df_plays.iterrows():
            cursor.execute(
                """
                INSERT INTO landing_zone.plays (uuid, modificationDate, entryDate, playDate, usesTeams, durationMin,
                    ignored, manualWinner, rounds, bggId, importPlayId, locationRefId, gameRefId, rating, nemestatsId,
                    load_date) 
                VALUES (%(uuid)s, %(modificationDate)s, %(entryDate)s, %(playDate)s, %(usesTeams)s, %(durationMin)s,
                    %(ignored)s, %(manualWinner)s, %(rounds)s, %(bggId)s, %(importPlayId)s, %(locationRefId)s,
                    %(gameRefId)s, %(rating)s, %(nemestatsId)s, CURRENT_TIMESTAMP)
                """,
                row.to_dict()  # Convert row to a dictionary and pass it as arguments
            )

        # Insert into player_scores table
        cursor.execute("TRUNCATE TABLE landing_zone.player_scores")
        for _, row in df_player_scores.iterrows():
            cursor.execute(
                """
                INSERT INTO landing_zone.player_scores (play_uuid, score, winner, newPlayer, startPlayer, playerRefId, 
                    rank, seatOrder, load_date) 
                VALUES (%(play_uuid)s, %(score)s, %(winner)s, %(newPlayer)s, %(startPlayer)s, %(playerRefId)s,
                    %(rank)s, %(seatOrder)s, CURRENT_TIMESTAMP)
                """,
                row.to_dict()
            )

        conn.commit()


def main():
    """Authenticates using the service account and downloads a file from Google Drive."""
    creds = None
    # Use the service account credentials to authenticate
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=SCOPES)

    try:
        # Create Google Drive service object
        service = build("drive", "v3", credentials=creds, cache_discovery=False)

        # Call the Drive v3 API to list files
        results = service.files().list(
            q="name contains 'BGStatsExport' and fileExtension = 'json' and '1-HRwkuGUS1j1ZWBcgCb0e3wdTSwrJkxd' in parents",
            fields="nextPageToken, files(id, name, parents, fileExtension, createdTime)",
            pageSize=5
        ).execute()

        items = results.get("files", [])
        sorted_items = sorted(items, key=lambda x: x['createdTime'], reverse=True)
        first_item = sorted_items[0]

        if not first_item:
            logging.warning("No first file found.")
            return
    except Exception as error:
        logging.error(f"An error occurred: {error}")

    # Download file
    try:
        request = service.files().get_media(fileId=first_item['id'])
        file = io.BytesIO()
        downloader = MediaIoBaseDownload(file, request)
        done = False
        while not done:
            status, done = downloader.next_chunk()
            logging.info(f"Download {int(status.progress() * 100)}%.")
        file.seek(0)
        
        # Read JSON data directly from the file object
        data = json.load(file)
        
        # Insert JSON data into PostgreSQL
        try:
            insert_games(data, conn)
        except Exception as error:
            logging.error(f"An error occurred when inserting GAME data: {error}")
            conn.rollback()

        try:
            insert_players(data, conn)
        except Exception as error:
            logging.error(f"An error occurred when inserting PLAYER data: {error}")
            conn.rollback()

        try:
            insert_locations(data, conn)
        except Exception as error:
            logging.error(f"An error occurred when inserting LOCATION data: {error}")
            conn.rollback()

        try:
            insert_plays(data, conn)
        except Exception as error:
            logging.error(f"An error occurred when inserting PLAY data: {error}")
            conn.rollback()
        
        conn.close()
        logging.info(f"Inserted data from {first_item['name']} into PostgreSQL.")
        
    except Exception as error:
        logging.error(f"An error occurred: {error}")


if __name__ == "__main__":
    main()
