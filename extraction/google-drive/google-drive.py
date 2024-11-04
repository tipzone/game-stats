import io
import os.path

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseDownload

# If modifying these scopes, delete the file token.json.
SCOPES = ["https://www.googleapis.com/auth/drive.readonly"]


def main():
  """Shows basic usage of the Drive v3 API.
  Prints the names and ids of the first 10 files the user has access to.
  """
  creds = None
  # The file token.json stores the user's access and refresh tokens, and is
  # created automatically when the authorization flow completes for the first
  # time.
  if os.path.exists("token.json"):
    creds = Credentials.from_authorized_user_file("token.json", SCOPES)
  # If there are no (valid) credentials available, let the user log in.
  if not creds or not creds.valid:
    if creds and creds.expired and creds.refresh_token:
      creds.refresh(Request())
    else:
      flow = InstalledAppFlow.from_client_secrets_file(
          "credentials.json", SCOPES
      )
      creds = flow.run_local_server(port=0)
    # Save the credentials for the next run
    with open("token.json", "w") as token:
      token.write(creds.to_json())

  try:
    # Create Google Drive service object
    service = build("drive", "v3", credentials=creds)

    # Call the Drive v3 API
    results = (
        # RESOURCE OBJECTs are logical groups containing methods within the API.
        # e.g. if the API is Wal-Mart, this is the one section like tech or toys
        service.files() # This is the files RESOURCE OBJECT
        
        # Call methods within RESOURCE OBJECT
        # A method within a RESOURCE OBJECT returns a REQUEST OBJECT (a configured request ready to go but not yet sent)
        # You can only use one method when creating a request object.
        .list(
            q="name contains 'BGStatsExport' and fileExtension = 'json' and '1-HRwkuGUS1j1ZWBcgCb0e3wdTSwrJkxd' in parents",
            fields="nextPageToken, files(id, name, parents, fileExtension, createdTime)",
            pageSize=5
            )

        # Send the REQUEST OBJECT to the API
        .execute()
    )

    items = results.get("files", [])
    sorted_items = sorted(items, key=lambda x: x['createdTime'], reverse=True, )
    first_item = sorted_items[0]

    if not first_item:
      print("No first file found.")
      return
  except HttpError as error:
    # TODO(developer) - Handle errors from drive API.
    print(f"An error occurred: {error}")

  # Download file
  try:
    first_item_id = first_item['id']
    first_item_name = first_item['name']
    
    # create drive api client
    request = service.files().get_media(fileId=first_item_id)
    file = io.BytesIO() # Create BytesIO object to hold downloaded content in memory.
    downloader = MediaIoBaseDownload(file, request)
    done = False
    while done is False:
      status, done = downloader.next_chunk()
      print(f"Download {int(status.progress() * 100)}.")

    # Write the file to disk
    file.seek(0)  # Move to the beginning of the BytesIO buffer
    with open(first_item_name, "wb") as f:  # Open the file in write-binary mode
      f.write(file.read())  # Write the content to a file
      print(f"Downloaded {first_item_name}.")

  except HttpError as error:
    print(f"An error occurred: {error}")
    file = None



if __name__ == "__main__":
  main()