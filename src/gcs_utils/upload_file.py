import os
from typing import List # This import is not strictly needed anymore but kept for consistency
from src.components.env_cred_loader import get_credential_path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.auth.transport.requests import Request
from constants import GoogleAuthConstants
from src.components.logfactory import get_logger
from dotenv import load_dotenv
load_dotenv()

logger = get_logger(__name__)


# If modifying these SCOPES, delete the token.json file.
SCOPES = [(GoogleAuthConstants.GDRIVE_UPLOAD_SCOPE)]

# Get credential files
GCS_CRED = get_credential_path(base_var_name="CREDENTIAL_PATH")
GCS_UPLOAD_TOKEN = get_credential_path(base_var_name="GCS_UPLOAD_TOKEN_PATH")

def authenticate():
    """
    Authenticates with Google Drive API using stored credentials or by initiating
    an OAuth 2.0 flow.
    """
    creds = None
    if os.path.exists(GCS_UPLOAD_TOKEN):
        creds = Credentials.from_authorized_user_file(GCS_UPLOAD_TOKEN, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except Exception as e:
                logger.warning(f"Token refresh failed: {e}. Re-authenticating...")
                flow = InstalledAppFlow.from_client_secrets_file(GCS_CRED, SCOPES)
                creds = flow.run_local_server(port=0)
        else:
            flow = InstalledAppFlow.from_client_secrets_file(GCS_CRED, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save refreshed or new token
        with open(GCS_UPLOAD_TOKEN, 'w') as token:
            token.write(creds.to_json())

    return creds


def move_file_to_gdrive_folder(file_path: str, gdrive_folder_id: str):
    """
    Moves a local file to a specified Google Drive folder.
    If a file with the same name exists in the target folder, it updates that file.
    Otherwise, it uploads the file as a new file in the target folder.

    Args:
        file_path (str): The full path to the local file to be moved/uploaded.
        gdrive_folder_id (str): The ID of the Google Drive folder where the file
                                should be moved.
    """
    creds = authenticate()
    service = build('drive', 'v3', credentials=creds)

    # Get the base name of the file (e.g., "my_document.pdf" from "/path/to/my_document.pdf")
    file_name = os.path.basename(file_path)

    # Prepare the media for upload
    media = MediaFileUpload(file_path)

    try:
        # Search for an existing file with the same name in the target folder
        query = f"name = '{file_name}' and '{gdrive_folder_id}' in parents and trashed = false"
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name, mimeType)',
            pageSize=1
        ).execute()
        files = results.get('files', [])

        if files:
            # If a file with the same name exists, update it
            existing_file = files[0]
            file_id = existing_file['id']
            logger.info(f"File '{file_name}' already exists in folder (ID: {file_id}). Updating content...")

            # Update the existing file's content
            updated_file = service.files().update(
                fileId=file_id,
                media_body=media,
                # Ensure the file remains in the same folder if it was already there
                # and you are just updating its content. If you want to change its parent,
                # you would modify 'addParents' and 'removeParents' parameters.
                # For this "move" operation, we assume it's either new or updating in place.
            ).execute()
            logger.info(f"Updated file ID: {updated_file.get('id')}")
        else:
            # If no file with the same name exists, create a new one in the specified folder
            logger.info(f"File '{file_name}' does not exist in folder. Uploading as new file...")
            file_metadata = {
                'name': file_name,
                'parents': [gdrive_folder_id] # Specify the target folder ID
            }
            new_file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            logger.info(f"Uploaded new file ID: {new_file.get('id')}")
    except Exception as e:
        logger.error(f"An error occurred while moving/uploading file '{file_name}': {e}")
        # Depending on requirements, you might want to re-raise the exception or handle it differently.


move_file_to_gdrive_folder(file_path='axis_06_15.pdf', gdrive_folder_id='1EG6dzy2uKExg6D8xDqjz0Ddoa4nsclOo')