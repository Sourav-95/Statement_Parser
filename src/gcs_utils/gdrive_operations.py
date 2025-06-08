import os
from typing import List
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
DEL_SCOPES = [(GoogleAuthConstants.GDRIVE_DEL_SCOPE)]

# Get credential files
GCS_CRED = get_credential_path(base_var_name="CREDENTIAL_PATH")
GCS_UPLOAD_TOKEN = get_credential_path(base_var_name="GCS_UPLOAD_TOKEN_PATH")

def authenticate_upload():
    creds = None
    # The file token.json stores the user's access and refresh tokens.
    if os.path.exists(GCS_UPLOAD_TOKEN):
        creds = Credentials.from_authorized_user_file(GCS_UPLOAD_TOKEN, SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(GCS_CRED, SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(GCS_UPLOAD_TOKEN, 'w') as token:
            token.write(creds.to_json())
    return creds

def authenticate_for_delete():
    creds = None
    # The file token.json stores the user's access and refresh tokens.
    if os.path.exists(GCS_UPLOAD_TOKEN):
        creds = Credentials.from_authorized_user_file(GCS_UPLOAD_TOKEN, DEL_SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(GCS_CRED, DEL_SCOPES)
            creds = flow.run_local_server(port=0)
        # Save the credentials for the next run
        with open(GCS_UPLOAD_TOKEN, 'w') as token:
            token.write(creds.to_json())
    return creds

def upload_or_update_file_to_gdrive(data, gdrive_folder_id, new_file_name):
    """
    Uploads a pandas DataFrame as CSV to Google Drive folder.
    If a file with the same name exists and is NOT a binary file, updates it.
    If a binary file with the same name exists, skips upload.
    """
    creds = authenticate_upload()
    service = build('drive', 'v3', credentials=creds)

    # Save DataFrame locally as CSV
    data.to_csv(new_file_name, index=False)


    # Search for existing file with the same name in the folder, including mimeType
    query = f"name = '{os.path.basename(new_file_name)}' and '{gdrive_folder_id}' in parents and trashed = false"
    results = service.files().list(
        q=query,
        spaces='drive',
        fields='files(id, name, mimeType)',
        pageSize=1
    ).execute()
    files = results.get('files', [])

    media = MediaFileUpload(new_file_name)

    try:
        if files:
            existing_file = files[0]
            mime_type = existing_file['mimeType']
            file_id = existing_file['id']

            # List of common binary MIME types you want to skip updating
            binary_mime_types = [
                'application/pdf',
                'image/png',
                'image/jpeg',
                'application/octet-stream',
                'application/zip',
                # add more as needed
            ]

            if mime_type in binary_mime_types:
                logger.info(f"File '{existing_file['name']}' exists as binary type '{mime_type}'. Skipping upload/update.")
                return  # Skip upload/update for binary files

            # Otherwise update the file
            logger.info(f"File exists with ID: {file_id} and MIME type: {mime_type}. Updating file...")
            updated_file = service.files().update(
                fileId=file_id,
                media_body=media
            ).execute()
            logger.info(f"Updated file ID: {updated_file.get('id')}")
        else:
            # File does not exist, create new
            file_metadata = {
                'name': os.path.basename(new_file_name),
                'parents': [gdrive_folder_id]
            }
            new_file = service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            logger.info(f"Uploaded new file ID: {new_file.get('id')}")
    finally:
        if os.path.exists(new_file_name):
            os.remove(new_file_name)
            logger.debug(f"Deleted local file: {new_file_name}")

def delete_file_from_gdrive(file_name: str, folder_id: str):
    """
    Deletes a file from a specified Google Drive folder by its name.
    If multiple files with the same name exist in the folder, the first one found will be deleted.

    Args:
        file_name: The name of the file to delete (e.g., 'my_data.csv').
        folder_id: The ID of the Google Drive folder where the file is located.
    """
    # Placeholder for your authentication logic
    # Make sure 'creds' is properly initialized with your Google Drive API credentials
    # If you're not using a global 'authenticate()' function, you'll need to pass creds or build it here.

    creds = authenticate_for_delete()
    service = build('drive', 'v3', credentials=creds) # If you handle authentication outside, or use a service account key

    try:
        # Search for the file by name within the specified folder
        # 'trashed = false' ensures we only search for files not already in the trash
        query = f"name = '{file_name}' and '{folder_id}' in parents and trashed = false"
        
        results = service.files().list(
            q=query,
            spaces='drive',
            fields='files(id, name)', # Request only the ID and name
            pageSize=1 # We only need the first match
        ).execute()

        files = results.get('files', [])

        if not files:
            logger.warning(f"File '{file_name}' not found in folder ID '{folder_id}'. No file deleted.")
            return

        file_to_delete = files[0]
        file_id = file_to_delete['id']
        found_name = file_to_delete['name']

        logger.debug(f"Found file '{found_name}' with ID: {file_id} in folder ID '{folder_id}'. Attempting to delete...")

        # Delete the file using its ID
        service.files().delete(fileId=file_id).execute()
        logger.info(f"File '{found_name}' (ID: {file_id}) deleted successfully.")

    except Exception as e:
        logger.error(f"Error occured while Source File Deletion: {e}")