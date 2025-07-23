from googleapiclient.http import MediaIoBaseDownload
import io
from src.components.logfactory import get_logger

logger = get_logger(__name__)

class GoogleDriveFolderManager:
    """
    This class is to manage Google Drive folder operations such as listing and downloading files.
    Initializes the GoogleDriveFolderManager with an authenticated Google Drive service.
        Class Args:   service (googleapiclient.discovery.Resource): Authenticated Google Drive API service.

    Methods : (1) `list_files(folder_id, page_size=10)`
             -------------------------------------------------------------------
                Lists all files in a specified Google Drive folder, handling pagination to retrieve all files.
                Args:
                    folder_id (str): The ID of the Google Drive folder to list files from.
                    page_size (int, optional): Number of files to retrieve per API call (max 1000). Default is 10.
                Returns:
                    list of dict: A list of file metadata dictionaries, each containing 'id' and 'name' keys.
             (2) download_pdf_file(file_id, destination_path)
             -------------------------------------------------------------------
                    Downloads a file from Google Drive by its file ID to a local destination path.
                Args:
                    file_id (str): The ID of the file to download.
                    destination_path (str): The local file path where the downloaded file will be saved.
    """

    def __init__(self, service):
        self.service = service

    def list_files(self, folder_id, page_size=10):
        files = []
        page_token = None

        while True:
            response = self.service.files().list(
                q=f"'{folder_id}' in parents and mimeType != 'application/vnd.google-apps.folder' and trashed = false",
                pageSize=page_size,
                fields="nextPageToken, files(id, name, mimeType)",
                pageToken=page_token
            ).execute()

            batch_files = response.get('files', [])
            files.extend(batch_files)

            if not batch_files:
                logger.warning(f'No files found in the folder : `{folder_id}`')
                break

            logger.info(f'Scanning Folder ID: `{folder_id}`')
            page_token = response.get('nextPageToken')
            if not page_token:
                break

        logger.info(f"Total files in FolderID `{folder_id}` :- {len(files)}\n")
        return files


    def file_downloader(self, file_id, destination_path):
        # First, get the file metadata to check MIME type

        file_metadata = self.service.files().get(fileId=file_id, fields='mimeType, fileExtension, name, size, md5Checksum').execute()
        
        mime_type = file_metadata.get('mimeType')
        file_name = file_metadata.get('name')
        ext = file_metadata.get('fileExtension')

        # Define MIME types for Google Docs Editors files and their export formats
        google_docs_mime_types = {
            'application/vnd.google-apps.document': 'application/pdf',  # Google Docs -> PDF
            'application/vnd.google-apps.spreadsheet': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',  # Sheets -> XLSX
            'application/vnd.google-apps.presentation': 'application/pdf',  # Slides -> PDF
            # Add more if needed
        }

        try:
            if mime_type in google_docs_mime_types:
                export_mime_type = google_docs_mime_types[mime_type]
                request = self.service.files().export_media(fileId=file_id, mimeType=export_mime_type)
            else:
                request = self.service.files().get_media(fileId=file_id)
        except Exception as e:
            logger.error(f"Error creating request for file ID {file_id}: {e}")
            pass

        # Open local file for writing in binary mode
        fh = io.FileIO(destination_path, 'wb')

        # Create a downloader object to download the file in chunks
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()

        return file_metadata
