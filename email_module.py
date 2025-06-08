import os
import base64
import tempfile
from src.gcs_utils.gcs_connection import GoogleOAuth2Service
from src.components.logfactory import get_logger
from constants import ConstantRetriever
logger = get_logger(__name__)

_, mail_service = GoogleOAuth2Service.initialize_auth_service_built()

def download_latest_pdf_from_gmail(gmail_service):
    """Download the most recent PDF attachment from Gmail to a temp file."""
    results = gmail_service.users().messages().list(userId='me', 
                                                    q="has:attachment filename:pdf", 
                                                    maxResults=5
                                                    ).execute()
    messages = results.get('messages', [])

    if not messages:
        logger.warning("No PDF attachments found.")
        return None

    for msg in messages:
        msg_data = gmail_service.users().messages().get(userId='me', id=msg['id']).execute()
        for part in msg_data['payload'].get('parts', []):
            filename = part.get('filename')
            body = part.get('body', {})
            if filename and filename.lower().endswith('.pdf'):
                logger.info(f'Found PDF attachment: {filename}')
                attachment_id = body.get('attachmentId')
                logger.info(f'Attachment ID: {attachment_id}')
                if attachment_id:
                    attachment = gmail_service.users().messages().attachments().get(
                        userId='me', messageId=msg['id'], id=attachment_id
                    ).execute()


                    # file_data = base64.urlsafe_b64decode(attachment['data'].encode('UTF-8'))
                    # # temp_path = os.path.join(ConstantRetriever.SCRIPT_BASE, filename)
                    # temp_path = os.path.join(tempfile.gettempdir(), filename)
                    # temp_path = os.path.join(os.getcwd(),filename)
                    # os.mkdir(temp_path)
                    # with open(temp_path, 'wb') as f:
                    #     f.write(file_data)

                    # logger.info(f"📥 PDF saved to: {temp_path}")
                    # return temp_path

    logger.error("No downloadable PDF found.")
    return None

path = download_latest_pdf_from_gmail(mail_service)