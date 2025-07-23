import os
import pickle
import platform
from dotenv import load_dotenv
from src.components.env_cred_loader import get_credential_path
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
import gspread
# from gspread.auth import AuthorizedUserCredentials
from src.components.logfactory import get_logger
from constants import ConstantRetriever, GoogleAuthConstants
import logging

# Set the logging level of googleapiclient.discovery (and related libraries) to WARNING or ERROR
logging.getLogger("googleapiclient.discovery").setLevel(logging.WARNING)
logging.getLogger("googleapiclient").setLevel(logging.WARNING)
logging.getLogger("google.auth").setLevel(logging.WARNING)
logging.getLogger("google_auth_httplib2").setLevel(logging.WARNING)
logging.getLogger("google.api_core").setLevel(logging.WARNING)
logging.getLogger("google.cloud").setLevel(logging.WARNING)
logging.getLogger("httplib2").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)

logger = get_logger(__name__)

load_dotenv()

class GoogleAuthenticator:

    def __init__(self, scopes, token_file='token.pickle'):
        self.scopes = scopes
        self.base_dir = ConstantRetriever.SCRIPT_BASE
        self.token_file = os.path.join(self.base_dir, 'auth_connection', token_file)
    
    # def get_credential_file(self, credential_file='credentials.json'): 
    #     credential_file = os.path.join(self.base_dir, 'auth_connection', credential_file)
    #     if not os.path.exists(credential_file):
    #         raise FileNotFoundError(f"Credentials file not found at {credential_file}")
    #     else:
    #         logger.debug(f"Credential File Retrieved")
    #         return credential_file

    def get_token(self):
        tokenizer = None
        try: 
            with open(self.token_file, 'rb') as token:
                tokenizer = pickle.load(token)
        except Exception as e:
            logger.error(f"Error loading token file: {e}")
                
        return tokenizer
    
    def save_token(self, creds):
        try:
            with open(self.token_file, 'wb') as token:
                pickle.dump(creds, token)
        except Exception as e:
            logger.error(f"Error saving token file: {e}")

    def refresh_token(self, creds):
        try:
            creds.refresh(Request())
            self.save_token(creds)
            logger.debug("Credentials refreshed successfully.")
            return creds
        except Exception as e:
            logger.error(f"Failed to refresh credentials: {e}")
            return None
        
    @staticmethod
    def build_service(service_name, version, creds):
    
        logger.debug(f"Building service : `{service_name.upper()}` with version : {version}")
        try:
            service = build(service_name, version, credentials=creds)
            logger.debug(f"Service `{service_name.upper()}` built successfully.")
        except Exception as e: 
            logger.error(f"Error building `{service_name.upper()}` service: {e}")
            raise

        return service

    def authenticate(self):
        creds = None
        try:
            # Check if token.pickle file exists then retrieve the token
            if os.path.exists(self.token_file):
                logger.debug("Token file found, loading credentials.")
                creds = self.get_token()

            # Call refresh_token only if creds exist, are expired, and have a refresh token
            if creds and hasattr(creds, 'expired') and creds.expired and hasattr(creds, 'refresh_token'):
                logger.debug("Token Expired...Refreshing expired credentials....")
                creds = self.refresh_token(creds)

            # If there are no (valid) credentials available, let the user log in
            if not creds or not hasattr(creds, 'valid') or not creds.valid:
                logger.info("No tokens found, initiating authentication flow.")

                # Gets cred file from the environment with `base_var_name` match in .env
                credential_file = get_credential_path(base_var_name="CREDENTIAL_PATH")
                flow = InstalledAppFlow.from_client_secrets_file(credential_file, self.scopes)
                creds = flow.run_local_server(port=0)
                if creds:
                    logger.info(f"[AUTHENTICATE] Scopes in token: {getattr(creds, 'scopes', 'unknown')}")
                self.save_token(creds)
        except Exception as e:
            logger.error(f"Google API Authentication Failed!!: {e}")
            raise
        return creds


class GoogleOAuth2Service:

    @classmethod
    def initialize_auth_service_built(cls):
        try:
            all_scopes = [GoogleAuthConstants.GMAIL_SCOPES, 
                          GoogleAuthConstants.GDRIVE_SCOPES, 
                          GoogleAuthConstants.GSHEET_SCOPES
                          ]
            authenticator = GoogleAuthenticator(scopes=all_scopes)
            
            creds = authenticator.authenticate()
            logger.debug(f'Scopes passed to Service Account Authentication Method: \n{creds.scopes}')
            logger.info("Google API Connection authenticated successfully.")
        
            drive_service = authenticator.build_service(service_name=GoogleAuthConstants.GDRIVE_SERVICE_NAME,
                                                             version=GoogleAuthConstants.GDRIVE_VERSION, creds=creds
                                                             )
            gmail_service = authenticator.build_service(service_name=GoogleAuthConstants.GMAIL_SERVICE_NAME,
                                                             version=GoogleAuthConstants.GMAIL_VERSION, creds=creds
                                                             )
            gspread_client = gspread.authorize(creds)
            
            logger.info("Google API services built successfully.\n")
            return drive_service, gmail_service, gspread_client
        except Exception as e:
            logger.error(f"Failed to authenticate Google API service: {e}\n")
            return None, None, None
    
    
        
        