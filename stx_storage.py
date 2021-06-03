from configparser import ConfigParser
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload
from google.oauth2.credentials import Credentials
import io
import logging
from oauth2client import GOOGLE_REVOKE_URI, GOOGLE_TOKEN_URI, client
import os
import sys
import traceback as tb

class GoogleDriveClient():
    def __init__(self, db_backup_folder_name, reports_folder_name):
        # Get all rights to list files, download a file, and upload files
        SCOPES = ['https://www.googleapis.com/auth/drive']
        # instantiate config parser
        config = ConfigParser()
        # parse existing configuration file
        cfg_file_path = os.path.abspath(os.path.join(os.getenv('HOME'),
                                                     'stx_cfg.ini'))
        config.read(cfg_file_path)
        # read client ID, client secret and refresh token from
        # Google Drive section
        g_drive_client_id = config.get('google_drive', 'client_id')
        g_drive_client_secret = config.get('google_drive', 'client_secret')
        g_drive_refresh_token = config.get('google_drive', 'refresh_token')
        logging.info(f'Read config from = {cfg_file_path}')
        """Get access token from client id/secret and refresh token """
        creds = client.OAuth2Credentials(
            access_token=None,
            client_id=g_drive_client_id,
            client_secret=g_drive_client_secret,
            refresh_token=g_drive_refresh_token,
            token_expiry=None,
            token_uri=GOOGLE_TOKEN_URI,
            user_agent=None,
            revoke_uri=GOOGLE_REVOKE_URI
        )
        logging.info('Got access token from client id/secret and refresh '
                     'token')
        """Connect to google drive """
        self.drive_service = build('drive', 'v3', credentials=creds)
        logging.info('Connected to Google Drive')
        # store the envelope folder and file name in class members
        self.db_backup_folder_name = db_backup_folder_name
        self.reports_folder_name = reports_folder_name

    """Get Google Drive IDs for the envelope csv folder and file"""
    def get_folder_id(self, folder_name):
        # Call Drive v3 API, first get the ID of EnvelopeData folder
        folder_data = self.drive_service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            q=f"name='{folder_name}'",
            fields="nextPageToken, files(id, name)"
        ).execute()
        drive_folders = folder_data.get('files', [])
        # Throw exception if could not find folder
        if not drive_folders:
            raise Exception(f'Could not find {folder_name} folder')
        # Get the ID of the Envelope folder
        folder_id = drive_folders[0].get('id')
        if folder_id is None:
            raise Exception(f'No folder ID for {folder_name} ')
        logging.debug(f'ID of {folder_name} folder is {folder_id}')
        return folder_id

    def upload_file(self, folder_id, file_path, file_name):
        media = MediaFileUpload(file_path)
        response = self.drive_service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            q=f"name='{file_name}' and parents='{folder_id}'",
            fields='nextPageToken, files(id, name)',
            pageToken=None
        ).execute()
        if len(response['files']) == 0:
            file_metadata = {
                'name': file_name,
                'parents': [folder_id]
            }
            file = self.drive_service.files().create(
                supportsAllDrives=True,
                body=file_metadata,
                media_body=media,
                fields='id'
            ).execute()
            logging.info(f"A new file was created {file.get('id')}")
        else:
            for file in response.get('files', []):
                # Process change
                update_file = self.drive_service.files().update(
                    supportsAllDrives=True,
                    fileId=file.get('id'),
                    media_body=media,
                ).execute()
                logging.info(f'Updated File {file}')


def main():
    # initialize logging
    logging.basicConfig(
        format='%(asctime)s %(levelname)s [%(filename)s:%(lineno)d] - '
        '%(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        level=logging.INFO
        # level=logging.DEBUG
    )

    # These two names are hard-coded.  We might set them up as
    # arguments later
    db_backup_folder_name = 'db_backups'
    reports_folder_name = 'reports'
    try:
        gdc = GoogleDriveClient(db_backup_folder_name, reports_folder_name)
        # Get the Google Drive IDs of the DB backup and reports folders
        db_folder_id = gdc.get_folder_id(db_backup_folder_name)
        report_folder_id = gdc.get_folder_id(reports_folder_name)
        logging.info(f'db_folder_id = {db_folder_id}; '
                     f'report_folder_id = {report_folder_id}')
        report_file_name = '2021-05-28_EOD.pdf'
        report_file_path = os.path.join(os.getenv('HOME'), 'market',
                                        report_file_name)
        gdc.upload_file(report_folder_id, report_file_path, report_file_name)
    except:
        tb.print_exc()


if __name__ == '__main__':
    main()
