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
    def get_envelope_folder_file_id(self):
        # Call Drive v3 API, first get the ID of EnvelopeData folder
        envelope_data = self.drive_service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            q=f"name='{self.envelope_folder_name}'",
            fields="nextPageToken, files(id, name)"
        ).execute()
        envelope_folders = envelope_data.get('files', [])
        # Throw exception if could not find EnvelopeData folder
        if not envelope_folders:
            raise Exception(f'Could not find {self.envelope_folder_name} '
                            'folder')
        # Get the ID of the Envelope folder
        envelope_folder_id = envelope_folders[0].get('id')
        if envelope_folder_id is None:
            raise Exception(f'Could not retrieve {self.envelope_folder_name} '
                            'folder ID')
        logging.debug(f'The ID of the {self.envelope_folder_name} folder '
                      f'is {envelope_folder_id}')
        # Get the ID of the Envelope csv file
        env_csv_query = f"'{envelope_folder_id}' in parents and "\
            f"name='{self.envelope_file_name}'"
        envelope_csv = self.drive_service.files().list(
            supportsAllDrives=True,
            includeItemsFromAllDrives=True,
            q=env_csv_query,
            fields="nextPageToken, files(id, name)"
        ).execute()
        envelope_csv_id = None
        envelope_csv_files = envelope_csv.get('files', [])
        if envelope_csv_files:
            envelope_csv_id = envelope_csv_files[0].get('id')
        logging.debug(f'The ID of {self.envelope_file_name} is '
                      f'{envelope_csv_id}')
        return envelope_folder_id, envelope_csv_id

    def download_envelope_file(self, file_id):
        # If there is no ID for the envelope CSV file, do not
        # download; need to create new file
        if file_id is None:
            return None
        request = self.drive_service.files().get_media(fileId=file_id)
        # TODO: add current timestamp to download path
        download_path = os.path.abspath(os.path.join(
            os.sep, 'var', 'www', 'html', 'download', self.envelope_file_name))
        fh = io.FileIO(download_path, 'w')
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            logging.debug(f"Download {int(status.progress()) * 100}%.")
        return download_path

    def upload_envelope_file(self, folder_id, file_path, file_name):
        if file_path is None:
            logging.info(f'No changes made to {file_name}, no upload needed')
            return
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
        folder_id, file_id = egd.get_envelope_folder_file_id()
        downloaded_file_path = egd.download_envelope_file(file_id)
        updated_file_path = egd.update_envelope_file(downloaded_file_path)
        egd.upload_envelope_file(folder_id, updated_file_path,
                                 envelope_file_name)
    except:
        tb.print_exc()


if __name__ == '__main__':
    main()
