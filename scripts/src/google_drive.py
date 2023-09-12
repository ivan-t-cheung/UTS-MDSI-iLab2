def create_gdrive_client(cred_file=r'../auth/gdrive_credentials.txt'):
    """Authenticates for app and user access using PyDrive2 and creates a Google Drive client.

    Args:
        cred_file (str): path to file for user access authentification. If it does not exist a file will be created.
    Returns:
        Google Drive client.
    
    This function requires a "client_secrets.json" file to be stored in the working folder for app authentification.
    Download the file from https://console.cloud.google.com/apis/credentials.
    """

    ### Import libraries ###
    import os
    from pydrive2.auth import GoogleAuth
    from pydrive2.drive import GoogleDrive

    ### Authenticate ###
    gauth = GoogleAuth()
    if os.path.isfile(cred_file):
        # try to load saved client credentials from file
        gauth.LoadCredentialsFile(cred_file)
    if (gauth.credentials is None) or (not os.path.isfile(cred_file)):
        # authenticate if they're not there
        gauth.LocalWebserverAuth()
    elif gauth.access_token_expired:
        # refresh them if expired
        gauth.Refresh()
    else:
        # initialize the saved creds
        gauth.Authorize()
    # save the current credentals to a file
    gauth.SaveCredentialsFile(cred_file)

    ### Create client ###
    return GoogleDrive(gauth)

def upload_file(drive_client, folder_id, file_path):
    """Uploads a local file into a Google Drive folder.

    Args:
        drive_client: Google Drive client from create_gdrive_client().
        folder_id (str): to get folder ID use "Share -> Copy link" in Google Drive and copy ID after "folders/".
        file_path (str): path to local file to upload.
    """    
    
    # get folder
    f = drive_client.CreateFile({"parents": [{"kind": "drive#fileLink", "id": folder_id}]})
    # upload file to folder
    f.SetContentFile(file_path)
    f.Upload()
    return