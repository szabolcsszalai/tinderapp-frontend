# SwipeTag Backend
Python Flask backend for the SwipeTag annotator app

## How to use
### Build the Docker image:

```bash
docker build . -t IMAGE_NAME
```

### Run container:
```bash
docker run --rm -e "AZURE_BLOB_ACCOUNT_NAME=NAME_OF_AZURE_BLOB_STORAGE_ACCOUNT" \
-e "AZURE_BLOB_ACCOUNT_KEY=ACCOUNT_KEY_OF_AZURE_BLOB_STORAGE" \
-e "AZURE_SQL_SERVER=ADDRESS_OF_AZURE_SQL_SERVER" \
-e "AZURE_DB_NAME=NAME_OF_THE_DATABASE" \
-e "AZURE_DB_USER=USER_NAME_FOR_THE_DATABASE" \
-e "AZURE_DB_PASS=PASSWORD_FOR_THE_DATABASE" \
--network=host IMAGE_NAME
```

 - NAME_OF_AZURE_BLOB_STORAGE_ACCOUNT should be the name of the blob storage account.
 - ACCOUNT_KEY_OF_AZURE_BLOB_STORAGE should be the account key for the blob storage.
 - ADDRESS_OF_AZURE_SQL_SERVER should specify the Azure SQL server address (e.g., mydatabase.database.windows.net).
 - NAME_OF_THE_DATABASE should be the name of the dataset.
 - USER_NAME_FOR_THE_DATABASE should be the user name used to access the database.
 - PASSWORD_FOR_THE_DATABASE should be the password for the database for username USER_NAME_FOR_THE_DATABASE.