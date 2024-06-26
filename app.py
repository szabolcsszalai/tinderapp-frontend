import os
import datetime
import pytz
import pyodbc
import logging
from azure.storage.blob import BlobClient, generate_blob_sas, BlobSasPermissions
from azure.core.credentials import AzureNamedKeyCredential
from flask import Flask, g, request, jsonify, render_template, Response
from flask_cors import CORS
from pyodbc import Connection, Row
from sqlalchemy.pool import QueuePool



##################################################################
# Constants

# Blob storage
BLOB_STORAGE_ACCOUNT_NAME = os.environ['AZURE_BLOB_ACCOUNT_NAME']
BLOB_STORAGE_ACCOUNT_KEY = os.environ['AZURE_BLOB_ACCOUNT_KEY']
BLOB_STORAGE_CREDENTIAL = AzureNamedKeyCredential(BLOB_STORAGE_ACCOUNT_NAME, BLOB_STORAGE_ACCOUNT_KEY)

# Database
DB_SERVER = os.environ['AZURE_SQL_SERVER']
DB_NAME = os.environ['AZURE_DB_NAME']
DB_USER = os.environ['AZURE_DB_USER']
DB_PASS = os.environ['AZURE_DB_PASS']

DB_DRIVER= '{ODBC Driver 18 for SQL Server}'

GMT = pytz.timezone("Etc/GMT")



##################################################################
# Blob storage utils

def create_sas_token_for_blob(blob_client: BlobClient, account_key: str, expiry_minutes: int) -> str:
    '''Create and return a read-only SAS token for a blob

    Args:
        - blob_client: The BlobClient object for the blob
        - account_key: Blob storage account key
        - expiry_minutes: SAS token expiry in minutes
    
    Returns:
        - The created SAS token
    '''
    start_time = datetime.datetime.now(GMT)  #  The blob storage uses the GMT timezone
    expiry_time = start_time + datetime.timedelta(minutes=expiry_minutes)

    sas_token = generate_blob_sas(
        account_name=blob_client.account_name,
        container_name=blob_client.container_name,
        blob_name=blob_client.blob_name,
        account_key=account_key,
        permission=BlobSasPermissions(read=True),
        expiry=expiry_time,
        start=start_time
    )

    return sas_token



##################################################################
# Database utils

# Maintain a connection pool for long running operation
pool = QueuePool(
    creator=lambda: pyodbc.connect('DRIVER='+DB_DRIVER+';SERVER='+DB_SERVER+',1433'+';DATABASE='+DB_NAME+';UID='+DB_USER+';PWD='+ DB_PASS),
    pool_size=10,
    max_overflow=20,
    timeout=30,
    recycle=1800
)


def get_db_connection() -> Connection:
    '''Return a DB connection from the connection pool

    Returns:
        - The DB connection stored in Flask's "g" object
    '''
    if 'db_conn' not in g:
        g.db_conn = pool.connect()
    return g.db_conn


def fetch_rows_from_images_table(offset: int, chunk_size: int, start_date: datetime.datetime, product: str) -> list[Row]:
    '''Fetch and return a chunk of rows from the IMAGES table

    Args:
        - offset: The current offset (which image to start with)
        - chunk_size: The chunk size (how many rows to return)
        - start_date: Date to start the listing from
        - product: Product name to filter rows by (a value of "Mind" will return all rows)
    
    Returns:
        - The list of rows
    '''
    db_conn = get_db_connection()
    cursor = db_conn.cursor()

    if product == 'Mind':
        # List all rows starting from "start_date" and return "chunk_size" number of rows starting from "offset"
        query = "SELECT * FROM [IMAGES] WHERE CreatedTime > (?) ORDER BY Id OFFSET (?) ROWS FETCH NEXT (?) ROWS ONLY"
        cursor.execute(query, start_date, offset, chunk_size)
    else:
        # List rows with annotations for "product" starting from "start_date" and return "chunk_size" number of rows starting from "offset"
        query = '''
        SELECT * FROM [IMAGES]
            WHERE CreatedTime > (?) AND Id IN (
                SELECT ImageId FROM [ANNOTATIONS]
                    WHERE LabelId = (
                        SELECT Id FROM [LABELS]
                            WHERE Name = (?)
                    ) AND Value = 1
            )
            ORDER BY Id OFFSET (?) ROWS
            FETCH NEXT (?) ROWS ONLY
        '''
        cursor.execute(query, start_date, product, offset, chunk_size)
    return cursor.fetchall()


def fetch_image_annotations(img_id: int, auto_label: int=0) -> list[Row]:
    '''Fetch and return annotations for an image

    Args:
        - img_id: The ID of the image
        - auto_label: Bit signaling wether to return auto or manually labeled rows (0-manual, 1-auto)
    
    Returns:
        - The list of rows
    '''
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    cursor.execute("SELECT * FROM [ANNOTATIONS] WHERE ImageId = (?) AND AutoLabel = (?) ORDER BY LabelId", img_id, auto_label)
    return cursor.fetchall()


def fetch_labels() -> list[Row]:
    '''Fetch and return the contents of the LABELS table

    Returns:
        - The list of rows
    '''
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    cursor.execute("SELECT Id, Name, Parent FROM [LABELS]")
    return cursor.fetchall()


def update_image_status(img_id: int, status: int) -> None:
    '''Update image status in the DB

    Args:
        - img_id: The ID of the image
        - status: The status to be set
    '''
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    cursor.execute("UPDATE [IMAGES] SET AnnotationStatusId = (?) WHERE Id = (?)", status, img_id)
    db_conn.commit()


def delete_annotations(img_id: int) -> None:
    '''Delete manual annotations from the DB, associated with an image

    Args:
        - img_id: The ID of the image
    '''
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    cursor.execute('DELETE FROM [ANNOTATIONS] WHERE ImageId = (?) AND AutoLabel = 0', img_id)
    db_conn.commit()


def add_annotations(img_id: int, labels: list[dict[str,int | str]]) -> None:
    '''Add manual annotations to the DB, associated with an image

    Args:
        - img_id: The ID of the image
        - labels: The list describing the labels' values
    '''
    db_conn = get_db_connection()
    cursor = db_conn.cursor()
    delete_annotations(img_id)  # Delete previous manual annotations (could also modify existing ones but this seems easier)
    cursor.executemany("INSERT INTO [ANNOTATIONS] (ImageId, LabelId, Value, AutoLabel) VALUES (?, ?, ?, ?)", [(img_id, label['id'], label['value'], 0) for label in labels])
    db_conn.commit()


def images_row_generator(start_date: datetime.datetime, product: str='Mind'):
    '''Generator that reads rows from the IMAGES table in chunks
    '''
    global offset, chunk_size
    while True:
        rows = fetch_rows_from_images_table(offset, chunk_size, start_date, product)
        if not rows:
            break
        for row in rows:
            offset += 1
            yield row



##################################################################
# Flask setup

app = Flask(__name__)
CORS(app)  # Enable CORS for all routes


@app.teardown_appcontext
def close_db_connection(exception: Exception | None) -> None:
    '''Close DB connection on teardown
    '''
    db_conn = g.pop('db_conn', None)
    if db_conn is not None:
        db_conn.close()


@app.route("/")
def date_select() -> str:
    '''Handle requests to the root endpoint

    Displays an HTML were start date and product type can be set for listing images
    '''
    global current_date

    # Get label the names of labels where the parent is "Termék", add "Mind" option
    product_labels = [label[1] for label in fetch_labels() if label[2]==3]
    product_labels.insert(0, 'Mind')
    return render_template('index.html', dropdown_options=product_labels, current_date = current_date)


@app.route('/submit', methods=['POST'])
def submit() -> str:
    '''Handle POST request to the "submit" endpoint

    Sets start date and product type for listing images, recreates the image generator
    '''
    global imgs_list, offset
    selected_date = datetime.datetime.strptime(str(request.form.get('selected_date')), '%Y-%m-%d')
    selected_option = str(request.form.get('selected_option'))
    offset = 0
    imgs_list = images_row_generator(selected_date, selected_option)
    app.logger.info(f'Selected Date: {selected_date}, Selected Option: {selected_option}')
    return f'Selected Date: {selected_date}, Selected Option: {selected_option}'


@app.route('/swipe', methods=['POST'])
def swipe() -> Response:
    '''Handle POST request to the "swipe" endpoint

    Update image status and annotations based on the swipe result
    '''
    data = request.get_json()
    app.logger.info(f'Swipe direction received: {data["direction"]} , ImageId: {data["imageId"]}')
    if data['direction'] == 'right':
        # If swiped right set the status to "accept" (status Id: 3)
        update_image_status(data['imageId'], 3)
    else:
        # If swiped left set the status to "reject" (status Id: 2)
        labels = data.get('labels')
        if labels:
            app.logger.info(f'labels: {labels}')
            update_image_status(data['imageId'], 2)
            add_annotations(data['imageId'], labels)
    return jsonify(success=True)


@app.route('/img', methods=['GET'])
def img() -> Response:
    '''Handle GET request to the "img" endpoint

    Send image data to the frontend
    '''
    global imgs_list, current_date

    # Defaults
    img_label = ''  # Image auto label to be displayed on the frontend
    labels = []  # List for storing labels and their values for this image's annotation
    status = 1  # Default annotation status (todo)
    try:
        # Get next row from DB
        img_id, img_name, img_url, img_creation_date, status  = imgs_list.__next__()

        # Update current date (displayed when setting start date on the backend's root endpoint)
        current_date = img_creation_date
        
        # Create SAS token-authenticated url for the image
        blob_client = BlobClient.from_blob_url(img_url, BLOB_STORAGE_CREDENTIAL)
        img_url = blob_client.url + '?' + create_sas_token_for_blob(blob_client, BLOB_STORAGE_ACCOUNT_KEY, 5)

        # Populate labels
        lab = fetch_labels()  # All possible labels
        labels = [{'id': label_id, 'value': value} for _, _, label_id, value, _ in fetch_image_annotations(img_id)]  # Label IDs and values in image's annotation
        labels = [d | {'name': [label_name for label_id, label_name, _ in lab if label_id==d['id']][0]} for d in labels]  # Append label names as well

        # Check for auto-labels in the ANNOTATIONS table and set img_label accordingly
        auto_labels = [label_id for _, _, label_id, value, _ in fetch_image_annotations(img_id, 1) if value == 1]
        auto_labels = [label_name for label_id, label_name, _ in lab if label_id in auto_labels]
        if auto_labels:
            img_label = ','.join(auto_labels)
        else:
            img_label = '???'
    except StopIteration:
        # Use the default image if there are no more images
        img_id = 'default_img/default.png'  #TODO: Make default ID an int
        img_url = ''

    return jsonify(
        {
            'imageId': img_id,
            'imageData': img_url,
            'imageLabel': img_label,
            'labels': labels,
            'status': status
        }
    )


@app.route('/select/<parent>', methods=['GET'])
def select(parent):
    '''Handle GET request to the "select" endpoint

    Send button data to the frontend
    '''
    buttons = [{"id": e[0], "text": e[1], "parent": e[2]} for e in fetch_labels()]
    app.logger.info(f'Got request for parent {parent}')
    return jsonify([b for b in buttons if b['parent']==int(parent)])


if __name__=='__main__':
    # Initialize globals
    offset = 0
    chunk_size = 2
    current_date = datetime.datetime(year=2024, month=1,day=1)

    # Setup log level
    app.logger.setLevel(logging.INFO)

    # Create images generator
    imgs_list = images_row_generator(current_date)

    # Start backend
    app.run(host='0.0.0.0', port=6969)