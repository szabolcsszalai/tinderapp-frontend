FROM python:3.11
WORKDIR /app/swipetag_backend

# Install python packages and DB driver
RUN pip install --upgrade pip && pip install flask flask-cors pytz azure-storage-blob sqlalchemy
RUN apt-get update && apt-get upgrade && apt-get install -y unixodbc
COPY . /app/swipetag_backend
RUN ./sql_driver_install.sh
RUN pip install pyodbc

# Setup environment variables
ENV AZURE_BLOB_ACCOUNT_NAME=""
ENV AZURE_BLOB_ACCOUNT_KEY=""
ENV AZURE_SQL_SERVER=""
ENV AZURE_DB_NAME=""
ENV AZURE_DB_USER=""
ENV AZURE_DB_PASS=""

# Run the backend app
ENTRYPOINT python app.py
