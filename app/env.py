import os

user = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
host = os.getenv("DB_HOST")
port = os.getenv("DB_PORT")
dbname = os.getenv("DB_NAME")

DATABASE_URL = f"mysql+pymysql://{user}:{password}@{host}:{port}/{dbname}"

REDIS_URL = os.getenv("REDIS_URL")

SECRET_KEY = os.getenv("SECRET_KEY")
ALGORITHM = os.getenv("ALGORITHM")

ELASTICSEARCH_URL = f'https://{host}:9200'
ELASTIC_USER = os.getenv("ES_USER")
ELASTIC_PASSWORD = os.getenv("ES_PASSWORD")
INDEX = os.getenv("ES_INDEX")
ES_CA_CERTS = os.getenv("ELASTIC_CA_CERT_PATH")