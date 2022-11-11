from google.cloud import storage
import os
from datetime import datetime

# Creating an Environmental Variable for the service key configuration
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloud.json'

# Creating a storage client
storage_client = storage.Client()

today_date = str(datetime.today().year) + '-' +str(datetime.today().month) + '-' +str(datetime.today().day)

try:
    bucket = storage_client.get_bucket('orders_etl_bar')
    
except Exception:
    print('Bucket Does not Exist. Creating a new one.')
    storage_client.create_bucket('orders_etl_bar')
    bucket = storage_client.get_bucket('orders_etl_bar')
finally:
    blob = bucket.blob(blob_name=today_date)
    blob.upload_from_filename('binlog_orders.csv')

bucket = storage_client.get_bucket('orders_etl_bar')
print(bucket.self_link)
