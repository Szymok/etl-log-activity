from csv import field_size_limit
from google.cloud import bigquery, storage
import os
from datetime import datetime

# Setting configs
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloud.json'

# Create a big query client
bigquery_client = bigquery.Client()

# Grabbing today's date
today_date = str(datetime.today().year) + '-' + str(
 datetime.today().month) + '-' + str(datetime.today().day)


# Create a dataset called test_dataset
def getOrCreate_dataset(dataset_name):
	'''
    Get dataset. If dataset does not exist, create one.
    
    Args:
        - dataset_name(String)
    Returns:
        - dataset
    '''

	print('Fetching Dataset...')

	try:
		# get and return dataset if exist
		dataset = bigquery_client.get_dataset(dataset_name)
		print('Done')
		print(dataset.self_link)
		return dataset

	except Exception as e:
		# If not, create and return dataset
		if e.code == 404:
			print('Dataset does not exist. Creating a new one.')
			bigquery_client.create_dataset(dataset_name)
			dataset = bigquery_client.get_dataset(dataset_name)
			print('Done')
			print(dataset.self_link)
			return dataset
		else:
			print(e)


def getOrCreate_table(dataset_name, table_name):
	'''
    Get table. If table does not exist, create one. If dataset does not exist, create one.
    Args:
        - dataset_name(String)
        - table_name(String)
    Returns:
        - table
    '''

	# Grab prerequisites for creating a table
	dataset = getOrCreate_dataset(dataset_name)
	project = dataset.project
	dataset = dataset.dataset_id
	table = project + '.' + dataset + '.' + table_name

	print('\nFetching Table...')

	try:
		# Get table if exists
		t = bigquery_client.get_table(table)
		print('Done')
		print(t.self_link)
	except Exception as e:

		# If not, create and get table
		if e.code == 404:
			print('Table does not exist. Creating a new one.')
			bigquery_client.create_table(table)
			t = bigquery_client.get_table(table)
			print(t.self_link)
	finally:
		return t


def load_to_bigQuery(dataset_name='log_activity',
                     table_name='log_table',
                     date_to_load=today_date):
	'''
    Load CSV file to BigQuery.
    Args:
        - date_to_load(String)
            - Default - today
    Returns:
        - None
    '''

	# Creating a storage client
	storage_client = storage.Client()

	# Grab bucket data for loading
	bucket_name = 'orders_etl_bar' + date_to_load
	bucket = storage_client.get_bucket('orders_etl_bar')
	blob = bucket.blob(blob_name=date_to_load)

	table = getOrCreate_table(dataset_name=dataset_name, table_name=table_name)

	job_config = bigquery.LoadJobConfig(schema=[
	 bigquery.SchemaField("Action", "STRING"),
	 bigquery.SchemaField("orderNumber", "INTEGER"),
	 bigquery.SchemaField("orderDate", "DATE"),
	 bigquery.SchemaField("requiredDate", "DATE"),
	 bigquery.SchemaField("shippedDate", "DATE"),
	 bigquery.SchemaField("status", "STRING"),
	 bigquery.SchemaField("comments", "STRING"),
	 bigquery.SchemaField("customerNumber", "INTEGER")
	],
	                                    field_delimiter='|',
	                                    source_format=bigquery.SourceFormat.CSV)

	uri = 'https://storage.cloud.google.com/orders_etl_bar/' + date_to_load
	table_id = table.project + '.' + table.dataset_id + '.' + table.table_id

	print('\nLoading log activity data...')
	load_job = bigquery_client.load_table_from_uri(uri,
	                                               table_id,
	                                               job_config=job_config)

	load_job.result()
	print("Done. Loaded {} rows.".format(load_job.output_rows))


load_to_bigQuery()
