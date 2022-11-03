from csv import field_size_lim
import os
from google.cloud import bigquery, storage
from datetime import datetime

# Setting config
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'ServiceKey_GoogleCloud.json'

# A Big Query Client
bigquery_client = bigquery.Client()

# Today's date
today_date = str(datetime.today().year) + '-' + str(datetime.today().month) + '-' + str(datetime.today().day)

# Create a dataset
def get_or_create_dataset(dataset_name):
	
	'''
	Get dataset. If dataset does not exist, create it
 	
	Args:
 		- dataset_name(String)
	Returns:
 		- dataset
 	'''

	print('Fetching dataset')

	try:
		# get and return dataset if exist
		dataset = bigquery_client.get_dataset(dataset_name)
		print('Done')
		print(dataset.self_link)
		return dataset

	except Exception as e:
		# If not, create and return dataset
		if e.code == 404:
			print('Dataset does not exist. Creating a new one')
			bigquery_client.create_dataset(dataset_name)
			dataset = bigquery_client.get_dataset(dataset_name)
			print('Done')
			print(dataset.self_link)
			return dataset
		else:
			print(e)

def get_or_create_table(dataset_name, table_name):

	'''
 	Get table. If table does not exist, create one. If dataset doen not exist, create one.

	Args:
  	- dataset_name(String)
	 	- table_name(String)
	Returns:
 		- table
 	'''

	