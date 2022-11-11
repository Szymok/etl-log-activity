import pymysql
import configparser

# Creating a parser object and reading from the config file
parser = configparser.ConfigParser()
parser.read('pipeline.conf')

# Grabing the values from the config file
hostname = parser.get('mysql_config', 'hostname')
port = parser.get('mysql_config', 'port')
username = parser.get('mysql_config', 'username')
password = parser.get('mysql_config', 'password')
dbname = parser.get('mysql_config', 'database')

# Connecting
print('Connecting to MySQL')
conn = pymysql.connect(host=hostname,
                       user=username,
                       password=password,
                       db=dbname,
                       port=int(port))

cursor = conn.cursor()

print('Simulating orders..')
cursor.callproc(procname='simulate_orders')
cursor.close()
conn.close()
print('Done')