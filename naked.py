import requests
import json
import datetime
import time
import logging
import logging.config
import yaml
import mysql.connector

from datetime import datetime
from configparser import ConfigParser
from mysql.connector import Error

# Loading logging configuration
with open('./log_naked.yaml', 'r') as stream:
	log_config = yaml.safe_load(stream)

logging.config.dictConfig(log_config)

# Creating logger
logger = logging.getLogger('root')

logger.info('Asteroid processing service')

# Initiating and reading config values
logger.info('Loading configuration from file')

try:
	config = ConfigParser()
	config.read('config.ini')
	# Initiating and reading nasa API key and url from config.ini file
	nasa_api_key = config.get('nasa', 'api_key')
	nasa_api_url = config.get('nasa', 'api_url')

	# Initiating and reading DB access info from config.ini file
	mysql_config_mysql_host = config.get('mysql_config', 'mysql_host')
	mysql_config_mysql_db = config.get('mysql_config', 'mysql_db')
	mysql_config_mysql_user = config.get('mysql_config', 'mysql_user')
	mysql_config_mysql_pass = config.get('mysql_config', 'mysql_pass')
except:
	logger.exception('')
logger.info('DONE')

# Defines and creates connection to DB
def init_db():
	global connection
	connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)

# Defines and creates cursor
def get_cursor():
	global connection
	try:
		connection.ping(reconnect=True, attempts=1, delay=0)
		connection.commit()
	except mysql.connector.Error as err:
		logger.error("No connection to db " + str(err))
		connection = init_db()
		connection.commit()
	return connection.cursor()

# Checks if asteroid exists in db
def mysql_check_if_ast_exists_in_db(request_day, ast_id):
	records = []
	cursor = get_cursor()
	try:
		cursor = connection.cursor()
		result  = cursor.execute("SELECT count(*) FROM ast_daily WHERE `create_date` = '" + str(request_day) + "' AND `ast_id` = '" + str(ast_id) + "'")
		records = cursor.fetchall()
		connection.commit()
	except Error as e :
		logger.error("SELECT count(*) FROM ast_daily WHERE `create_date` = '" + str(request_day) + "' AND `ast_id` = '" + str(ast_id) + "'")
		logger.error('Problem checking if asteroid exists: ' + str(e))
		pass
	return records[0][0]

# Inserts asteroid values into ast_daily table in DB
def mysql_insert_ast_into_db(create_date, hazardous, name, url, diam_min, diam_max, ts, dt_utc, dt_local, speed, distance, ast_id, ast_mgn):
	cursor = get_cursor()
	try:
		cursor = connection.cursor()
		result  = cursor.execute( "INSERT INTO `ast_daily` (`create_date`, `hazardous`, `name`, `url`, `diam_min`, `diam_max`, `ts`, `dt_utc`, `dt_local`, `speed`, `distance`, `ast_id`, `ast_mgn`) VALUES ('" + str(create_date) + "', '" + str(hazardous) + "', '" + str(name) + "', '" + str(url) + "', '" + str(diam_min) + "', '" + str(diam_max) + "', '" + str(ts) + "', '" + str(dt_utc) + "', '" + str(dt_local) + "', '" + str(speed) + "', '" + str(distance) + "', '" + str(ast_id) + "', '" + str(ast_mgn) + "')")
		connection.commit()
	except Error as e :
		logger.error( "INSERT INTO `ast_daily` (`create_date`, `hazardous`, `name`, `url`, `diam_min`, `diam_max`, `ts`, `dt_utc`, `dt_local`, `speed`, `distance`, `ast_id`, `ast_mgn`) VALUES ('" + str(create_date) + "', '" + str(hazardous) + "', '" + str(name) + "', '" + str(url) + "', '" + str(diam_min) + "', '" + str(diam_max) + "', '" + str(ts) + "', '" + str(dt_utc) + "', '" + str(dt_local) + "', '" + str(speed) + "', '" + str(distance) + "', '" + str(ast_id) + "', '" + str(ast_mgn) + "')")
		logger.error('Problem inserting asteroid values into DB: ' + str(e))
		pass

# Puts arrays containing info about asteroids into DB, checks each asteroid in array.
def push_asteroids_arrays_to_db(request_day, ast_array, hazardous):
	for asteroid in ast_array:
		if mysql_check_if_ast_exists_in_db(request_day, asteroid[9]) == 0:
			logger.debug("Asteroid NOT in db")
			mysql_insert_ast_into_db(request_day, hazardous, asteroid[0], asteroid[1], asteroid[2], asteroid[3], asteroid[4], asteroid[5], asteroid[6], asteroid[7], asteroid[8], asteroid[9], asteroid[10])
		else:
			logger.debug("Asteroid already IN DB")

if __name__ == "__main__":

	connection = None
	connected = False

	# Starts connection to DB
	init_db()

	# Opening connection to mysql DB
	logger.info('Connecting to MySQL DB')
	try:
		# connection = mysql.connector.connect(host=mysql_config_mysql_host, database=mysql_config_mysql_db, user=mysql_config_mysql_user, password=mysql_config_mysql_pass)
		cursor = get_cursor()
		if connection.is_connected():
			db_Info = connection.get_server_info()
			logger.info('Connected to MySQL database. MySQL Server version on ' + str(db_Info))
			cursor = connection.cursor()
			cursor.execute("select database();")
			record = cursor.fetchone()
			logger.debug('You are connected to - ' + str(record))
			connection.commit()
	except Error as e :
		logger.error('Error while connecting to MySQL' + str(e))


	# Getting todays date
	dt = datetime.now()
	request_date = str(dt.year) + "-" + str(dt.month).zfill(2) + "-" + str(dt.day).zfill(2)  
	logger.debug("Generated today's date: " + str(request_date))

	# Requests info from NASA API
	logger.debug("Request url: " + str(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key))
	r = requests.get(nasa_api_url + "rest/v1/feed?start_date=" + request_date + "&end_date=" + request_date + "&api_key=" + nasa_api_key)

	logger.debug("Response status code: " + str(r.status_code))
	logger.debug("Response headers: " + str(r.headers))
	logger.debug("Response content: " + str(r.text))

	# If there is response from NASA API
	if r.status_code == 200:

		# Response data is parsed as a jason structure
		json_data = json.loads(r.text)

		ast_safe = []
		ast_hazardous = []

		if 'element_count' in json_data:
			# Counts actual asteroids for the day
			ast_count = int(json_data['element_count'])
			logger.info("Asteroid count today: " + str(ast_count))

			if ast_count > 0:
				for val in json_data['near_earth_objects'][request_date]:
					if 'name' and 'nasa_jpl_url' and 'estimated_diameter' and 'is_potentially_hazardous_asteroid' and 'close_approach_data' in val:
						# Gets name of asteroid
						tmp_ast_name = val['name']
						# Gets urk of asteroid description
						tmp_ast_nasa_jpl_url = val['nasa_jpl_url']
						# Gets id of asteroid
						tmp_ast_id = val['id']
						# Gets magnitude of asteroid
						tmp_ast_mgn = float(val['absolute_magnitude_h'])
						# Gets diameter values - takes kilometers/not meters or miles
						if 'kilometers' in val['estimated_diameter']:
							# Gets min and max diameter values
							if 'estimated_diameter_min' and 'estimated_diameter_max' in val['estimated_diameter']['kilometers']:
								tmp_ast_diam_min = round(val['estimated_diameter']['kilometers']['estimated_diameter_min'], 3)
								tmp_ast_diam_max = round(val['estimated_diameter']['kilometers']['estimated_diameter_max'], 3)
							else:
								logger.warning("Could not get min and max diameter")
								tmp_ast_diam_min = -2
								tmp_ast_diam_max = -2
						else:
							logger.warning("Could not get diameter values in km")
							tmp_ast_diam_min = -1
							tmp_ast_diam_max = -1

						# Checks if asteroid is branded as potentially hazardous
						tmp_ast_hazardous = val['is_potentially_hazardous_asteroid']

						# Checks if there is data in close approach data array
						if len(val['close_approach_data']) > 0:
							# Gets close approach data
							if 'epoch_date_close_approach' and 'relative_velocity' and 'miss_distance' in val['close_approach_data'][0]:
								# Gets and calculates time in readable form
								tmp_ast_close_appr_ts = int(val['close_approach_data'][0]['epoch_date_close_approach']/1000)
								tmp_ast_close_appr_dt_utc = datetime.utcfromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')
								tmp_ast_close_appr_dt = datetime.fromtimestamp(tmp_ast_close_appr_ts).strftime('%Y-%m-%d %H:%M:%S')

								# Gets speed km/h 
								if 'kilometers_per_hour' in val['close_approach_data'][0]['relative_velocity']:
									tmp_ast_speed = int(float(val['close_approach_data'][0]['relative_velocity']['kilometers_per_hour']))
								else:
									logger.warning("Could not get speed")
									tmp_ast_speed = -1

								# Gets MISS distance
								if 'kilometers' in val['close_approach_data'][0]['miss_distance']:
									tmp_ast_miss_dist = round(float(val['close_approach_data'][0]['miss_distance']['kilometers']), 3)
								else:
									logger.warning("Could not get miss distance")
									tmp_ast_miss_dist = -1
							else:
								logger.warning("Could not get close approach data")
								tmp_ast_close_appr_ts = -1
								tmp_ast_close_appr_dt_utc = "1969-12-31 23:59:59"
								tmp_ast_close_appr_dt = "1969-12-31 23:59:59"
						else:
							logger.warning("No close approach data in message")
							tmp_ast_close_appr_ts = 0
							tmp_ast_close_appr_dt_utc = "1970-01-01 00:00:00"
							tmp_ast_close_appr_dt = "1970-01-01 00:00:00"
							tmp_ast_speed = -1
							tmp_ast_miss_dist = -1

						logger.info("------------------------------------------------------- >>")
						logger.info("Asteroid name: " + str(tmp_ast_name) + " | INFO: " + str(tmp_ast_nasa_jpl_url) + " | Diameter: " + str(tmp_ast_diam_min) + " - " + str(tmp_ast_diam_max) + " km | Hazardous: " + str(tmp_ast_hazardous))
						logger.info("Close approach TS: " + str(tmp_ast_close_appr_ts) + " | Date/time UTC TZ: " + str(tmp_ast_close_appr_dt_utc) + " | Local TZ: " + str(tmp_ast_close_appr_dt))
						logger.info("Speed: " + str(tmp_ast_speed) + " km/h" + " | MISS distance: " + str(tmp_ast_miss_dist) + " km" + " | Magnitude " + str(tmp_ast_mgn) + " km")
						
						# Adding asteroid data to the corresponding array
						if tmp_ast_hazardous == True:
							ast_hazardous.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist, tmp_ast_id, tmp_ast_mgn])
						else:
							ast_safe.append([tmp_ast_name, tmp_ast_nasa_jpl_url, tmp_ast_diam_min, tmp_ast_diam_max, tmp_ast_close_appr_ts, tmp_ast_close_appr_dt_utc, tmp_ast_close_appr_dt, tmp_ast_speed, tmp_ast_miss_dist, tmp_ast_id, tmp_ast_mgn])

			else:
				logger.info("No asteroids are going to hit earth today")

		logger.info("Hazardous asteorids: " + str(len(ast_hazardous)) + " | Safe asteroids: " + str(len(ast_safe)))

		if len(ast_hazardous) > 0:
			# Sorts hazardous asteroid array by time
			ast_hazardous.sort(key = lambda x: x[4], reverse=False)

			logger.info("Today's possible apocalypse (asteroid impact on earth) times:")
			
			# Loops through hazardous asteroid array and prints info starting with closest time
			for asteroid in ast_hazardous:
				logger.info(str(asteroid[6]) + " " + str(asteroid[0]) + " " + " | more info: " + str(asteroid[1]))
			
			# Sorts hazardous asteroid array by passing distance and prints info about the closest one
			ast_hazardous.sort(key = lambda x: x[8], reverse=False)
			logger.info("Closest passing distance is for: " + str(ast_hazardous[0][0]) + " at: " + str(int(ast_hazardous[0][8])) + " km | more info: " + str(ast_hazardous[0][1]))
			# Loops through hazardous astroids array and saves data in DB
			push_asteroids_arrays_to_db(request_date, ast_hazardous, 1)
		else:
			logger.info("No asteroids close passing earth today")
		if len(ast_safe) > 0:
			# Loops through safe asteroids array and saves info in DB 
			push_asteroids_arrays_to_db(request_date, ast_safe, 0)

	else:
		logger.error("Unable to get response from API. Response code: " + str(r.status_code) + " | content: " + str(r.text))
