# Passwords and Keys
connection_password_GCP = 'Connection Password MySQL Google Cloud Instance '

Flights_API_Key = 'API Key' # https://aerodatabox.p.rapidapi.com/flights
Weather_API_Key = 'API Key' # https://api.openweathermap.org
Ninja_Key = 'erL338qTF08k2IUS5JpnMA==WAaM1WJ1jbXHArfJ'   # https://api.api-ninjas.com/v1/

# connection string GCP
schema_GCP = "gans_sql_GCP"
host_GCP = "IP Google Cloud Instance"
user_GCP = "root"
password_GCP = connection_password_GCP #"YOUR_PASSWORD"
port_GCP = 3306

connection_string_GCP = f'mysql+pymysql://{user_GCP}:{password_GCP}@{host_GCP}:{port_GCP}/{schema_GCP}'