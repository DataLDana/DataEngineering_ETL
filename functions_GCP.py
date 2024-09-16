import pandas as pd
import requests
from datetime import datetime 

#++++++++++++++++++++++++++ cities with API function ++++++++++++++++++++++++++++++
def city_to_df_api(city_names):
    """
        Description:
        1) prepare the API call
        2) get city information from API
        3) send city table to SQL and get it back with ids
        Parameters:
            None
        Returns:
            df with city information
        """
    
    # 1) prepare API call
    # import connection string for SQL
    from keys import connection_string_GCP
    # import ninja api key
    from keys import Ninja_Key
    
    # specify API settings
    url = 'https://api.api-ninjas.com/v1/city'
    header = {'X-Api-Key': Ninja_Key}
    
    # 2) get city information from API
    city_items = []
    for item in city_names:   # loop different cities
        # set city name for API
        querystring = {'name': item}
        # get resonsponse/json of API
        response = requests.request('GET', url, headers=header,params=querystring)
        json_city = response.json()
    
        # query city information and store in dictionary
        if json_city:     # only do if json is found
            city_item = {
                'city_name' : item,
                'country' : json_city[0]['country'],
                'latitude' : json_city[0]['latitude'],
                'longitude' : json_city[0]['longitude']}
        else:
            print('City not found')
        # store dictionary in list    
        city_items.append(city_item)
        
    # create dataframe
    df = pd.DataFrame(city_items)
    
    # 3) load city information to mySQL
    only_add_new('cities', df, ['city_name','country'], connection_string_GCP)

    # get back cities table from SQL with city_id
    df = pd.read_sql('cities', con=connection_string_GCP)
    
    return df
#++++++++++++++++++++++++++ airport API function ++++++++++++++++++++++++++
def airport_to_df():
    """
        Description:
        1) prepare the API call
        2) get airport information from API
        3) send airport table to SQL and get it back with ids
        4) create bridge table and send it to sql as 
           airport/cities is a many to many relationsship
        Parameters:
            None
        Returns:
            df with airport information
     """
    
    # 1) prepare API call
    # get connection string
    from keys import connection_string_GCP 
    # get API key from key function
    from keys import Flights_API_Key 
    key = Flights_API_Key
    # get cities_df from database
    cities_df = pd.read_sql('cities', con = connection_string_GCP) # get cities_df from sql database
    # specify API settings
    url = 'https://aerodatabox.p.rapidapi.com/airports/search/location'
    radius = 50  # API will search for airports within a 50km radius of the city 
    headers = {
            'x-rapidapi-key': key,
        	'x-rapidapi-host': 'aerodatabox.p.rapidapi.com'}
    
    # 2) get airport information from API 
    airport_items=[] # empty list for each airport
    # loop through all cities
    for i, row in cities_df.iterrows():

        # get latitude and longitude from cities_df
        lat = row['latitude']
        lon = row['longitude']
        
        # get resonsponse/json of API
        querystring = {
            'lat':lat,'lon':lon,
            'radiusKm':radius,'limit':'5','withFlightInfoOnly':'true'}
        response = requests.get(url, headers=headers, params=querystring)
        jsons = response.json()
        
        # query information and store in dictionary
        for item in jsons['items']: 
            airport_item ={
                        'city_id' : row['city_id'],        
                        'airport_name' : item['name'],
                        'iata' : item['iata'],
                        'time_zone' : item['timeZone']}
            airport_items.append(airport_item)

    # create dataframe
    df = pd.DataFrame(airport_items) # temporary df (partwise merge of cities and airport, airport id missing)

    # 3) create airport table and send to sql, get back with ids
    
    # create airport table (unique airports)
    airports_df = df[['airport_name','iata','time_zone']].drop_duplicates()
    # send airports_df to sql to generate id
    only_add_new('airports', airports_df, ['iata'], connection_string_GCP)
    # get airports back from sql with airports_id
    airports_df = pd.read_sql('airports', con=connection_string_GCP)

    # 4) create bridge table and send it to sql as airport/cities is a many to many relationsship
    
    # create bridge table airport_id & city_id
    city_airports = df.merge(
                airports_df[['airport_id','iata']],
                on = 'iata',
                how = 'left'
                ).drop(['airport_name','iata','time_zone'],axis=1)
    # send to sql
    only_add_new(
        'city_airports', city_airports, ['city_id','airport_id'], connection_string_GCP)

    return airports_df, city_airports
#+++++++++++++++++++ population with api function +++++++++++++++++++++++++++

def population_to_df():
    """
    Description:
    1) Prepare the API call
    2) Get population information from API
    3) send population table to SQL
    Parameters:
        None
    Returns:
        df with population information
    """
    
    # 1) prepare the API call
    # get connection string
    from keys import connection_string_GCP
    # import ninja api key
    from keys import Ninja_Key
    # get cities_df from database
    cities_df = pd.read_sql('cities', con=connection_string_GCP)
    # get todays year
    today = datetime.today().strftime('%Y')
    # specify API settings
    url='https://api.api-ninjas.com/v1/city'
    header={'X-Api-Key': Ninja_Key}
    
    # 2) get population information from API 
    # loop different cities
    population_items = []
    for j, row in cities_df.iterrows():
        # set the city name
        querystring = {'name': row['city_name']}
        # get resonsponse/json of API
        response = requests.request('GET', url, headers=header,params=querystring)
        json_city = response.json()
        # query information and store in dictionary
        population_item = {
                          'city_id' : row['city_id'],
                          'population' : json_city[0]['population'],
                          'query_time' : today}
        # store dictionary in list  
        population_items.append(population_item)
    
    # create dataframe
    df = pd.DataFrame(population_items)
    
    # 3) send info to SQL
    only_add_new('populations', df, ['city_id','query_time'], connection_string_GCP)
    
    return df
    
#+++++++++++++++++++++ weather from api function +++++++++++++++++++++++++++
def weather_to_df():
    """
    Description:
    1) prepare the API call
    2) Get weather information from API
    3) send  weather table to SQL and get it back with ids
    Parameters:
        None
    Returns:
        df with weather information
    """
    
    # 1) prepare the API call
    # get connection string
    from keys import connection_string_GCP
    # get Weather API Key
    from keys import Weather_API_Key
    key = Weather_API_Key
    # pull cities from SQL data base
    cities_df = pd.read_sql('cities', con = connection_string_GCP)
    
    # 2) Get weather information from API
    weather_items = [] # create empty list for saving
    for j, row in cities_df.iterrows():  # loop through different cities

        # get cities information
        city_id = row['city_id']
        lat = row['latitude']
        lon = row['longitude']
        
        # get resonsponse/json of API
        url = (f'https://api.openweathermap.org/data/2.5/forecast?'
               f'lat={lat}&lon={lon}&appid={key}&units=metric')
        response = requests.get(url)
        jsons = response.json()
    
        # Iterate over different forcast_times
        for item in jsons['list']:    
            # get one weather 
            weather_item = {
                # state the city
               'city_id':city_id, 
                # create query_time
                'query_time': pd.Timestamp(datetime.today()).strftime('%Y-%m-%d %H:%M:%S'),
                # create forecast time        
               'forcast_times':item['dt_txt'],
                # create temperature    
               'temperature':item['main']['temp'],
                # create main weather, which is an overview        
               'weather':item['weather'][0]['main'],
                # create weather desciption to get an idea of the weather        
               'weather_desc':item['weather'][0]['description'],
                # get weather id, weather will be identified by this
               'weather_ident':item['weather'][0]['id'],
                # get windspeed in m/s         
               'wind':item['wind']['speed'],
                # get rain volumne in the last 3h in mm if its there      
                # item('rain',{}), get rain if its there else set it to {},
                # neccessary because the second get cannot get None
               'rain':item.get('rain', {}).get('3h',0),   
                # get snow volume in the last 3h in mmm if its there
               'snow':item.get('snow', {}).get('3h',0),
                # get the probability of precipitation from 0 to 1
               'prop':item['pop'],
                # get the visibilty in km, max = 10000
               'visibility':item['visibility'] }
            weather_items.append(weather_item)

    # create the dataframe
    df = pd.DataFrame(weather_items)
    # change time to datetime
    df['forcast_times'] = pd.to_datetime(df['forcast_times']) 
    # change unix code in timestamp
    df['query_time'] = pd.to_datetime(df['query_time'], unit='s') 

    # 3) sending weather data to SQL
    # only send if city_id & query_time combination is new (updates for new query times)
    only_add_new('weathers', df, ['city_id','query_time'], connection_string_GCP)
    
    return df

# ++++++++++++++++++++++++++ flights API function ++++++++++++++++++++++++++
def flights_to_df():
    """
    Description:
    1) Prepare the API call
    2) Get flight information from API
    3) send flights table to SQL and get it back with ids
    Parameters:
        None
    Returns:
        df with flight information
    """
    # get key from key function
    from keys import Flights_API_Key
    key = Flights_API_Key
    # get connection string
    from keys import connection_string_GCP 

    # 1) prepairing the API call  
    # get airports from sql database
    airports_df = pd.read_sql('airports', con = connection_string_GCP)
    
    # set times, get flight infos between now + 24 h and for a duration of 12h
    now =  pd.Timestamp(datetime.today())  # the time of now
    # times in right format
    froms = (now + pd.Timedelta('24h')).strftime('%Y-%m-%dT%H:%M')  # now + 24 h
    # froms = pd.Timestamp(now.year, now.month, now.day +1)  # alternative: next day 00:00 h
    tos = (now + pd.Timedelta('36h')).strftime('%Y-%m-%dT%H:%M') # flight for 12 h
    
    # set querystring and header for API
    querystring = {
                   'withLeg':'False',      
                   'direction':'Arrival',  # only show arrivals
                   'withCancelled':'false',
                   'withCodeshared':'true', # If set to true, result will include flights with all code-shared statuses.
                   'withCargo':'false',
                   'withPrivate':'false',
                   'withLocation':'false'}
    headers = {
              'x-rapidapi-key': key,
              'x-rapidapi-host': 'aerodatabox.p.rapidapi.com'}
    
    # 2) get flight information from API
    flight_items = []   # emtpy list for saving flights 
    for i in range(len(airports_df)): # loop trough different airports 
        # set the airport
        iata = airports_df.loc[i,'iata']# eg 'BER'
        # inset API specifications
        url = f'https://aerodatabox.p.rapidapi.com/flights/airports/iata/{iata}/{froms}/{tos}'
        response = requests.get(url, headers = headers, params = querystring)
        jsons = response.json()
        
        # loop trough different flights
        for item in jsons['arrivals']:
            # only take operator flights, no double entries in case of codeshared flights
            if item['codeshareStatus'] == 'IsOperator': 
                flight_item = {
                'airport_id' : airports_df.loc[i,'airport_id'],
                'query_time' : now.strftime('%Y-%m-%d %H:%M:%S'), # time of query
                'flight_number' : item['number'], # fligh number
                 # scheduled Time of arrival; convert string to timestamp and then convert to right format
                'landing_time'  : pd.Timestamp(item['movement']['scheduledTime']['local']
                                                              ).strftime('%Y-%m-%d %H:%M'), 
                # scheduled Time of arrival, utc
                'landing_time_utc'  : pd.Timestamp(item['movement']['scheduledTime']['utc']
                                                              ).strftime('%Y-%m-%d %H:%M')
                }
                flight_items.append(flight_item)
        
    df = pd.DataFrame(flight_items)
    
    # 2) send flights table to SQL and get it back with ids
    # send df to sql to generate id
    # only send if airport_id, flight_number and landing_time is new
    # (updates landing times)
    only_add_new(
            'flights', df, ['airport_id','flight_number','landing_time'], 
            connection_string_GCP)
    # get airports back from sql with airports_id
    df = pd.read_sql('flights', con=connection_string_GCP)

    return df
#++++++++++++++++++++++++++ only add new data to sql database ++++++++++++++++++++++
def only_add_new(table_name,
                 data,
                 composite_key,
                 connection_string):
    """
    Code kindly provided by Rockwell Gulassa
    Description: 
        Only send fresh data from a data frame to the schema.
        Creates a composite key out multiple columns, then 
        only appends data that does not match existing data's 
        composite keys
    Parameters:
        table_name: string | name of table to send data to
        data: data frame | new data to send to schema
        composite_key: iterable | column names to match
        connection_string: string | connection to schema
    Returns:
        None
    """
    new_data = data.copy()
    # Pull old data from database
    old_data = pd.read_sql(table_name,
                           con=connection_string)
    # Create composite key column for new data and old data
    # (Data that matches values in these columns will not be appended)
    old_data['composite_key'] = old_data.apply(
        lambda row: ''.join(str(row[column]) for column in composite_key), axis=1)
    new_data['composite_key'] = new_data.apply(
        lambda row: ''.join(str(row[column]) for column in composite_key), axis=1)
    # Subtract set of composite keys of old data from set of new data
    old_keys = set(old_data['composite_key'])
    new_keys = set(new_data['composite_key'])
    fresh_data_keys = new_keys - old_keys
    # Use .loc to select and then append the fresh data
    fresh_data = new_data.loc[new_data['composite_key'].isin(fresh_data_keys)].copy()
    fresh_data.drop(columns='composite_key', inplace=True)
    fresh_data.to_sql(table_name,
                      con=connection_string,
                      if_exists='append',
                      index=False)
