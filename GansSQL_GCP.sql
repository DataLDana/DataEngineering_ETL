/***************************
Setting up the environment
***************************/

-- Drop the database if it already exists
DROP DATABASE IF EXISTS gans_sql ;

-- Create the database
CREATE DATABASE gans_sql;

-- Use the database
USE gans_sql;


-- Create the 'cities' table
CREATE TABLE cities (
    city_id INT AUTO_INCREMENT, -- Automatically generated ID for each city
	city_name VARCHAR(255) NOT NULL UNIQUE, -- Name of the author
    country VARCHAR(255), -- country code DE, GB
    -- TO DO introduce countrycode
    latitude FLOAT NOT NULL, -- TO DO change to decimal
    longitude FLOAT NOT NULL, -- change to decimal
    PRIMARY KEY (city_id) -- Primary key to uniquely identify each author
);

-- Create the 'populations' table
CREATE TABLE populations (
    city_id INT , -- city ID, which is the foreign key
	population DECIMAL NOT NULL, -- population
    query_time YEAR, -- timestamp
    FOREIGN KEY (city_id) REFERENCES cities(city_id)-- secondary key
    -- this is the many side city(one) to population(many)
);

--  DROP TABLE weathers;

-- create the weather table
CREATE TABLE weathers (
	weather_id INT AUTO_INCREMENT, -- weather id, primary key
    city_id INT , -- city ID, which is the foreign key
    query_time DATETIME,
	forcast_times DATETIME NOT NULL, -- population
    temperature FLOAT,
    weather VARCHAR(255),
    weather_desc VARCHAR(255),
    weather_ident INT,
    wind FLOAT,
    rain FLOAT,
    snow FLOAT,
    prop FLOAT,
    visibility INT,
    PRIMARY KEY (weather_id), -- Primary key to uniquely identify each weather
    FOREIGN KEY (city_id) REFERENCES cities(city_id)-- secondary key
    -- this is the many side city(one) to population(many)
);

-- DROP TABLE airports;
CREATE TABLE airports (
	airport_id INT AUTO_INCREMENT, -- airport id, primary key
	airport_name CHAR(255) NOT NULL, -- airport name
    iata CHAR(3) NOT NULL, -- iata code for the airport
    time_zone CHAR(255), -- time zone of the airport
    PRIMARY KEY (airport_id) -- Primary key 
);

CREATE TABLE city_airports (
    city_id INT , -- city ID
    airport_id INT, -- airport id
    FOREIGN KEY (city_id) REFERENCES cities(city_id),-- secondary key
    FOREIGN KEY (airport_id) REFERENCES airports(airport_id)-- secondary key
);

CREATE TABLE flights (
	flight_id INT AUTO_INCREMENT, -- flight id, primary key
    airport_id INT,
	query_time DATETIME, -- airport name
    flight_number CHAR(255) NOT NULL, -- flight number
    landing_time CHAR(255), -- landing time
    landing_time_utc CHAR(255), -- landing time
    PRIMARY KEY (flight_id), -- Primary key 
    FOREIGN KEY (airport_id) REFERENCES airports(airport_id)-- secondary key
);

SELECT * FROM cities;
SELECT * FROM weathers;
SELECT * FROM populations;
SELECT * FROM airports;
SELECT * FROM city_airports;
SELECT * FROM flights;
