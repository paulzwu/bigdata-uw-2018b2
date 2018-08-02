

# Some SQL
'''
>>> df = spark.read.format("json").load("/data/flight-data/json/2015-summary.json")


>>> df.createOrReplaceTempView("flight_data")
>>> spark.sql("select * from flight_data").show();


>>> spark.sql("select dest_country_name as dest, origin_country_name as orig, count c from flight_data").show()




>>>

SELECT *, (DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME) as withinCountry FROM flight_data LIMIT 2
select * from flight_data where DEST_COUNTRY_NAME = ORIGIN_COUNTRY_NAME

CREATE TABLE flights (
  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
USING JSON OPTIONS (path '/data/flight-data/json/2015-summary.json')

CREATE TABLE flights (
  DEST_COUNTRY_NAME STRING, ORIGIN_COUNTRY_NAME STRING, count LONG)
USING JSON OPTIONS (path '/data/flight-data/json/2015-summary.json')

'''