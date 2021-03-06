

# About SQL

Prepare Sample Data
```
$> git clone https://github.com/databricks/Spark-The-Definitive-Guide.git
$> cd Spark-The-DEfinitive-Guide/data
$> hdfs dfs -put flight_data /data

```

Sample Code:
```
$> git clone https://github.com/paulzwu/bigdata-uw-2018b2.git
$> cd bigdata-uw-2018b2/code
$> spark-submit sql_1.py
$> spark-submit sq2_1.py
```
Spark Tool Commands

```
$#Run spark programs.
$spark-submit --driver-java-options "-Dlog4j.configuration=https://raw.githubusercontent.com/paulzwu/bigdata-uw-2018b2/master/log4j" sql_1.py
$spark-submit --driver-java-options "-Dlog4j.configuration=https://raw.githubusercontent.com/paulzwu/bigdata-uw-2018b2/master/log4j" sql_2.py
$#spark-sql shell
$spark-sql --driver-java-options "-Dlog4j.configuration=https://raw.githubusercontent.com/paulzwu/bigdata-uw-2018b2/master/log4j"


```


```
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

CREATE TABLE flights_csv (
  DEST_COUNTRY_NAME STRING,
  ORIGIN_COUNTRY_NAME STRING COMMENT "remember, the US will be most prevalent",
  count LONG)
USING csv OPTIONS (header true, path '/data/flight-data/csv/2015-summary.csv')

```