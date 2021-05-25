#Import Libraries
from pyspark.sql import SparkSession, functions as F
from urllib.request import urlopen
from pyspark.sql.functions import explode

#Load data from API as a JSON Dataframe
url = "https://randomuser.me/api/0.8/?results=1000"
jsonData = urlopen(url).read().decode('utf-8')
rdd = spark.sparkContext.parallelize([jsonData])
df = spark.read.json(rdd)

   
#Create Table from JSON Dataframe
dataframe = df.select(explode("results").alias("results")).select(
"results.user.email", \
"results.user.gender", \
"results.user.location.city", \
"results.user.location.state", \
"results.user.name.last", \
"results.user.phone", \
"results.user.registered", \
"results.user.username", \
"results.user.dob")
#Add dynamic timestamp
dataframe = dataframe.withColumn('current_time', F.current_timestamp())


#Use Case I

#Create a temporary table so that spark.sql can be used
dataframe.registerTempTable("table_1")

output_1 = spark.sql('''
SELECT
gender
, split(email,'@')[1] as email_provider
, count(username) as total
FROM table_1
GROUP BY
gender
, split(email,'@')[1]
''')

#Write as a CSV File
output_1.coalesce(1).write.format('csv').option('header',True).option('sep',',').save('C:\\Users\\SUNBTC\\Desktop\\Air_Asia_Interview\\Assessment_2\\CASE_I_Output')

#Use Case II
dataframe.write.format('parquet').partitionBy('gender','state').save('C:\\Users\\SUNBTC\\Desktop\\Air_Asia_Interview\\Assessment_2\\CASE_II_Output')
