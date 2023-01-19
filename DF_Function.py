from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
import pyspark.sql.functions as F

########################################################################################################################

# DataFrame Operations:

# columns = ["City","State","ZipcodeType","Zipcode"]
# data = [("Satara","MH","STD","704"),
#         ("Rahimatpur","NA","STD","703"),
#         ("Vaduth","NA","UNI","704115"),
#         ("Songaon","MH","STD","704"),
#         ("Dahigaon","PU","STD","705"),
#         ("Shivthar","MH","UNI","415011"),
#         ("Bhunj","SA","STD","705")]

# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# collect: Returns all Row
# print(df.collect())

# take: Returns record Given as an Argument
# print(df.take(2))

# printSchema: Show Datatype
# df.printSchema()

# count: Return no. of rows in df
# print(df.count())

# select: Select Column
# df.select("City","State").show()    # We can pass no. of rows in show

# filter: print dataframe based on condition
# df.filter(df["Zipcode"]>704).show()

# like: match alphabet and returns result
# df.select("City").filter("City like 'S%'").show()

# sort: sort column in ascending
# df.sort("City").show()

# describe: returns descriptive statistics (only for numerical columns)
# df.describe().show()

# columns: Returns columns
# print(df.columns)

# getItem(): gets an item at position
########################################################################################################################

# SQL Queries:

# columns = ["City","State","ZipcodeType","Zipcode"]
# data = [("Satara","MH","STD","704"),
#         ("Rahimatpur","NA","STD","703"),
#         ("Vaduth","NA","UNI","704115"),
#         ("Songaon","MH","STD","704"),
#         ("Dahigaon","PU","STD","705"),
#         ("Shivthar","MH","UNI","415011"),
#         ("Bhunj","SA","STD","705")]

# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# >>>>> df.createOrReplaceTempView("df") <<<<<

# spark.sql("select * from df").show()

# spark.sql("select * from df where State='MH'").show()

# spark.sql("select count(City) from df").show()

# spark.sql("select City,State from df where zipcode=705").show()

#########################################################################################################################

# Window Functions: - used to Carry out Aggregations.
#                   - window function returns result for each record from df.

import pyspark.sql.functions as F
from pyspark.sql import Window
import pyspark.sql.types as T

# columns = ["year","dept","salary"]
# data = [('2004','IT','3000'),
#         ('2004','IT','4000'),
#         ('2005','HR','5000'),
#         ('2005','IT','6000'),
#         ('2004','HR','5000'),
#         ('2006','Sales','2000'),
#         ('2007','Accounts','9000')
#         ]

# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# windowSpec = Window.partitionBy("dept")

# With Columns Creates new column in dataframe
# data1=df.withColumn("list_salary",F.collect_list(F.col("salary")).over(windowSpec)) \
#         .withColumn("average_salary",F.avg(F.col("salary")).over(windowSpec))  \
#         .withColumn("total_salary",F.sum(F.col("salary")).over(windowSpec))

# data1.show()

#-----------------------------------------------------------------------------------------------------------------------

# groupBy function: used to group everything into one row or one record

# columns = ["year","dept","salary"]
# data = [('2004','IT','3000'),
#         ('2004','IT','4000'),
#         ('2005','HR','5000'),
#         ('2005','IT','6000'),
#         ('2004','HR','5000'),
#         ('2006','Sales','2000'),
#         ('2007','Accounts','9000')]

# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# import pyspark.sql.functions as F

# data2 = df.groupBy("dept").agg(
#         F.expr("collect_list(salary)").alias("list_salary"),
#         F.expr("avg(salary)").alias("average_salary"),
#         F.expr("sum(salary)").alias("total_salary"))

# data2.show()

########################################################################################################################
# withColumn, withColumnRenamed

# columns = ["year","dept","salary"]
# data = [('2004','IT','3000'),
#         ('2004','IT','4000'),
#         ('2005','HR','5000'),
#         ('2005','IT','6000'),
#         ('2004','HR','5000'),
#         ('2006','Sales','2000'),
#         ('2007','Accounts','9000')]

# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# withColumn: Used to create a new column (but when we give the same column name then it will replace the original column and creates new one)
# df.withColumn("double_salary",df.salary*2).show()

# withColumnRenamed: Used to rename a column using functions
# df.withColumnRenamed("year","joining_year").show()

########################################################################################################################
# Alias, Distinct, OrderBY

# data=[("001","Ram","25","20000"),
#     ("002","Sham","29","30000"),
#     ("003","Kiran","31","10000"),
#     ("004","Mina","34","14000"),]

# columns=["id","name","age","salary"]

# df=spark.createDataFrame(data).toDF(*columns)
# print(df.show())


# Alias: used to Give Temporary name to the column, it will not change original col name
# df.select(df.name.alias("StdName")).show()


# Distinct: used to give unique values
# df.select("salary").distinct().show()


# OrderBy: Used to sort column in Ascending or descending order
# df.select(df.salary).orderBy(df.salary.asc()).show()
# df.select(df.salary).orderBy(df.salary.desc()).show()

########################################################################################################################
# isNull,isNotNull,na.drop,na.fill

# columns = ["year","dept","salary"]
# data = [('2004','IT','3000'),
#         ('2004','IT','3000'),
#         ('2005','HR','5000'),
#         ('2005','IT','6000'),
#         ('2004','HR','5000'),
#         ('2006','Sales','2000'),
#         ('2007','Accounts','9000')]

# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# isNull: Displays row which are null
# df.filter(df.salary.isNull()).show()

# isNotNull: Displays row which are not null
# df.filter(df.salary.isNotNull()).show()

# na.drop: drop all rows from entire dataframe having null/ Missing values
# df.na.drop().show()

# na.fill: fill missing values in any column
# df.na.fill({'salary':5000}).show()


########################################################################################################################
# countDistinct,concat,collect_list and length

import pyspark.sql.functions as F

# data=[("001","Ram","25","20000"),
#     ("002","Sham","29","30000"),
#     ("003","Kiran","31","10000"),
#     ("004","Mina","34","14000"),]
# columns=["id","name","age","salary"]
# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# countDistinct:
# df.agg(F.countDistinct("age")).show()


# collect_list: returns result in the form of list
# df.agg(F.collect_list("name")).show(truncate=False)
# >>>>> for group by >>>>> df.groupBy('department_no').agg(F.collect_list("employee_no").alias('employee_no'))


# length: gives length of words
# df.select(F.length("name")).show()


# concat: we can concat two columns using this method
# df.select(F.concat(df["id"],df["name"])).show()


# split(str,pattern,limit): splits columns
# df.select('*',F.split('salary','').alias('new_sal')).show()

########################################################################################################################
# isin,like,rlike,substr

# data=[("001","Ram","25","20000"),
#     ("002","Sham","29","30000"),
#     ("003","Kiran","31","10000"),
#     ("004","Mina","34","14000"),]

# columns=["id","name","age","salary"]

# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# isin: returns value found in column
# df[df.name.isin("Kiran")].show()

# like: returns value from column based on match (%)
# df.filter(df.name.like("R%")).show()

# rlike: return column value based on right side match ($)
# df.filter(df.name.rlike("m$")).show()

# substr: returns substring from column
# df.select(df.name.substr(1,3).alias("result")).show()

########################################################################################################################

# row_number,Rank, Dense Rank, Percent Rank:

import pyspark.sql.functions as F
from pyspark.sql import Window

# columns = ["year","dept","salary"]
# data = [('2004','IT','3000'),
#         ('2004','IT','3000'),
#         ('2005','HR','5000'),
#         ('2005','IT','6000'),
#         ('2004','HR','5000'),
#         ('2006','Sales','2000'),
#         ('2007','Accounts','9000')]

# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# >>> partition on column
# windowSpec=Window.partitionBy("dept").orderBy("salary")

# row_number: used to give sequential row_number starting from 1 to result
# df.withColumn("row_number",F.row_number().over(windowSpec)).show()

# rank: used to provide rank (If result is same then it give the same rank and skip the next number)
# df.withColumn("rank",F.rank().over(windowSpec)).show()

# dense_rank: used to provide rank (If result is same then it give same rank and then gives next number)
# df.withColumn("dense_rank",F.dense_rank().over(windowSpec)).show()

# percent_rank: used to provide rank based on percent (If two rows have different result then it will be 0%)
# df.withColumn("percent_rank",F.percent_rank().over(windowSpec)).show()

########################################################################################################################
# Analytical Function: lead, lag

import pyspark.sql.functions as F
from pyspark.sql import Window

# columns = ["year","dept","salary"]
# data = [('2004','IT','3000'),
#         ('2004','IT','3000'),
#         ('2005','HR','5000'),
#         ('2005','IT','6000'),
#         ('2004','HR','5000'),
#         ('2006','Sales','2000'),
#         ('2007','Accounts','9000')]

# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# windowSpec = Window.partitionBy("dept").orderBy("salary")

# Lag: To get previous value of current value
# df.withColumn("lag",F.lag("salary",1).over(windowSpec)).show()

# Lead: To get next value of current value
# df.withColumn("lead",F.lead("salary",1).over(windowSpec)).show()

########################################################################################################################
# Grouped data aggregation function: min,max,sum,mean,avg

# columns = ["year","dept","salary"]
# data = [('2004','IT','3000'),
#         ('2004','IT','3000'),
#         ('2005','HR','5000'),
#         ('2005','IT','6000'),
#         ('2004','HR','5000'),
#         ('2006','Sales','2000'),
#         ('2007','Accounts','9000')]

# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

import pyspark.sql.functions as F

# min:
# df.agg(F.min(df.salary)).show()

# max:
# df.agg(F.max(df.salary)).show()

# sum:
# df.agg(F.sum(df.salary)).show()

# mean:
# df.agg(F.mean(df.salary)).show()

# avg:
# df.agg(F.avg(df.salary)).show()

########################################################################################################################
# expr function: Takes sql expression as a string argument, executes the expression and returns a pyspark column type

# columns = ["year","dept","salary"]
# data = [('2004','IT','3000'),
#         ('2004','IT','3000'),
#         ('2005','HR','5000'),
#         ('2005','IT','6000'),
#         ('2004','HR','5000'),
#         ('2006','Sales','2000'),
#         ('2007','Accounts','9000')]

# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# concat columns using || with expr
# df.withColumn("name",F.expr("year || ',' || dept")).show()

# case when with expr function:  used to update existing row of dataframe
# df.withColumn("Department", F.expr("case when dept = 'IT' then 'Information Technology'" + "when dept = 'HR' then 'Human Resources' else 'UNKNOWN' end")).show()

# arithmetic operations with expr function
# df.select(df.dept, df.salary, F.expr('salary + 1000 as new_salary')).show()

# filter method using expr function: used to check two column have same value
# df.filter(F.expr("year == salary")).show()

########################################################################################################################
# Data/Time Functions:

from pyspark.sql.functions import *

# data=[["1","2020-02-01"],["2","2019-03-01"],["3","2021-03-01"]]
# df=spark.createDataFrame(data,["id","dt"])
# df.show()
# df.printSchema()

# current_date():
# df.withColumn("todays_date",current_date()).show()

# current_timestamp():
# df.withColumn("todays_date_time",current_timestamp()).show()

# date_add(): (if we give -1 then it will subtract date)
# df.select(col("dt"),date_add(col("dt"),1).alias("date_add")).show()

# date_format():
# df.select(col("dt"),date_format(col("dt"),"MMMddyyy")).show()

# date_trunc():
# df.select(col("dt"),date_trunc("year",to_timestamp(col("dt")))).show()

########################################################################################################################
# filter, Where

# columns = ["year","dept","salary"]
# data = [('2004','IT','3000'),
#         ('2004','IT','4000'),
#         ('2005','HR','5000'),
#         ('2005','IT','6000'),
#         ('2004','HR','5000'),
#         ('2006','Sales','2000'),
#         ('2007','Accounts','9000')]
# df=spark.createDataFrame(data).toDF(*columns)
# df.show()

# filter(): use to filter the rows from dataframe

# df.filter("dept == 'IT'").show()     # >>>>> filter with sql Expression
# df.filter(df.dept == 'IT').show()
# df.filter((df.year == '2004') & (df.dept == 'IT')).show()

# where(): we can use where instead of filter

# df.where(df.dept == 'IT').show()

########################################################################################################################
# Union, UnionAll

# columns1 = ["year","dept","salary"]
# data1 = [('2004','IT','3000'),
#         ('2004','IT','4000'),
#         ('2005','HR','5000'),
#         ('2005','IT','6000'),
#         ('2004','HR','5000'),
#         ('2006','Sales','2000'),
#         ('2007','Accounts','9000')]
# df1=spark.createDataFrame(data1).toDF(*columns1)
# df1.show()
#------------------------------------------------------------------------------
# columns2 = ["year","dept","salary"]
# data2 = [('2002','Accounts','8000'),
#         ('2004','IT','41000'),
#         ('2003','HR','55000'),
#         ('2005','IT','61000'),
#         ('2001','HR','58000'),
#         ('2005','Sales','32000'),
#         ('2006','Accounts','9000')]
# df2=spark.createDataFrame(data2).toDF(*columns2)
# df2.show()
#------------------------------------------------------------------------------

# union(): used to merge two dataframes of same structure.
# df_union=df1.union(df2).show()

# use distinct() function to remove duplicate records
# df_union_distinct=df1.union(df2).distinct().show()


# unionall(): unionAll() method is deprecated since PySpark “2.0.0” and recommends using the union()


########################################################################################################################
# explode Function: split multiple array column into row

# >>>>> with array column <<<<<
# array_appliances=[
#     ('ram',['tv','refrigerator','machine']),
#     ('sham',['washing machine','oven'])]
# array_schema=['name','appliances']

# df=spark.createDataFrame(data=array_appliances,schema=array_schema)
# df.show(truncate=False)
# df.printSchema()

# df_explode=df.select(df.name,explode(df.appliances)).show()


# <<<<< with map column >>>>> :key value Pair

# map_brand=[
#           ('ram',{'TV':'LG','refrigerator':'samsung','oven':'philips'}),
#           ('sham',{'ac':'voltas','washing machine':'LG'})
#           ]
# map_schema=['name','brand']
# df=spark.createDataFrame(data=map_brand,schema=map_schema)
# df.show(truncate=False)

# df_mapexplode=df.select(df.name,explode(df.brand)).show()

########################################################################################################################

# rlike() function:  It takes a literal regex expression string as a parameter and returns a boolean column based on a regex match.

# df=spark.read.csv(r"C:\Users\HP\OneDrive\Desktop\Pycharm coding\Assignment_2\employee.csv",header=True)
# df.show()

# df1=df.filter(col('Department').rlike("[0-9a-zA-Z_\-\s]"))
# df1.show()


