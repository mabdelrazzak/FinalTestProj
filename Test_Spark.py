from pyspark.sql import SparkSession
import pandas as pd
import gc
import parseSQL
import csv
from sqlalchemy import create_engine
import datetime




tBefore = datetime.datetime.now()
print("Time before: ", str(tBefore))

## create a SparkSession
builder = SparkSession.builder
builder = builder.config("spark.executor.memory", "4G")
builder = builder.config("spark.driver.memory", "4G")
builder = builder.config("spark.driver.maxResultSize", "6G")
builder = builder.config("spark.dynamicAllocation.enabled", "true")
builder = builder.config("spark.default.parallelism", "6")
spark = builder.appName("CG").getOrCreate()


chunkSize = 100000
sqlQueriesFilePath = "sql_queries.csv"

src_dbUser = "mabdelrazzak"
src_dbPassword = "1Rt5HrP6rGM4XvbTpUNqewvq4xVxDG"
src_dbHost = "10.233.237.174/"
src_dbURL = 'mysql+mysqlconnector://' + src_dbUser + ":" + src_dbPassword + "@" + src_dbHost

trg_dbUser = "mabdelrazzak"
trg_dbPassword = "Hy3^y@rT"
trg_dbHost = "ec2-52-28-206-42.eu-central-1.compute.amazonaws.com"
trg_dbURL = "jdbc:exa:" + trg_dbHost

sqlFile = open(sqlQueriesFilePath, 'r')
#print(sqlFile.read().split("@")[1].split(";")[1])
csvFile = csv.reader(sqlFile,delimiter=";")


sqlFile = sqlFile.read().split("@")

for line in sqlFile:
    q = line.split(";")
    cnt = 0
    for val in q:
        if cnt == 0:
            cnt = cnt + 1
            sqlStatement = val.strip(" ")
        else:
            targetTable = val.strip(" ")
    print("DB Query is: ",sqlStatement)
    print("Corresponding DB Table is: ",targetTable)
    sqlStatement = sqlStatement.lower()

    numOfRecs = 0
    print("**************** Time before starting the loop: ", datetime.datetime.now())

    engine_mysql_rapido = create_engine(src_dbURL)
    conn = engine_mysql_rapido.connect()

    tblLoadStartTime = datetime.datetime.now()
    for chunk in pd.read_sql_query(sqlStatement, conn, chunksize=chunkSize):
        print("*********** Started the loop ******************************** ", datetime.datetime.now())

        gc.collect()
        chunk = chunk.fillna(value='NULL')
        df = spark.createDataFrame(chunk)
        dataFrameCount = len(chunk)
        print("********** Create the DataFrame ********************* ", datetime.datetime.now())
        try:
            start = datetime.datetime.now()
            print("*************** Start time for chunk is: ", start)

            df.write \
                .format("jdbc") \
                .option("driver", "com.exasol.jdbc.EXADriver") \
                .option("url", trg_dbURL) \
                .option("dbtable", targetTable) \
                .option("port", "8563") \
                .option("user", trg_dbUser) \
                .option("password", trg_dbPassword) \
                .option("useSSL", "false") \
                .mode('append') \
                .save()

            end = datetime.datetime.now()
            print("*************** End time for chunk is: ", end)
            elapsed = end - start
            print("*************** Elapsed time to process a chunk is: ", elapsed)

            tAfter = datetime.datetime.now()
            print("************************** Time elapsed so far: ", tAfter - tBefore,
                  " ***************************************")

            numOfRecs = numOfRecs + dataFrameCount
            print("*************** Number of records processed so far is: ", numOfRecs)
            del chunk
            del df
            conn.close()
            gc.collect()
        except Exception as e:
            print(e)
            df.coalesce(4).write.format("com.databricks.spark.csv").option("sep", "|").option(
                "quoteAll", "true").csv(
                "rejection.csv",
                mode="append")
            del df
            gc.collect()
    tblLoadEndTime = datetime.datetime.now()
    print("*************** Time taken to process table {} is {}".format(targetTable, tblLoadEndTime - tblLoadStartTime))

    conn.close()
    del df
    gc.collect()

# for line in csvFile:
#
#     sqlStatement = line[0]
#     print("Query is: ", sqlStatement)
#     targetTable = line[1]
#
#     print("Query is: ",sqlStatement)
#     print("Target Table is: ",targetTable)
#
#     #sqlStatement = parseSQL.sqlNULLParser(sqlStatement)
#     sqlStatement = sqlStatement.lower()
#
#     print(sqlStatement)
#
#     numOfRecs = 0
#     print("**************** Time before starting the loop: ", datetime.datetime.now())
#
#     engine_mysql_rapido = create_engine(src_dbURL)
#     conn = engine_mysql_rapido.connect()
#
#     tblLoadStartTime = datetime.datetime.now()
#     for chunk in pd.read_sql_query(sqlStatement, conn, chunksize=chunkSize):
#         print("*********** Started the loop ******************************** ", datetime.datetime.now())
#
#         gc.collect()
#         chunk = chunk.fillna(value='NULL')
#         df = spark.createDataFrame(chunk)
#         dataFrameCount = len(chunk)
#         print("********** Create the DataFrame ********************* ", datetime.datetime.now())
#         try:
#             start = datetime.datetime.now()
#             print("*************** Start time for chunk is: ", start)
#
#             df.write \
#                 .format("jdbc") \
#                 .option("driver", "com.exasol.jdbc.EXADriver") \
#                 .option("url", trg_dbURL) \
#                 .option("dbtable", targetTable) \
#                 .option("port", "8563") \
#                 .option("user", trg_dbUser) \
#                 .option("password", trg_dbPassword) \
#                 .option("useSSL", "false") \
#                 .mode('append') \
#                 .save()
#
#             end = datetime.datetime.now()
#             print("*************** End time for chunk is: ", end)
#             elapsed = end - start
#             print("*************** Elapsed time to process a chunk is: ", elapsed)
#
#             tAfter = datetime.datetime.now()
#             print("************************** Time elapsed so far: ", tAfter - tBefore,
#                   " ***************************************")
#
#             numOfRecs = numOfRecs + dataFrameCount
#             print("*************** Number of records processed so far is: ", numOfRecs)
#             del chunk
#             conn.close()
#         except Exception as e:
#             print(e)
#             df.coalesce(4).write.format("com.databricks.spark.csv").option("sep", "|").option(
#                 "quoteAll", "true").csv(
#                 "rejection.csv",
#                 mode="append")
#     tblLoadEndTime = datetime.datetime.now()
#     print("*************** Time taken to process table {} is {}".format(targetTable, tblLoadEndTime - tblLoadStartTime))
#
#     conn.close()
#     del df
#     gc.collect()


spark.stop()

tAfter = datetime.datetime.now()
print("Time after: ", str(tAfter))

print("**************** Total script time: ", tAfter - tBefore, " ****************")
