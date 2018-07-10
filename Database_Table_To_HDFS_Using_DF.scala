//commit to git
create table sparkDfTest (a varchar(10),b int);
//inserts
insert into sparkDfTest values ('a',1);
insert into sparkDfTest values ('b',2);
insert into sparkDfTest values ('c',3);
insert into sparkDfTest values ('d',4);

mysql> select * from sparkDfTest;
+------+------+
| a    | b    |
+------+------+
| a    |    1 |
| b    |    2 |
| c    |    3 |
| d    |    4 |
+------+------+
4 rows in set (0.00 sec)

      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.6.0
      /_/

	  
// enter spark-shell with following command.
spark-shell --jars /home/avneet10july/work/jars/mysql-connector-java-5.1.38.jar,/home/avneet10july/work/jars/spark-csv_2.10-1.5.0.jar,/home/avneet10july/work/jars/commons-csv-1.1.jar

// load database table as dataFrame
scala> val dfSql = sqlContext.read.format("jdbc").
     | option("url","jdbc:mysql://ip address of mysql server/db name").
     | option("driver","com.mysql.jdbc.Driver").
     | option("dbtable","sparkDfTest").
     | option("user","-- your username").
     | option("password","-- your pwd").load()
	 

// display table content	 
dfSql.show()

// write dataFrame to HDFS
dfSql.write.format("com.databricks.spark.csv").option("header", "false").save("HDFS location")

