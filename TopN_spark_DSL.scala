scala> val empList = List(
     | (101,50),
     | (101,200),
     | (102,110),
     | (101,100),
     | (103,140),
     | (101,40),
     | (102,120),
     | (102,150),
     | (102,130) 
     | )

scala> val lrdd = sc.parallelize(empList,3)
scala> lrdd.getNumPartitions

scala> val df = lrdd.toDF("empId","sal")

scala> import org.apache.spark.sql.functions.{row_number, max, broadcast}
scala> import org.apache.spark.sql.expressions.Window

scala> val w = Window.partitionBy($"empId").orderBy($"sal".desc)

// Display top 1 for each employee id
scala> val dfTop = df.withColumn("rn", row_number.over(w)).where($"rn" === 1).drop("rn")

scala> dfTop.show

// Display top 2 for each employee id
scala> val dfTopN = df.withColumn("rn", row_number.over(w)).where($"rn" < 3).drop("rn")

scala> dfTopN.show

