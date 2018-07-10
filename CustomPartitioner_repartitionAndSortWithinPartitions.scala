     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 1.6.0
      /_/

scala> :paste  
// Entering paste mode (ctrl-D to finish)

class CustomPartitioner(numParts: Int) extends Partitioner {
 override def numPartitions: Int = numParts
 override def getPartition(key: Any): Int = key match{
  case key: Int if(key < 4) => 0
  case key: Int if(key >= 4 && key < 7) => 1
  case key: Int if(key >= 7) => 2
 }
 override def equals(other: scala.Any): Boolean = {
  other match{
   case obj: CustomPartitioner => obj.numPartitions == numPartitions
   case _ => false
  }
 }
}

scala> val data = sc.parallelize(List(1,3,4,6,7,2,9,8),3)
scala> val mdata = data.map(x => (x,1))
scala> mdata.saveAsTextFile("HDFS location")
scala> val sdata = mdata.repartitionAndSortWithinPartitions(new CustomPartitioner(3))
scala> sdata.saveAsTextFile("HDFS location")
