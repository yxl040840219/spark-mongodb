package com.stratio.deep.test

import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.deep.mongodb.MongodbConfig._
import com.stratio.deep.mongodb._
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

/**
 * Created by yxl on 15/5/25.
 */
object LocalTest {
   def main(args:Array[String]): Unit ={

     val sparkConf = new SparkConf().setAppName("HdfsTest").setMaster("local[4]")
       .set("spark.cores.max","1").set("spark.executor.memory","128m")
       .set("spark.driver.memory","128m")
       .set("spark.local.dir","/Users/yxl/data/spark.dir")

     val sc = new SparkContext(sparkConf);

     val builder = MongodbConfigBuilder(
       Map(
         Host -> List("localhost:27017"),
         Database -> "test",
         Collection -> "user",
         SamplingRatio -> 1.0,
         WriteConcern -> MongodbWriteConcern.Normal
       )
     )

     val readConfig = builder.build()

     val sqlContext = new SQLContext(sc)


     val mongoRDD = sqlContext.fromMongoDB(readConfig)

     mongoRDD.registerTempTable("user")
     val newRDD = sqlContext.sql("SELECT sum(sub.b) FROM user")
     //newRDD.saveToMongodb(builder.build(),true)
     newRDD.foreach(println(_))
   }
}
