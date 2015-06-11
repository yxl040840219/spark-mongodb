package com.stratio.deep.test

import com.mongodb.casbah.{WriteConcern => MongodbWriteConcern}
import com.stratio.deep.mongodb.MongodbConfig._
import com.stratio.deep.mongodb._
import com.stratio.deep.mongodb.writer.MongodbSimpleWriter
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

     val select = MongodbConfigBuilder(
       Map(
         Host -> List("localhost:27017"),
         Database -> "test",
         Collection -> "user",
         SamplingRatio -> 1.0,
         WriteConcern -> MongodbWriteConcern.Normal
       )
     )



     val write = MongodbConfigBuilder(
       Map(
         Host -> List("localhost:27017"),
         Database -> "test",
         Collection -> "result",
         SamplingRatio -> 1.0,
         WriteConcern -> MongodbWriteConcern.Normal
       )
     )

     val readConfig = select.build()

     val writeConfig = write.build()

     val sqlContext = new SQLContext(sc)


     val writer = new MongodbSimpleWriter(writeConfig)


     val mongoRDD = sqlContext.fromMongoDB(readConfig)

     mongoRDD.registerTempTable("user")
     val newRDD = sqlContext.sql("SELECT type,sum(sub.a) as t_count FROM user group by type")
     // 写数据
     newRDD.saveToMongodb(writeConfig,false)
     newRDD.foreach(println(_))
   }
}
