package com.stratio.deep.test

import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by yxl on 15/6/8.
 */
object DataFrameTest {
    def main(args:Array[String]): Unit ={

      val sparkConf = new SparkConf().setAppName("DataFrameTest").setMaster("local[4]")
        .set("spark.cores.max","1").set("spark.executor.memory","128m")
        .set("spark.driver.memory","128m")
        .set("spark.local.dir","/Users/yxl/data/spark.dir")
      val sc = new SparkContext(sparkConf);

      val sqlContext = new SQLContext(sc)

      val df = sqlContext.jsonFile("/Users/yxl/data/person.json")

      df.show()

      df.printSchema()

      df.select(df("age") > 21).show()

      df.filter(df("age") > 21).show()

      // Create an RDD
      val people = sc.textFile("/Users/yxl/data/person.txt")

      // The schema is encoded in a string
      val schemaString = "name age"

      // Import Row.
      import org.apache.spark.sql.Row;

      // Import Spark SQL data types
      import org.apache.spark.sql.types.{StructType,StructField,StringType};

      // Generate the schema based on the string of schema
      val schema =
        StructType(
          schemaString.split(" ").map(fieldName => StructField(fieldName, StringType, true)))

      // Convert records of the RDD (people) to Rows.
      val rowRDD = people.map(_.split(",")).map(p => Row(p(0), p(1).trim))

      // Apply the schema to the RDD.
      val peopleDataFrame = sqlContext.createDataFrame(rowRDD, schema)

      // Register the DataFrames as a table.
      peopleDataFrame.registerTempTable("people")

      // SQL statements can be run by using the sql methods provided by sqlContext.
      val results = sqlContext.sql("SELECT name FROM people")

      // The results of SQL queries are DataFrames and support all the normal RDD operations.
      // The columns of a row in the result can be accessed by ordinal.
      results.map(t => "Name: " + t(0)).collect().foreach(println)


    }
}
