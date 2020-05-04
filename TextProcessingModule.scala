package com.inese.cities

import org.apache.spark.sql.SparkSession

object TextProcessingModule {

    def getCities (filePath: String, spark: SparkSession): Unit = {
      println("Opening CSV file and converting it to spark DF")
      val citiesDF = spark.read.option("header", "true").csv("C:\\Users\\merlins\\Downloads\\states.csv")
      println(citiesDF.show)
    }


    def getStates (filePath: String, spark: SparkSession): Unit = {
      println (s"Opening JSON at $filePath as spark DF")
      val statesDF = spark.read.option("multiline", "true").json(filePath)
      println(statesDF.show)
    }

}








