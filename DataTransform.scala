package com.inese.cities

import org.apache.spark.sql.SparkSession

object DataTransform {
  def combineData (filePath:String, spark: SparkSession): Unit = {

    println ("Combining both spark DFs, dropping duplicates and creating new spark DF.")

    val citiesDF = spark.read.option("header", "true").csv("C:\\Users\\merlins\\Downloads\\states.csv")

    val statesDF = spark.read.option("multiline", "true").json(filePath)

    val joinedDF = citiesDF.join(
      statesDF
      , citiesDF("city") === statesDF("city")
      , "inner")

    val newData = joinedDF.dropDuplicates("city").select(
      citiesDF("city")
      , citiesDF("state")
      , statesDF("state_abbrev"))
    println (newData.show)


    println ("Saving combined data as new csv file.")

    newData.coalesce(1)
      .write
      .option("header","true")
      .option("sep",",")
      .mode("overwrite")
      .csv("C:\\Users\\merlins\\Downloads\\newData")


  }


}
