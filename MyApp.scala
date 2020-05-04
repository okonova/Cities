package com.inese.cities
import org.apache.spark.sql.SparkSession

object MyApp {
  def main(args: Array[String]) = {

    println(s"Starting our App with ${args.length} arguments")

    if (args.length >= 2) {

      val spark = init(Array("Something", "Something"))
      println(s"Our first argument is ${args(0)}")


      TextProcessingModule.getCities(args(0), spark)


      TextProcessingModule.getStates(args(1), spark)


      DataTransform.combineData(args(1), spark)


    }

    println("All done")

    def init(configArguments: Array[String]): SparkSession = {
      val spark = SparkSession.builder.appName("Simple Application").config("spark.master", "local").getOrCreate()
      spark
    }

  }
}
