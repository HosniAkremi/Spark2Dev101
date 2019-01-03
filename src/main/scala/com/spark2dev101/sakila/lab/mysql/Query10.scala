package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import com.spark2dev101.sakila.DataModel
import com.spark2dev101.sakila.common.Constants.{COMMON, MYSQL, SPARK}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query10  extends App {
  val spark = SparkSession.builder()
    .master(SPARK.MASTER)
    .appName(SPARK.APP_NAME)
    .getOrCreate()

  // Query and answer:
  //10.What is that average running time of all the films in the sakila DB?
  //select AVG(length) as average_duration_in_minutes from film;

  val config = ConfigFactory.load()
  val url = config.getString(MYSQL.JDBC)
  val filmTable = MYSQL.FILM_TABLE
  val properties = new Properties()
  properties.put(COMMON.USER, config.getString(MYSQL.USER))
  properties.put(COMMON.PASSWORD, config.getString(MYSQL.PASSWORD))

  Class.forName(MYSQL.DRIVER)

  val filmDF = spark.read.jdbc(url, filmTable, properties)
  val df = filmDF
    .agg(avg(DataModel.Film.length.column).as("Avg_duration_in_min"))
    df.show()

}
