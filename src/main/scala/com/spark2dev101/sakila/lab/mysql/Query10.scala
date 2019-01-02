package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import com.spark2dev101.sakila.DataModel
import com.spark2dev101.sakila.common.Constants.{MYSQL, SPARK}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query10  extends App {
  val spark = SparkSession.builder()
    .master(SPARK.master)
    .appName("Spark Sakila")
    .getOrCreate()

  // Query and answer:
  //10.What is that average running time of all the films in the sakila DB?
  //select AVG(length) as average_duration_in_minutes from film;

  val config = ConfigFactory.load()
  val url = config.getString(MYSQL.JDBC)
  val filmTable = MYSQL.film_table
  val properties = new Properties()
  properties.put("user", config.getString(MYSQL.USER))
  properties.put("password", config.getString(MYSQL.PASSWORD))

  Class.forName(MYSQL.driver)

  val filmDF = spark.read.jdbc(url, filmTable, properties)
  val df = filmDF
    .agg(avg(DataModel.Film.length.column).as("Avg_duration_in_min"))
    df.show()

}
