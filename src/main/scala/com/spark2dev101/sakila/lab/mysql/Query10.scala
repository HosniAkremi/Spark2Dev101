package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query10  extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Spark Sakila")
    .getOrCreate()

  // Query and answer:
  //10.What is that average running time of all the films in the sakila DB?
  //select AVG(length) as average_duration_in_minutes from film;

  val url = "jdbc:mysql://127.0.0.1:3306"
  val filmTable = "sakila.film"
  val properties = new Properties()
  properties.put("user", "root")
  properties.put("password", "password")

  Class.forName("com.mysql.jdbc.Driver")

  val filmDF = spark.read.jdbc(url, filmTable, properties)
  val df = filmDF
    .agg(avg("length").as("Avg_duration_in_min"))
    df.show()

}
