package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query11 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Spark Sakila")
    .getOrCreate()

  // Query and answer:
  //11.What is the average running time of films by category?
  //select c.category_id, AVG(f.length) as average_duration_per_cat_in_minutes
  // from film f join film_category c on f.film_id = c.film_id group by c.category_id ;

  val url = "jdbc:mysql://127.0.0.1:3306"
  val filmTable = "sakila.film"
  val film_catTable = "sakila.film_category"
  val properties = new Properties()
  properties.put("user", "root")
  properties.put("password", "password")

  Class.forName("com.mysql.jdbc.Driver")

  val filmDF = spark.read.jdbc(url, filmTable, properties)
  val filmCatDF = spark.read.jdbc(url, film_catTable, properties)
  val df = filmDF.join(filmCatDF, "film_id")
    .groupBy("category_id")
    .agg(avg("length").as("average_duration_per_category_in_minutes"))
  df.show()
}
