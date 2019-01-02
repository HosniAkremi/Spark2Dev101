package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import com.spark2dev101.sakila.DataModel
import com.spark2dev101.sakila.common.Constants.{MYSQL, SPARK}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Query11 extends App {
  val spark = SparkSession.builder()
    .master(SPARK.master)
    .appName("Spark Sakila")
    .getOrCreate()

  // Query and answer:
  //11.What is the average running time of films by category?
  //select c.category_id, AVG(f.length) as average_duration_per_cat_in_minutes
  // from film f join film_category c on f.film_id = c.film_id group by c.category_id ;

  val config = ConfigFactory.load()
  val url = config.getString(MYSQL.JDBC)
  val filmTable = MYSQL.film_table
  val film_catTable = MYSQL.film_category
  val properties = new Properties()
  properties.put("user", config.getString(MYSQL.USER))
  properties.put("password", config.getString(MYSQL.PASSWORD))

  Class.forName(MYSQL.driver)

  val filmDF = spark.read.jdbc(url, filmTable, properties)
  val filmCatDF = spark.read.jdbc(url, film_catTable, properties)
  val df = filmDF.join(filmCatDF, DataModel.Film.film_id.column)
    .groupBy(DataModel.Film_Category.category_id.column)
    .agg(avg(DataModel.Film.length.column).as("average_duration_per_category_in_minutes"))
  df.show()
}
