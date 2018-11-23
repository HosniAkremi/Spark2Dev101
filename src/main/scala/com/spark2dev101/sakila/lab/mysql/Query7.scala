package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, desc}

object Query7 extends  App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Spark Sakila")
    .getOrCreate()

  // Query and answer:
  //7.Is 'Academy Dinosaur' available for rent from Store 1?
  //select  case when count(1) > 0 then 'YES' else 'NO' end as STATUS from inventory i
  //join store s on i.store_id = s.store_id
  //join film f on i.film_id = f.film_id
  //join rental r on i.inventory_id = r.inventory_id
  //where f.title = 'Academy Dinosaur' and s.store_id = 1
  //and return_date IS NOT NULL

  val url = "jdbc:mysql://127.0.0.1:3306"
  val inventoryTable = "sakila.actor"
  val filmTable = "sakila.film"
  val properties = new Properties()
  properties.put("user", "root")
  properties.put("password", "password")

  Class.forName("com.mysql.jdbc.Driver")

  val inventoryDF = spark.read.jdbc(url, inventoryTable, properties)
  val filmDF = spark.read.jdbc(url, filmTable, properties)
  val df = filmDF.join(inventoryDF, "film_id")
    .groupBy("actor_id", "first_name", "last_name")
    .agg(count("*").as("film_appearance"))
    .orderBy(desc("film_appearance"))
  df.show(1)


  /*def isFilmAvailable(x: Int): String = x match {
    case 0 => "film is not available"
    case _ => "film is available"

  }*/
}
