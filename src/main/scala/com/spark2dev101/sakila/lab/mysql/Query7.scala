package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import com.spark2dev101.sakila.DataModel
import com.spark2dev101.sakila.common.Constants.{MYSQL, SPARK}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{count, desc}

object Query7 extends  App {
  val spark = SparkSession.builder()
    .master(SPARK.master)
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

  val config = ConfigFactory.load()
  val url = config.getString(MYSQL.JDBC)
  val inventoryTable = MYSQL.inventory_table
  val filmTable = MYSQL.film_table
  val properties = new Properties()
  properties.put("user", config.getString(MYSQL.USER))
  properties.put("password", config.getString(MYSQL.PASSWORD))

  Class.forName(MYSQL.driver)

  val inventoryDF = spark.read.jdbc(url, inventoryTable, properties)
  val filmDF = spark.read.jdbc(url, filmTable, properties)
  val df = filmDF.join(inventoryDF, DataModel.Film.film_id.column)
    .groupBy("actor_id", "first_name", "last_name")
    .agg(count("*").as("film_appearance"))
    .orderBy(desc("film_appearance"))
  df.show(1)


  /*def isFilmAvailable(x: Int): String = x match {
    case 0 => "film is not available"
    case _ => "film is available"

  }*/
}
