package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import com.spark2dev101.sakila.DataModel
import com.spark2dev101.sakila.common.Constants.{MYSQL, SPARK}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession

object Query6 extends App {
  val spark = SparkSession.builder()
    .master(SPARK.master)
    .appName("Spark Sakila")
    .getOrCreate()

  // Query and answer:
  //6.Which actor has appeared in the most films?
  //select a.actor_id,a.first_name,a.last_name ,count(*) as film_appearance
  // from actor a join film_actor f on a.actor_id = f.actor_id
  // group by a.actor_id order by film_appearance desc limit 1
  val config = ConfigFactory.load()
  val url = config.getString(MYSQL.JDBC)
  val actorTable = MYSQL.actor_table
  val filmActorTable = MYSQL.film_actor_table
  val properties = new Properties()
  properties.put("user", config.getString(MYSQL.USER))
  properties.put("password", config.getString(MYSQL.PASSWORD))

  Class.forName(MYSQL.driver)

  val actorDF = spark.read.jdbc(url, actorTable, properties)
  val filmActorDF = spark.read.jdbc(url, filmActorTable, properties)
  val df = actorDF.join(filmActorDF, DataModel.Film_Actor.actor_id.column)
    .groupBy(col("actor_id"),col("first_name"), col("last_name"))
    .agg(count("*").as("film_appearance"))
    .orderBy(desc("film_appearance"))
  df.show(1)


}
