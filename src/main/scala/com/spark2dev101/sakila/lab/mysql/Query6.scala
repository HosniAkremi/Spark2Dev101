package com.spark2dev101.sakila.lab.mysql

import java.util.Properties
import org.apache.spark.sql.functions._


import org.apache.spark.sql.SparkSession

object Query6 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Spark Sakila")
    .getOrCreate()

  // Query and answer:
  //6.Which actor has appeared in the most films?
  //select a.actor_id,a.first_name,a.last_name ,count(*) as film_appearance
  // from actor a join film_actor f on a.actor_id = f.actor_id
  // group by a.actor_id order by film_appearance desc limit 1

  val url = "jdbc:mysql://127.0.0.1:3306"
  val actorTable = "sakila.actor"
  val filmActorTable = "sakila.film_actor"
  val properties = new Properties()
  properties.put("user", "root")
  properties.put("password", "password")

  Class.forName("com.mysql.jdbc.Driver")

  val actorDF = spark.read.jdbc(url, actorTable, properties)
  val filmActorDF = spark.read.jdbc(url, filmActorTable, properties)
  val df = actorDF.join(filmActorDF, "actor_id")
    .groupBy("actor_id", "first_name", "last_name")
    .agg(count("*").as("film_appearance"))
    .orderBy(desc("film_appearance"))
  df.show(1)


}
