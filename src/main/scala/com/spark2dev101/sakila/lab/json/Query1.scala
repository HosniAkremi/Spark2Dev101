package com.spark2dev101.sakila.lab.json

import java.util.Properties

import org.apache.spark.sql.SparkSession

object Query1 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Spark Sakila")
    .getOrCreate()


  val actorDF = spark.read
    .option("inferSchema","true").json("file:///Users/hosniakremi/Documents/Spark 2 DEV 101 Learning Plan Project/sakila-db_json/actor.js")
  //actorDF.select("actor_id","first_name", "last_name").where("first_name = 'Scarlett'").show()
actorDF.show()

}
