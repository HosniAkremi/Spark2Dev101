package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object Query2 extends App {

    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark Sakila")
      .getOrCreate()

    // Query and answer:
    //2.Which actors have the last name 'Johansson'
   // select actor_id, first_name, last_name from actor where last_name = 'Johansson';

    val url = "jdbc:mysql://127.0.0.1:3306"
    val table = "sakila.actor"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "password")

    Class.forName("com.mysql.jdbc.Driver")

    val actorDF = spark.read.jdbc(url, table, properties)
    actorDF.select("actor_id","first_name", "last_name").where("last_name = 'Johansson'").show()

}
