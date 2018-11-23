package com.spark2dev101.sakila.lab.mysql

import java.util.Properties
import org.apache.spark.sql.functions._

import org.apache.spark.sql.SparkSession

object Query1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark Sakila")
      .getOrCreate()

      // Query and answer:
      //1.Which actors have the first name 'Scarlett'?
      //select actor_id, first_name, last_name from actor where first_name = 'Scarlett';

    val url = "jdbc:mysql://127.0.0.1:3306"
    val table = "sakila.actor"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "password")

    Class.forName("com.mysql.jdbc.Driver")

    val actorDF = spark.read.jdbc(url, table, properties)
    actorDF.select("actor_id","first_name", "last_name").where("first_name = 'Scarlett'").show()

  }
}
