package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object Query3 extends App {

    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark Sakila")
      .getOrCreate()

     // Query and answer:
    //3.How many distinct actors last names are there?
    //select count(DISTINCT last_name) from actor;


    val url = "jdbc:mysql://127.0.0.1:3306"
    val table = "sakila.actor"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "password")

    Class.forName("com.mysql.jdbc.Driver")

    val actorDF = spark.read.jdbc(url, table, properties)
    actorDF.groupBy("last_name").count().distinct().show()



}
