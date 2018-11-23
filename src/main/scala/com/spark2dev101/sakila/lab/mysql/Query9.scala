//package com.spark2dev101.sakila.lab.mysql
//
//import java.util.Properties
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{count, desc}
//
//object Query9 extends App {
//  val spark = SparkSession.builder()
//    .master("local")
//    .appName("Spark Sakila")
//    .getOrCreate()
//  // Query and answer:
//  // Query was discarded
//
//  val url = "jdbc:mysql://127.0.0.1:3306"
//  val rentalTable = "sakila.rental"
//  val filmTable = "sakila.film"
//  val properties = new Properties()
//  properties.put("user", "root")
//  properties.put("password", "password")
//
//  Class.forName("com.mysql.jdbc.Driver")
//
//  val rentalDF = spark.read.jdbc(url, rentalTable, properties)
//  val filmDF = spark.read.jdbc(url, filmTable, properties)
//  rentalDF.createOrReplaceTempView("rental")
//  filmDF.createOrReplaceTempView("film")
//  spark.sql("select rental_date, rental_date + interval" +
//    " (select rental_duration from film where film_id = 1) day  as due_date " +
//    "from rental where rental_id = (select rental_id from rental order by rental_id desc limit 1)").show()
//}
