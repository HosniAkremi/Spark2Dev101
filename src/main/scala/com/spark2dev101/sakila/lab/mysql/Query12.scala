//package com.spark2dev101.sakila.lab.mysql
//
//import java.util.Properties
//
//import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{count, desc}
//
//object Query12 extends App {
//  val spark = SparkSession.builder()
//    .master("local")
//    .appName("Spark Sakila")
//    .getOrCreate()
//     Query and answer:
//     Query was discarded
//
//  val url = "jdbc:mysql://127.0.0.1:3306"
//  val inventoryTable = "sakila.inventory"
//  val filmTable = "sakila.film"
//  val properties = new Properties()
//  properties.put("user", "root")
//  properties.put("password", "password")
//
//  Class.forName("com.mysql.jdbc.Driver")
//
//  val inventoryDF = spark.read.jdbc(url, inventoryTable, properties)
//  val filmDF = spark.read.jdbc(url, filmTable, properties)
//  inventoryDF.createOrReplaceTempView("iT")
//  filmDF.createOrReplaceTempView("fT")
//  val df = spark.sql("select * from fT natural join iT")
//  df.show()
//  println("==========this query return the empty set because" +
//    " values in film_id from film table are not matching to film_id from inventory table=======")
//
//}
