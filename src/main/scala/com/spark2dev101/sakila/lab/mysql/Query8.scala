package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
object Query8 extends App {
  val spark = SparkSession.builder()
    .master("local")
    .appName("Spark Sakila")
    .getOrCreate()

  // Query and answer:
  //8.Insert a record to represent Mary Smith renting 'Academy Dinosaur' from Mike Hillyer at Store 1 today .??
  //insert into rental (rental_date, inventory_id, customer_id, staff_id) values (NOW(), 1, 1, 1);

  val url = "jdbc:mysql://127.0.0.1:3306"
  val rentalTable = "sakila.rental"
  val properties = new Properties()
  properties.put("user", "root")
  properties.put("password", "password")

  Class.forName("com.mysql.jdbc.Driver")
  val rentalDF = spark.read.jdbc(url, rentalTable, properties)
  rentalDF.createOrReplaceTempView("RT")
  spark.sql("insert into RT (rental_date, inventory_id, customer_id, staff_id) values (NOW(), 1, 1, 1)").show()

    //.write
    //.mode(SaveMode.Overwrite) // <--- Overwrite the existing table
    //.jdbc(url, rentalTable, properties)



}
