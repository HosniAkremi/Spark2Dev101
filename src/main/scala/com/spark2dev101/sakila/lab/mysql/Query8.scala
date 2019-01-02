package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import com.spark2dev101.sakila.common.Constants.{MYSQL, SPARK}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
object Query8 extends App {
  val spark = SparkSession.builder()
    .master(SPARK.master)
    .appName("Spark Sakila")
    .getOrCreate()

  // Query and answer:
  //8.Insert a record to represent Mary Smith renting 'Academy Dinosaur' from Mike Hillyer at Store 1 today .??
  //insert into rental (rental_date, inventory_id, customer_id, staff_id) values (NOW(), 1, 1, 1);

  val config = ConfigFactory.load()
  val url = config.getString(MYSQL.JDBC)
  val rentalTable = MYSQL.rental_table
  val properties = new Properties()
  properties.put("user", config.getString(MYSQL.USER))
  properties.put("password", config.getString(MYSQL.PASSWORD))

  Class.forName(MYSQL.driver)
  val rentalDF = spark.read.jdbc(url, rentalTable, properties)
  rentalDF.createOrReplaceTempView("RT")
  spark.sql("insert into RT (rental_date, inventory_id, customer_id, staff_id) values (NOW(), 1, 1, 1)").show()

    //.write
    //.mode(SaveMode.Overwrite) // <--- Overwrite the existing table
    //.jdbc(url, rentalTable, properties)



}
