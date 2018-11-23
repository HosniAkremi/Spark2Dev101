package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import org.apache.spark.sql.SparkSession

object Query4 extends App {

    val spark = SparkSession.builder()
      .master("local")
      .appName("Spark Sakila")
      .getOrCreate()

    // Query and answer:
    //4.Which last names are not repeated?
    //select last_name from actor group by last_name having count(*) = 1;

    val url = "jdbc:mysql://127.0.0.1:3306"
    val table = "sakila.actor"
    val properties = new Properties()
    properties.put("user", "root")
    properties.put("password", "password")

    Class.forName("com.mysql.jdbc.Driver")

    val actorDF = spark.read.jdbc(url, table, properties)
    val df = actorDF.groupBy("last_name").count().distinct()
    df.filter("count = 1").select("last_name").show()



}
