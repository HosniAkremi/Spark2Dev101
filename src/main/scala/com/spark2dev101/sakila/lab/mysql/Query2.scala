package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import com.spark2dev101.sakila.common.Constants.SPARK
import org.apache.spark.sql.functions._
import com.spark2dev101.sakila.DataModel._
import com.spark2dev101.sakila.common.Constants.{MYSQL, SPARK, COMMON}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object Query2 extends App {

    val spark = SparkSession.builder()
      .master(SPARK.MASTER)
      .appName(SPARK.APP_NAME)
      .getOrCreate()

    // Query and answer:
    //2.Which actors have the last name 'Johansson'
   // select actor_id, first_name, last_name from actor where last_name = 'Johansson';

    val config = ConfigFactory.load()
    val url = config.getString(MYSQL.JDBC)
    val table = MYSQL.ACTOR_TABLE
    val properties = new Properties()
    properties.put(COMMON.USER, config.getString(MYSQL.USER))
    properties.put(COMMON.PASSWORD, config.getString(MYSQL.PASSWORD))

  Class.forName(MYSQL.DRIVER)

    val actorDF = spark.read.jdbc(url, table, properties)
    actorDF
      .select(Actor.actor_id.column,Actor.first_name.column, Actor.last_name.column)
      .filter(Actor.last_name.column === lit("Johansson")).show()

}
