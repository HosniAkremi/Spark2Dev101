package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import org.apache.spark.sql.functions._
import com.spark2dev101.sakila.DataModel.Actor
import org.apache.spark.sql.SparkSession
import com.spark2dev101.sakila.common.Constants.{MYSQL, SPARK}
import com.typesafe.config._

object Query1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master(SPARK.master)
      .appName("Spark Sakila")
      .getOrCreate()


    val config = ConfigFactory.load()

      // Query and answer:
      //1.Which actors have the first name 'Scarlett'?
      //select actor_id, first_name, last_name from actor where first_name = 'Scarlett';

    val url = config.getString(MYSQL.JDBC)
    val table = MYSQL.actor_table
    val properties = new Properties()
    properties.put("user", config.getString(MYSQL.USER))
    properties.put("password", config.getString(MYSQL.PASSWORD))

    Class.forName(MYSQL.driver)

    val actorDF = spark.read.jdbc(url, table, properties)
    actorDF.select(Actor.actor_id.column,Actor.first_name.column, Actor.last_name.column)
      .filter(Actor.first_name.column === lit("Scarlett")).show()

  }
}
