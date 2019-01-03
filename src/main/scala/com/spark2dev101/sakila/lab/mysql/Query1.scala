package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import org.apache.spark.sql.functions._
import com.spark2dev101.sakila.DataModel.Actor
import org.apache.spark.sql.SparkSession
import com.spark2dev101.sakila.common.Constants.{MYSQL, SPARK, COMMON}
import com.typesafe.config._

object Query1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master(SPARK.MASTER)
      .appName(SPARK.APP_NAME)
      .getOrCreate()


    val config = ConfigFactory.load()

      // Query and answer:
      //1.Which actors have the first name 'Scarlett'?
      //select actor_id, first_name, last_name from actor where first_name = 'Scarlett';

    val url = config.getString(MYSQL.JDBC)
    val table = MYSQL.ACTOR_TABLE
    val properties = new Properties()
    properties.put(COMMON.USER, config.getString(MYSQL.USER))
    properties.put(COMMON.PASSWORD, config.getString(MYSQL.PASSWORD))

    Class.forName(MYSQL.DRIVER)

    val actorDF = spark.read.jdbc(url, table, properties)
    actorDF.select(Actor.actor_id.column,Actor.first_name.column, Actor.last_name.column)
      .filter(Actor.first_name.column === lit("Scarlett")).show()

  }
}
