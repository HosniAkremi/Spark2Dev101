package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import com.spark2dev101.sakila.DataModel._
import com.spark2dev101.sakila.common.Constants.{COMMON, MYSQL, SPARK}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object Query3 extends App {

    val spark = SparkSession.builder()
      .master(SPARK.MASTER)
      .appName(SPARK.APP_NAME)
      .getOrCreate()

     // Query and answer:
    //3.How many distinct actors last names are there?
    //select count(DISTINCT last_name) from actor;

    val config = ConfigFactory.load()
    val url = config.getString(MYSQL.JDBC)
    val table = MYSQL.ACTOR_TABLE
    val properties = new Properties()
    properties.put(COMMON.USER, config.getString(MYSQL.USER))
    properties.put(COMMON.PASSWORD, config.getString(MYSQL.PASSWORD))

    Class.forName(MYSQL.DRIVER)

    val actorDF = spark.read.jdbc(url, table, properties)
    actorDF.groupBy(Actor.last_name.column).count().distinct().show()



}
