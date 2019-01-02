package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import com.spark2dev101.sakila.DataModel.Actor
import com.spark2dev101.sakila.common.Constants.{MYSQL, SPARK}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object Query4 extends App {

    val spark = SparkSession.builder()
      .master(SPARK.master)
      .appName("Spark Sakila")
      .getOrCreate()

    // Query and answer:
    //4.Which last names are not repeated?
    //select last_name from actor group by last_name having count(*) = 1;
    val config = ConfigFactory.load()
    val url = config.getString(MYSQL.JDBC)
    val table = MYSQL.actor_table
    val properties = new Properties()
    properties.put("user",config.getString(MYSQL.USER))
    properties.put("password", config.getString(MYSQL.PASSWORD))

    Class.forName(MYSQL.driver)

    val actorDF = spark.read.jdbc(url, table, properties)
    val df = actorDF.groupBy(Actor.last_name.column).count().distinct()
    df.filter("count = 1").select(Actor.last_name.column).show()



}
