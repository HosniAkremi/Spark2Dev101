package com.spark2dev101.sakila.lab.mysql

import java.util.Properties

import com.spark2dev101.sakila.DataModel.Actor
import com.spark2dev101.sakila.common.Constants.{COMMON, MYSQL, SPARK}
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.SparkSession

object Query4 extends App {

    val spark = SparkSession.builder()
      .master(SPARK.MASTER)
      .appName(SPARK.APP_NAME)
      .getOrCreate()

    // Query and answer:
    //4.Which last names are not repeated?
    //select last_name from actor group by last_name having count(*) = 1;
    val config = ConfigFactory.load()
    val url = config.getString(MYSQL.JDBC)
    val table = MYSQL.ACTOR_TABLE
    val properties = new Properties()
    properties.put(COMMON.USER,config.getString(MYSQL.USER))
    properties.put(COMMON.PASSWORD, config.getString(MYSQL.PASSWORD))

    Class.forName(MYSQL.DRIVER)

    val actorDF = spark.read.jdbc(url, table, properties)
    val df = actorDF.groupBy(Actor.last_name.column).count().distinct()
    df.filter("count = 1").select(Actor.last_name.column).show()



}
