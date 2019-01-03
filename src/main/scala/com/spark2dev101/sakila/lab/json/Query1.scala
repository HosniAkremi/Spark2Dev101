package com.spark2dev101.sakila.lab.json

import java.util.Properties

import com.spark2dev101.sakila.common.Constants.{COMMON, SPARK}
import org.apache.spark.sql.SparkSession

object Query1 extends App {
  val spark = SparkSession.builder()
    .master(SPARK.MASTER)
    .appName(SPARK.APP_NAME)
    .getOrCreate()

  val actorDF = spark.read.json(COMMON.PATH + "/actor.js")
  //actorDF.select("actor_id","first_name", "last_name").where("first_name = 'Scarlett'").show()
    actorDF.show()

}
