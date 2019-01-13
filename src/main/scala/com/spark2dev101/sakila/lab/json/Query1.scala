package com.spark2dev101.sakila.lab.json

import java.util.Properties
import org.apache.spark.sql.functions._

import com.spark2dev101.sakila.common.Constants.{COMMON, SPARK}
import org.apache.spark.sql.SparkSession

object Query1 extends App {
  val spark = SparkSession.builder()
    .master(SPARK.MASTER)
    .appName(SPARK.APP_NAME)
    .getOrCreate()


  val actorDF = spark.read
    .option("inferSchema","true")
    .option("multiLine", true)
    .option("mode", "PERMISSIVE")
    .json(COMMON.PATH + "/actor.js")
    //actorDF.select("actor_id","first_name", "last_name").filter(col("first_name") === "Scarlet").show()


}
