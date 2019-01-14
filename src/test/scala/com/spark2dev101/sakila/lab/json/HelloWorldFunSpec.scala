package com.spark2dev101.sakila.lab.json


import com.github.mrpowers.spark.fast.tests.DataFrameComparer
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.scalatest.FunSpec

class DrySpec
  extends FunSpec
    with DataFrameComparer
    with SparkSessionTestWrapper {

  import spark.implicits._

  it("compare dataframe to itself") {

    val sourceDF = Seq(
      ("pari"),
      ("hosni")
    ).toDF("name")

    val actualDF = sourceDF

    assertSmallDataFrameEquality(actualDF, sourceDF)

  }

}
