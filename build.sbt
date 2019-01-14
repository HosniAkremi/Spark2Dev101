
name := "spark-boot-starter"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.2.0"
libraryDependencies += "mysql" % "mysql-connector-java" % "5.1.44"
libraryDependencies += "com.typesafe" % "config" % "1.2.1"


libraryDependencies += "MrPowers" % "spark-fast-tests" % "2.2.0_0.5.0" % "test"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

fork in Test := true
javaOptions ++= Seq("-Xms512M", "-Xmx2048M", "-XX:+CMSClassUnloadingEnabled")



spDependencies += "mrpowers/spark-daria:2.2.0_0.12.0"



