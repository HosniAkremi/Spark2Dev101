package com.spark2dev101.sakila.common

import java.util.Properties

object Constants {

  object COMMON {
    final val USER = "user"
    final val PASSWORD = "password"
    final val PATH = "file:///Users/hosniakremi/Documents/Spark2DEV101LearningPlanProject/sakila-db_json"
  }

  object MYSQL {
    final val JDBC = "mysql.jdbc"
    final val ACTOR_TABLE = "sakila.actor"
    final val FILM_ACTOR_TABLE = "sakila.film_actor"
    final val INVENTORY_TABLE = "sakila.inventory"
    final val FILM_TABLE = "sakila.film"
    final val RENTAL_TABLE = "sakila.rental"
    final val FILM_CATEGORY = "sakila.film_category"
    final val USER = "mysql.user"
    final val PASSWORD = "mysql.passwd"
    final val DRIVER = "com.mysql.jdbc.Driver"
  }

  object SPARK {

      final val MASTER = "local[2]"
      final val APP_NAME = "Spark Sakila"

  }
}
