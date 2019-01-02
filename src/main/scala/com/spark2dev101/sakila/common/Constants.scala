package com.spark2dev101.sakila.common

import java.util.Properties

object Constants {

  object MYSQL {

    final val JDBC = "mysql.jdbc"
    final val actor_table = "sakila.actor"
    final val film_actor_table = "sakila.film_actor"
    final val inventory_table = "sakila.inventory"
    final val film_table = "sakila.film"
    final val rental_table = "sakila.rental"
    final val film_category = "sakila.film_category"
    final val USER = "mysql.user"
    final val PASSWORD = "mysql.passwd"
    final val driver = "com.mysql.jdbc.Driver"


  }

  object SPARK {

      final val master = "local[2]"

  }
}
