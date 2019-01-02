package com.spark2dev101.sakila

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DataType, IntegerType, StringType}

object DataModel {

  case class field(name: String, dtype: DataType) {
    def column = col(name)
  }

  object Actor {

    def tablename: String = "actor"

    val actor_id = field("actor_id", IntegerType)
    val first_name = field("first_name", StringType)
    val last_name = field("last_name", StringType)
    val last_update = field("last_update", StringType)
  }

  object Address {

    def tablename: String = "address"

    val address_id = field("address_id", StringType)
    val address = field("address", StringType)
    val address2 = field("address2", StringType)
    val district = field("district", StringType)
    val city_id = field("city_id", IntegerType)
    val postal_code = field("postal_code", StringType)
    val phone = field("phone", StringType)
    val last_update = field("last_update", StringType)
  }

  object Country {

    def tablename: String = "country"

    val country_id = field("country_id", IntegerType)
    val country = field("country", StringType)
    val last_update = field("last_update", StringType)

  }

  object City {

    def tablename: String = "city"

    val city_id = field("city_id", StringType)
    val city = field("city", StringType)
    val country_id = field("country_id", StringType)
    val last_update = field("last_update", StringType)

  }

  object Customer {

    def tablename: String = "customer"

    val customer_id = field("customer_id", IntegerType)
    val store_id = field("store_id", IntegerType)
    val first_name = field("first_name", StringType)
    val last_name = field("last_name", StringType)
    val email = field("email", StringType)
    val active = field("active", IntegerType)
    val create_date = field("create_date", StringType)
    val last_update = field("last_update", StringType)
  }

  object Rental {

    def tablename: String = "customer"

    val rental_id = field("rental_id", IntegerType)
    val rental_date = field("store_id", StringType)
    val inventory_id = field("inventory_id", IntegerType)
    val customer_id = field("customer_id", IntegerType)
    val return_date = field("return_date", StringType)
    val staff_id = field("staff_id", IntegerType)
    val last_update = field("last_update", StringType)
  }

  object Payment {

    def tablename: String = "payment"

    val payment_id = field("payment_id", IntegerType)
    val customer_id = field("customer_id", IntegerType)
    val staff_id = field("staff_id", IntegerType)
    val rental_id = field("rental_id", IntegerType)
    val amount = field("amount", IntegerType)
    val payment_date = field("payment_date", IntegerType)
    val last_update = field("last_update", StringType)
  }

  object Inventory {

    def tablename: String = "inventory"

    val inventory_id = field("payment_id", IntegerType)
    val film_id = field("film_id", IntegerType)
    val store_id = field("staff_id", IntegerType)
    val last_update = field("last_update", StringType)
  }

  object Store {

    def tablename: String = "store"

    val store_id = field("store_id", IntegerType)
    val manager_staff_id = field("manager_staff_id", IntegerType)
    val address_id = field("address_id", IntegerType)
    val last_update = field("last_update", StringType)
  }

  object Staff {

    def tablename: String = "store"

    val staff_id = field("staff_id", IntegerType)
    val first_name = field("first_name", StringType)
    val last_name = field("last_name", StringType)
    val address_id = field("address_id", IntegerType)
    val picture = field("picture", IntegerType)
    val email = field("email", StringType)
    val store_id = field("store_id", IntegerType)
    val active = field("active", IntegerType)
    val username = field("username", StringType)
    val password = field("password", StringType)
    val last_update = field("last_update", StringType)

  }

  object Language {

    def tablename: String = "language"

    val language_id = field("language_id", IntegerType)
    val name = field("name", StringType)
    val last_update = field("last_update", StringType)
  }

  object Film_Category {

    def tablename: String = "film_category"

    val film_id = field("film_id", IntegerType)
    val category_id = field("name", IntegerType)
    val last_update = field("last_update", StringType)
  }

  object Category {

    def tablename: String = "category"

    val category_id = field("category_id", IntegerType)
    val name = field("name", StringType)
    val last_update = field("last_update", StringType)
  }

  object Film_Actor {

    def tablename: String = "film_actor"

    val actor_id = field("actor_id", IntegerType)
    val film_id = field("film_id", IntegerType)
    val last_update = field("last_update", StringType)
  }

  object Film_Text {

    def tablename: String = "film_text"

    val film_id = field("film_id", IntegerType)
    val title = field("title", StringType)
    val description = field("description", StringType)
  }

  object Film {

    def tablename: String = "film"

    val film_id = field("film_id", IntegerType)
    val title = field("title", StringType)
    val description = field("description", StringType)
    val release_year = field("release_year", IntegerType)
    val language_id = field("language_id", IntegerType)
    val original_language_id = field("original_language_id", IntegerType)
    val rental_duration = field("rental_duration", IntegerType)
    val rental_rate = field("rental_rate", IntegerType)
    val length = field("length", StringType)
    val replacement_cost = field("replacement_cost", StringType)
    val rating = field("rating", StringType)
    val special_featues = field("special_features", StringType)
    val last_update = field("last_update", StringType)

  }

}