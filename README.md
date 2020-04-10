# Spark2Dev101
This document specifies the scope the project and deliverables which are the translation of below sql queries to Scala Spark code.
Now there are 3 types of datasources, each will have their own README.txt file.
Below are the queries from the Sakila learning purpose database that are selected for this project:

1.Which actors have the first name 'Scarlett'?

select actor_id, first_name, last_name from actor where first_name = 'Scarlett';

2. Which actors have the last name 'Johansson'

select actor_id, first_name, last_name from actor where last_name = 'Johansson';

3.  How many distinct actors last names are there?

select count(DISTINCT last_name) from actor;

4.	Which last names are not repeated?

select last_name from actor group by last_name having count(*) = 1; 

5.	Which last names appear more than once?
select last_name, count(1) from actor group by last_name having count(1) > 1;

6.	Which actor has appeared in the most films?

select a.actor_id,a.first_name,a.last_name ,count(*) as film_appearance from actor a join film_actor f on a.actor_id = f.actor_id group by a.actor_id order by film_appearance desc limit 1

7.	Is 'Academy Dinosaur' available for rent from Store 1?
select  case when count(1) > 0 then 'YES' else 'NO' end as STATUS from inventory i
join store s on i.store_id = s.store_id
join film f on i.film_id = f.film_id
join rental r on i.inventory_id = r.inventory_id
where f.title = 'Academy Dinosaur' and s.store_id = 1
and return_date IS NOT NULL

8. Insert a record to represent Mary Smith renting 'Academy Dinosaur' from Mike Hillyer at Store 1 today .??

insert into rental (rental_date, inventory_id, customer_id, staff_id)
values (NOW(), 1, 1, 1);


9. When is 'Academy Dinosaur' due?

\! echo :add the rental duration to the rental date.

select rental_date,
       rental_date + interval
                   (select rental_duration from film where film_id = 1) day
                   as due_date
from rental
      where rental_id = (select rental_id from rental order by rental_id desc limit 1);

10. What is that average running time of all the films in the sakila DB?

select AVG(length) as average_duration_in_minutes from film; 

11.	What is the average running time of films by category?

select c.category_id, AVG(f.length) as average_duration_per_cat_in_minutes from film f join film_category c on f.film_id = c.film_id group by c.category_id ;

12. Why does this query return the empty set?

select * from film natural join inventory;
\! echo values in film_id from film table are not matching to film_id from inventory table
