CREATE TABLE sales(
   customer_id varchar(1),
   order_date date,
   product_id integer,
   foreign key(customer_id) references members (customer_id),
   foreign key(product_id) references menu(product_id)
)


