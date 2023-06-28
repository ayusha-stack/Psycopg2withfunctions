--What is the total amount each customer spent at the restaurant?

SELECT members.customer_id , SUM(menu.price) as total_amount
FROM members
JOIN SALES ON members.customer_id=sales.customer_id
JOIN menu  ON  sales.product_id= menu.product_id
GROUP BY members.customer_id;

--How many days has each customer visited the restaurant?

SELECT customer_id, COUNT(DISTINCT order_date) AS total_visit
FROM sales
GROUP BY customer_id;
;

--What was the first item from the menu purchased by customer A?
SELECT menu.product_name, sales.order_date AS first_purchase_date
    FROM sales
    JOIN menu ON sales.product_id = menu.product_id
    WHERE sales.customer_id = 'A'
    AND sales.order_date = (
        SELECT MIN(order_date)
        FROM sales
        WHERE customer_id = 'A'
    );

--What is the most purchased item on the menu and how many times was it purchased by all customers?

SELECT m.product_name, COUNT(*) AS total_purchases
FROM sales s
JOIN menu m ON s.product_id = m.product_id
GROUP BY m.product_name
ORDER BY total_purchases DESC
LIMIT 1;

--Which item was the most popular for each customer?

SELECT subquery.customer_id, m.product_name AS most_popular_item
FROM
  (
    SELECT customer_id, product_id,
      COUNT(*) AS purchase_count,
      ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY COUNT(*) DESC) AS rn
    FROM sales
    GROUP BY customer_id, product_id
  ) AS subquery
  INNER JOIN menu m ON subquery.product_id = m.product_id
WHERE subquery.rn = 1;










