CREATE SCHEMA pizza_runner;
use pizza_runner;

DROP TABLE IF EXISTS runners;
CREATE TABLE runners (
   runner_id INT,
   registration_date DATE
);

INSERT INTO runners
  (runner_id, registration_date)
VALUES
  (1, '2021-01-01'),
  (2, '2021-01-03'),
  (3, '2021-01-08'),
  (4, '2021-01-15');
  
  select * from runners;
  
  
  DROP TABLE IF EXISTS customer_orders;
  CREATE TABLE customer_orders (
  order_id INTEGER,
  customer_id INTEGER,
  pizza_id INTEGER,
  exclusions VARCHAR(4),
  extras VARCHAR(4),
  order_time TIMESTAMP
);

INSERT INTO customer_orders
  (order_id, customer_id, pizza_id, exclusions, extras, order_time)
VALUES
  ('1', '101', '1', '', '', '2020-01-01 18:05:02'),
  ('2', '101', '1', '', '', '2020-01-01 19:00:52'),
  ('3', '102', '1', '', '', '2020-01-02 23:51:23'),
  ('3', '102', '2', '', NULL, '2020-01-02 23:51:23'),
  ('4', '103', '1', '4', '', '2020-01-04 13:23:46'),
  ('4', '103', '1', '4', '', '2020-01-04 13:23:46'),
  ('4', '103', '2', '4', '', '2020-01-04 13:23:46'),
  ('5', '104', '1', 'null', '1', '2020-01-08 21:00:29'),
  ('6', '101', '2', 'null', 'null', '2020-01-08 21:03:13'),
  ('7', '105', '2', 'null', '1', '2020-01-08 21:20:29'),
  ('8', '102', '1', 'null', 'null', '2020-01-09 23:54:33'),
  ('9', '103', '1', '4', '1, 5', '2020-01-10 11:22:59'),
  ('10', '104', '1', 'null', 'null', '2020-01-11 18:34:49'),
  ('10', '104', '1', '2, 6', '1, 4', '2020-01-11 18:34:49');
  
  select * from customer_orders;
  
  DROP TABLE IF EXISTS runner_orders;
  CREATE TABLE runner_orders (
  order_id INTEGER,
  runner_id INTEGER,
  pickup_time VARCHAR(19),
  distance VARCHAR(7),
  duration VARCHAR(10),
  cancellation VARCHAR(23)
);

INSERT INTO runner_orders
  (order_id, runner_id, pickup_time, distance, duration, cancellation)
VALUES
  ('1', '1', '2020-01-01 18:15:34', '20km', '32 minutes', ''),
  ('2', '1', '2020-01-01 19:10:54', '20km', '27 minutes', ''),
  ('3', '1', '2020-01-03 00:12:37', '13.4km', '20 mins', NULL),
  ('4', '2', '2020-01-04 13:53:03', '23.4', '40', NULL),
  ('5', '3', '2020-01-08 21:10:57', '10', '15', NULL),
  ('6', '3', 'null', 'null', 'null', 'Restaurant Cancellation'),
  ('7', '2', '2020-01-08 21:30:45', '25km', '25mins', 'null'),
  ('8', '2', '2020-01-10 00:15:02', '23.4 km', '15 minute', 'null'),
  ('9', '2', 'null', 'null', 'null', 'Customer Cancellation'),
  ('10', '1', '2020-01-11 18:50:20', '10km', '10minutes', 'null');

select * from runner_orders;


DROP TABLE IF EXISTS pizza_names;
CREATE TABLE pizza_names (
  pizza_id INTEGER,
  pizza_name TEXT
);

INSERT INTO pizza_names
  (pizza_id, pizza_name)
VALUES
  (1, 'Meatlovers'),
  (2, 'Vegetarian');
  select * from pizza_names;
  
  DROP TABLE IF EXISTS pizza_recipes;
  CREATE TABLE pizza_recipes (
  pizza_id INTEGER,
  toppings TEXT
);

INSERT INTO pizza_recipes
  (pizza_id, toppings)
VALUES
  (1, '1, 2, 3, 4, 5, 6, 8, 10'),
  (2, '4, 6, 7, 9, 11, 12');
  

DROP TABLE IF EXISTS pizza_toppings;
CREATE TABLE pizza_toppings (
  topping_id INTEGER,
  topping_name TEXT
);

INSERT INTO pizza_toppings
  (topping_id, topping_name)
VALUES
  (1, 'Bacon'),
  (2, 'BBQ Sauce'),
  (3, 'Beef'),
  (4, 'Cheese'),
  (5, 'Chicken'),
  (6, 'Mushrooms'),
  (7, 'Onions'),
  (8, 'Pepperoni'),
  (9, 'Peppers'),
  (10, 'Salami'),
  (11, 'Tomatoes'),
  (12, 'Tomato Sauce');
  select * from pizza_toppings;

#====================================================================================
# Data Cleaning
#==================
UPDATE runner_orders
SET pickup_time = NULL
WHERE pickup_time = 'null' OR pickup_time = '';

UPDATE runner_orders
SET distance = NULL
WHERE distance = 'null' OR distance = '';

UPDATE runner_orders
SET duration = NULL
WHERE duration = 'null' OR duration = '';

UPDATE runner_orders
SET cancellation = NULL
WHERE cancellation = 'null' OR cancellation = '';

select * from runner_orders;
#=======================================
UPDATE runner_orders
SET distance = REPLACE(distance, 'km', '');

UPDATE runner_orders
SET distance = TRIM(distance) + 0;   -- force numeric

-- Clean duration: remove words and cast to integer
UPDATE runner_orders
SET duration = REPLACE(duration, 'minutes', '');

UPDATE runner_orders
SET duration = REPLACE(duration, 'minute', '');

UPDATE runner_orders
SET duration = REPLACE(duration, 'mins', '');

UPDATE runner_orders
SET duration = TRIM(duration) + 0;

select * from runner_orders;

#========================================

UPDATE customer_orders
SET exclusions = NULL
WHERE exclusions = 'null' OR exclusions = '';

UPDATE customer_orders
SET extras = NULL
WHERE extras = 'null' OR extras = '';

select * from customer_orders;
#====================================================
# QUESTION
#=============

# A. Pizza Metrics
#1 How many pizzas were ordered?
select count(*) as total_count from customer_orders;

#2 How many unique customer orders were made?
select count(distinct order_id) as unique_orders from customer_orders;

#3 How many successful orders were delivered by each runner?
select runner_id , count(order_id) as successful_order from runner_orders
 where cancellation is null group by runner_id;

#4 How many of each type of pizza was delivered?
select pizza_id , count(*) as pizza_delivered 
from customer_orders c join runner_orders r
on c.order_id = r.order_id 
where r. cancellation is null
group by pizza_id;


#5 How many Vegetarian and Meatlovers were ordered by each customer?
SELECT c.customer_id,
       p.pizza_name,
       COUNT(*) AS pizzas_ordered
FROM customer_orders c
JOIN pizza_names p
  ON c.pizza_id = p.pizza_id
GROUP BY c.customer_id, p.pizza_name
ORDER BY c.customer_id, p.pizza_name;


#6 What was the maximum number of pizzas delivered in a single order?
select c.order_id , count(*) as pizzas_in_order
from customer_orders c
join runner_orders r
on c.order_id = r.order_id
where r.cancellation is null
group by c.order_id
order by pizzas_in_order desc limit 1;


#7 For each customer, how many delivered pizzas had at least 1 change and how many had no changes?
SELECT c.customer_id,
       COUNT(*) AS pizzas_with_changes
FROM customer_orders c
JOIN runner_orders r
  ON c.order_id = r.order_id
WHERE r.cancellation IS NULL
  AND (c.exclusions IS NOT NULL OR c.extras IS NOT NULL)
GROUP BY c.customer_id;

#8 How many pizzas were delivered that had both exclusions and extras?
select count(*) as pizzas_with_exclusions_and_extras
from customer_orders c
join runner_orders r
where r.cancellation IS NULL
AND c.exclusions IS NOT NULL
  AND c.extras IS NOT NULL;
  
#9 What was the total volume of pizzas ordered for each hour of the day?
SELECT 
    HOUR(order_time) AS order_hour,
    COUNT(*) AS total_pizzas
FROM customer_orders c
JOIN runner_orders r 
    ON c.order_id = r.order_id
WHERE r.cancellation IS NULL
GROUP BY HOUR(order_time)
ORDER BY order_hour;

#10 What was the volume of orders for each day of the week?
select dayname(c.order_time) as day_of_week,
count(*) as total_pizzas
from customer_orders c
join runner_orders r
on c.order_id = r.order_id
where r.cancellation is null
group by dayname(order_time)
order by field(day_of_week,
    'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday');
    
    
#B. Runner and Customer Experience
#================================================
#How many runners signed up for each 1 week period? (i.e. week starts 2021-01-01)
SELECT 
    FLOOR(DATEDIFF(registration_date, '2021-01-01') / 7) + 1 AS week_number,
    COUNT(runner_id) AS runners_signed_up
FROM runners
GROUP BY week_number
ORDER BY week_number;

#What was the average time in minutes it took for each runner to arrive at the Pizza Runner HQ to pickup the order?
select r.runner_id,
round(avg(timestampdiff(minute,c.order_time,r.pickup_time)),2) as avg_arrival_time_minutes
from customer_orders c
join runner_orders r
on c.order_id = c.order_id
where r.cancellation is null
group by r.runner_id
order by r.runner_id;

#Is there any relationship between the number of pizzas and how long the order takes to prepare?
select c.order_id,
count(c.pizza_id) as number_of_pizzas,
timestampdiff(minute,c.order_time,r.pickup_time) as prep_time_minutes
from customer_orders c
join runner_orders r
on c.order_id = r.order_id
where r.cancellation is null
group by c.order_id,c.order_time,r.pickup_time
order by number_of_pizzas desc;


SELECT 
    number_of_pizzas,
    ROUND(AVG(prep_time_minutes), 2) AS avg_prep_time
FROM (
    SELECT 
        c.order_id,
        COUNT(c.pizza_id) AS number_of_pizzas,
        TIMESTAMPDIFF(MINUTE, c.order_time, r.pickup_time) AS prep_time_minutes
    FROM customer_orders c
    JOIN runner_orders r 
        ON c.order_id = r.order_id
    WHERE r.cancellation IS NULL
      AND r.pickup_time IS NOT NULL
    GROUP BY c.order_id, c.order_time, r.pickup_time
) sub
GROUP BY number_of_pizzas
ORDER BY number_of_pizzas;


#What was the average distance travelled for each customer?

SELECT  c.customer_id,
    ROUND(AVG(r.distance), 2) AS avg_distance
FROM customer_orders c
JOIN runner_orders r
    ON c.order_id = r.order_id
WHERE r.cancellation IS NULL
GROUP BY c.customer_id
ORDER BY c.customer_id;


#What was the difference between the longest and shortest delivery times for all orders?
select 
max(timestampdiff(minute,c.order_time,r.pickup_time)) -
min(timestampdiff(minute,c.order_time,r.pickup_time)) as delivery_time_diff
from customer_orders c
join runner_orders r
on c.order_id = r.order_id
where r.cancellation is null
and r.pickup_time is not null;

#What was the average speed for each runner for each delivery and do you notice any trend for these values?
SELECT 
    r.runner_id,
    r.order_id,
    ROUND(r.distance / (TIMESTAMPDIFF(MINUTE, MIN(c.order_time), r.pickup_time) / 60), 2) AS avg_speed_kmh
FROM runner_orders r
JOIN customer_orders c
    ON c.order_id = r.order_id
WHERE r.cancellation IS NULL
  AND r.distance IS NOT NULL
  AND r.pickup_time IS NOT NULL
GROUP BY r.runner_id, r.order_id, r.distance, r.pickup_time
ORDER BY r.runner_id, r.order_id;

#What is the successful delivery percentage for each runner?
SELECT 
    runner_id,
    ROUND(100 * SUM(CASE WHEN cancellation IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) AS success_percentage
FROM runner_orders
GROUP BY runner_id
ORDER BY runner_id;





#Pricing and Ratings
#===========================
#If a Meat Lovers pizza costs $12 and Vegetarian costs $10 and there were no charges for changes - how much money has Pizza Runner made so far if there are no delivery fees?
SELECT 
    SUM(
        CASE 
            WHEN pn.pizza_name = 'Meatlovers' THEN 12
            WHEN pn.pizza_name = 'Vegetarian' THEN 10
            ELSE 0
        END
    ) AS total_revenue
FROM customer_orders co
JOIN runner_orders r ON co.order_id = r.order_id
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
WHERE r.cancellation IS NULL;


#What if there was an additional $1 charge for any pizza extras?
#Add cheese is $1 extra

SELECT 
    SUM(
        CASE 
            WHEN pn.pizza_name = 'Meatlovers' THEN 12
            WHEN pn.pizza_name = 'Vegetarian' THEN 10
            ELSE 0
        END
        + 
        -- Count extras: add $1 per extra topping
        IF(co.extras IS NOT NULL AND co.extras <> '', 
           LENGTH(co.extras) - LENGTH(REPLACE(co.extras, ',', '')) + 1, 0)
    ) AS total_revenue_with_extras
FROM customer_orders co
JOIN runner_orders r ON co.order_id = r.order_id
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
WHERE r.cancellation IS NULL;




#If a Meat Lovers pizza was $12 and Vegetarian $10 fixed prices with no cost for extras and each runner is paid $0.30 per kilometre traveled - 
#how much money does Pizza Runner have left over after these deliveries?

SELECT 
    ROUND(SUM(
        CASE 
            WHEN pn.pizza_name = 'Meatlovers' THEN 12
            WHEN pn.pizza_name = 'Vegetarian' THEN 10
            ELSE 0
        END
    ), 2) AS total_revenue,
    
    ROUND(SUM(r.distance * 0.30), 2) AS total_runner_cost,
    
    ROUND(SUM(
        CASE 
            WHEN pn.pizza_name = 'Meatlovers' THEN 12
            WHEN pn.pizza_name = 'Vegetarian' THEN 10
            ELSE 0
        END
    ) - SUM(r.distance * 0.30), 2) AS net_earnings
FROM customer_orders co
JOIN runner_orders r ON co.order_id = r.order_id
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
WHERE r.cancellation IS NULL;




#===================================================================================
# Window Function
#================
# 1. Rank all runners by number of completed deliveries
SELECT
  runner_id,
  COUNT(order_id) AS completed_orders,
  RANK() OVER (ORDER BY COUNT(order_id) DESC) AS rank_position
FROM runner_orders
WHERE cancellation IS NULL
GROUP BY runner_id;

# 2. Show each runner’s delivery count and their percent of total deliveries
SELECT
  runner_id,
  COUNT(order_id) AS completed_orders,
  ROUND(100.0 * COUNT(order_id) / SUM(COUNT(order_id)) OVER (), 2) AS percent_of_total
FROM runner_orders
WHERE cancellation IS NULL
GROUP BY runner_id;

# 3 .Find the most recent order for each customer
SELECT *
FROM (
  SELECT
    customer_id,
    order_id,
    order_time,
    ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_time DESC) AS rn
  FROM customer_orders
) t
WHERE rn = 1;

#  4. Average delivery duration over last 3 runs (per runner)
SELECT
  runner_id,
  pickup_time,
  duration,
  ROUND(AVG(duration) OVER (
    PARTITION BY runner_id
    ORDER BY pickup_time
    ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
  ), 2) AS moving_avg_3
FROM runner_orders
WHERE duration IS NOT NULL
ORDER BY runner_id, pickup_time;

# 5. Cumulative deliveries per runner over time
SELECT
  runner_id,
  pickup_time,
  COUNT(order_id) OVER (
    PARTITION BY runner_id ORDER BY pickup_time
    ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
  ) AS cumulative_orders
FROM runner_orders
WHERE cancellation IS NULL AND pickup_time IS NOT NULL;

# 6. First and last order date per customer
SELECT
  customer_id,
  MIN(order_time) AS first_order,
  MAX(order_time) AS last_order
FROM customer_orders
GROUP BY customer_id;

#7. Difference in minutes between consecutive deliveries
SELECT
  runner_id,
  pickup_time,
  TIMESTAMPDIFF(
    MINUTE,
    LAG(pickup_time) OVER (PARTITION BY runner_id ORDER BY pickup_time),
    pickup_time
  ) AS minutes_diff
FROM runner_orders
WHERE pickup_time IS NOT NULL;

# 8. Top 2 most frequently ordered pizzas
SELECT * FROM (
  SELECT
    pizza_id,
    COUNT(*) AS total_orders,
    RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_pos
  FROM customer_orders
  GROUP BY pizza_id
) t
WHERE rank_pos <= 2;

#9. Running total of pizzas ordered per day
SELECT
  DATE(order_time) AS order_date,
  COUNT(*) AS daily_orders,
  SUM(COUNT(*)) OVER (ORDER BY DATE(order_time)) AS running_total
FROM customer_orders
GROUP BY DATE(order_time)
ORDER BY order_date;

#10. Gap in days between two orders (per customer)
SELECT
  customer_id,
  order_id,
  order_time,
  LAG(order_time) OVER (PARTITION BY customer_id ORDER BY order_time) AS prev_order,
  EXTRACT(DAY FROM (order_time - LAG(order_time) OVER (PARTITION BY customer_id ORDER BY order_time))) AS days_gap
FROM customer_orders;

#11. Delivery duration percentile (per runner)
SELECT
  runner_id,
  order_id,
  duration,
  PERCENT_RANK() OVER (PARTITION BY runner_id ORDER BY duration) AS duration_percentile
FROM runner_orders
WHERE duration IS NOT NULL;

# 12. Fastest and slowest delivery per runner
SELECT DISTINCT
  runner_id,
  FIRST_VALUE(duration) OVER (PARTITION BY runner_id ORDER BY duration ASC) AS fastest,
  FIRST_VALUE(duration) OVER (PARTITION BY runner_id ORDER BY duration DESC) AS slowest
FROM runner_orders
WHERE duration IS NOT NULL;

# 13. Rank runners by number of deliveries per day
SELECT
  DATE(pickup_time) AS delivery_date,
  runner_id,
  COUNT(order_id) AS deliveries,
  RANK() OVER (PARTITION BY DATE(pickup_time) ORDER BY COUNT(order_id) DESC) AS daily_rank
FROM runner_orders
WHERE cancellation IS NULL AND pickup_time IS NOT NULL
GROUP BY DATE(pickup_time), runner_id
ORDER BY delivery_date, daily_rank;

# 14. Rolling 7-day order count (per pizza)
SELECT
  pizza_id,
  DATE(order_time) AS order_date,
  COUNT(*) AS daily_count,
  SUM(COUNT(*)) OVER (
    PARTITION BY pizza_id ORDER BY DATE(order_time)
    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
  ) AS rolling_7d
FROM customer_orders
GROUP BY pizza_id, DATE(order_time)
ORDER BY pizza_id, order_date;

# 15. Compare runner’s avg delivery time vs global average
WITH runner_avg AS (
  SELECT runner_id, AVG(duration) AS avg_duration
  FROM runner_orders
  WHERE cancellation IS NULL AND duration IS NOT NULL
  GROUP BY runner_id
)
SELECT
  runner_id,
  avg_duration,
  ROUND(avg_duration - AVG(avg_duration) OVER (), 2) AS diff_from_global
FROM runner_avg;


#CTE Questions
#================================
# 1. Number of orders per day
WITH orders_per_day AS (
  SELECT DATE(order_time) AS order_date, COUNT(*) AS total_orders
  FROM customer_orders
  GROUP BY DATE(order_time)
)
SELECT * FROM orders_per_day;

#2. Orders with cancellations
WITH cancelled AS (
  SELECT order_id, runner_id, cancellation
  FROM runner_orders
  WHERE cancellation IS NOT NULL
)
SELECT * FROM cancelled;

#3. Runner earnings (assume $5 per completed delivery)
WITH completed AS (
  SELECT runner_id, COUNT(order_id) AS deliveries
  FROM runner_orders
  WHERE cancellation IS NULL
  GROUP BY runner_id
)
SELECT runner_id, deliveries, deliveries * 5 AS earnings
FROM completed;

# 4. Recursive CTE — generate dates between min and max order date

WITH RECURSIVE dates AS (
  SELECT MIN(DATE(order_time)) AS dt, MAX(DATE(order_time)) AS max_dt
  FROM customer_orders
  UNION ALL
  SELECT DATE_ADD(dt, INTERVAL 1 DAY), max_dt
  FROM dates
  WHERE dt < max_dt
)
SELECT dt FROM dates;

# 5. Number of pizzas ordered by each customer
WITH pizza_count AS (
  SELECT customer_id, COUNT(pizza_id) AS total_pizzas
  FROM customer_orders
  GROUP BY customer_id
)
SELECT * FROM pizza_count;

#6. Vegetarian pizza orders only
WITH vegetarian_orders AS (
  SELECT co.*
  FROM customer_orders co
  JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
  WHERE pn.pizza_name = 'Vegetarian'
)
SELECT customer_id, COUNT(*) AS veg_pizzas
FROM vegetarian_orders
GROUP BY customer_id;

# 7. Runners with above-average delivery duration
WITH runner_avg AS (
  SELECT runner_id, AVG(duration) AS avg_duration
  FROM runner_orders
  WHERE duration IS NOT NULL AND cancellation IS NULL
  GROUP BY runner_id
),
overall AS (
  SELECT AVG(avg_duration) AS global_avg FROM runner_avg
)
SELECT r.runner_id, r.avg_duration
FROM runner_avg r
JOIN overall o ON 1=1
WHERE r.avg_duration > o.global_avg;

# 8. Orders with extras
WITH with_extras AS (
  SELECT * FROM customer_orders
  WHERE extras IS NOT NULL
)
SELECT COUNT(*) AS total_extra_orders FROM with_extras;

# 9. Cleaned exclusions & extras
WITH cleaned AS (
  SELECT
    order_id,
    NULLIF(exclusions, 'null') AS exclusions,
    NULLIF(extras, 'null') AS extras
  FROM customer_orders
)
SELECT * FROM cleaned;

#10. Busiest order hour
WITH hours AS (
  SELECT HOUR(order_time) AS order_hour, COUNT(*) AS total_orders
  FROM customer_orders
  GROUP BY HOUR(order_time)
)
SELECT order_hour, total_orders
FROM hours
ORDER BY total_orders DESC
LIMIT 1;

# 11. Daily revenue (assume: Meatlovers = $12, Vegetarian = $10)
WITH revenue AS (
  SELECT
    DATE(co.order_time) AS order_date,
    SUM(CASE WHEN pn.pizza_name = 'Meatlovers' THEN 12
             WHEN pn.pizza_name = 'Vegetarian' THEN 10
             ELSE 0 END) AS daily_revenue
  FROM customer_orders co
  JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
  GROUP BY DATE(co.order_time)
)
SELECT * FROM revenue;

#12. Expand pizza recipes into rows (pizza_id → topping_id)
SELECT 
    pr.pizza_id,
    pt.topping_name
FROM pizza_recipes pr
JOIN JSON_TABLE(
    CONCAT('["', REPLACE(pr.toppings, ',', '","'), '"]'),
    "$[*]" COLUMNS (topping_id INT PATH "$")
) jt ON TRUE
JOIN pizza_toppings pt ON jt.topping_id = pt.topping_id;



# 13. Runner average speed (distance/duration)
WITH runner_speed AS (
  SELECT
    runner_id,
    AVG(distance / duration) AS avg_speed_km_per_min
  FROM runner_orders
  WHERE distance IS NOT NULL AND duration IS NOT NULL AND cancellation IS NULL
  GROUP BY runner_id
)
SELECT * FROM runner_speed;

# 14. Detect duplicate orders
WITH duplicates AS (
  SELECT order_id, COUNT(*) AS cnt
  FROM customer_orders
  GROUP BY order_id, customer_id, pizza_id, order_time
  HAVING COUNT(*) > 1
)
SELECT * FROM duplicates;

# 15. Runner success rate (% completed)
WITH stats AS (
  SELECT runner_id,
         COUNT(order_id) AS total_orders,
         SUM(CASE WHEN cancellation IS NULL THEN 1 ELSE 0 END) AS completed_orders
  FROM runner_orders
  GROUP BY runner_id
)
SELECT runner_id,
       total_orders,
       completed_orders,
       ROUND(100.0 * completed_orders / total_orders, 2) AS success_rate
FROM stats;


#Join Query
#===========================
# 1. Show each customers orders with pizza name
SELECT 
    co.customer_id,
    co.order_id,
    pn.pizza_name
FROM customer_orders co
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id;

# 2. List orders with runner assigned
SELECT 
    co.order_id,
    co.customer_id,
    ro.runner_id
FROM customer_orders co
JOIN runner_orders ro ON co.order_id = ro.order_id;

#3. Find which pizzas each runner delivered
SELECT 
    ro.runner_id,
    co.order_id,
    pn.pizza_name
FROM runner_orders ro
JOIN customer_orders co ON ro.order_id = co.order_id
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
WHERE ro.cancellation IS NULL;

# 4. Show pizzas and their topping list
SELECT 
    pn.pizza_name,
    pr.toppings
FROM pizza_names pn
JOIN pizza_recipes pr ON pn.pizza_id = pr.pizza_id;

#5. Orders with exclusions or extras
SELECT 
    co.order_id,
    co.customer_id,
    pn.pizza_name,
    co.exclusions,
    co.extras
FROM customer_orders co
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
WHERE co.exclusions IS NOT NULL OR co.extras IS NOT NULL;

#8. Total pizzas delivered by each runner
SELECT 
    ro.runner_id,
    COUNT(co.order_id) AS total_pizzas
FROM runner_orders ro
JOIN customer_orders co ON ro.order_id = co.order_id
WHERE ro.cancellation IS NULL
GROUP BY ro.runner_id;

#9. Which customers received Vegetarian pizzas
SELECT DISTINCT 
    co.customer_id
FROM customer_orders co
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
WHERE pn.pizza_name = 'Vegetarian';

# 10. Distance covered by each runner
SELECT 
    ro.runner_id,
    SUM(ro.distance) AS total_distance
FROM runner_orders ro
WHERE ro.cancellation IS NULL
GROUP BY ro.runner_id;

# 11. Join pizzas with their toppings (expanded)
SELECT 
    pn.pizza_name,
    pt.topping_name
FROM pizza_recipes pr
JOIN pizza_names pn ON pr.pizza_id = pn.pizza_id
JOIN JSON_TABLE(
    CONCAT('["', REPLACE(pr.toppings, ',', '","'), '"]'),
    "$[*]" COLUMNS (topping_id INT PATH "$")
) jt ON TRUE
JOIN pizza_toppings pt ON jt.topping_id = pt.topping_id;

#12. Runner earnings ($5 per successful delivery)
SELECT 
    ro.runner_id,
    COUNT(ro.order_id) * 5 AS earnings
FROM runner_orders ro
WHERE ro.cancellation IS NULL
GROUP BY ro.runner_id;

# 13. Which customer ordered which pizza and who delivered it
SELECT 
    co.customer_id,
    pn.pizza_name,
    ro.runner_id
FROM customer_orders co
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
JOIN runner_orders ro ON co.order_id = ro.order_id
WHERE ro.cancellation IS NULL;

#14. Find the busiest runner (most deliveries)
SELECT 
    ro.runner_id,
    COUNT(*) AS deliveries
FROM runner_orders ro
WHERE ro.cancellation IS NULL
GROUP BY ro.runner_id
ORDER BY deliveries DESC
LIMIT 1;

#15. Orders not delivered (JOIN + filter)
SELECT 
    co.order_id,
    co.customer_id
FROM customer_orders co
LEFT JOIN runner_orders ro ON co.order_id = ro.order_id
WHERE ro.cancellation IS NOT NULL;


# Combined (CTE + Window + Joins)
#======================================
#1. Top 3 most active customers by number of pizzas ordered
WITH customer_counts AS (
  SELECT customer_id, COUNT(*) AS total_pizzas
  FROM customer_orders
  GROUP BY customer_id
)
SELECT customer_id, total_pizzas,
       ROW_NUMBER() OVER (ORDER BY total_pizzas DESC) AS rank_num
FROM customer_counts
WHERE total_pizzas > 0
ORDER BY total_pizzas DESC
LIMIT 3;

#2. Average delivery time per runner vs overall average
WITH runner_times AS (
  SELECT runner_id, AVG(duration) AS avg_duration
  FROM runner_orders
  WHERE cancellation IS NULL AND duration IS NOT NULL
  GROUP BY runner_id
),
overall AS (
  SELECT AVG(duration) AS overall_avg
  FROM runner_orders
  WHERE cancellation IS NULL AND duration IS NOT NULL
)
SELECT r.runner_id, r.avg_duration, o.overall_avg
FROM runner_times r
CROSS JOIN overall o;

#3. Customer who ordered most Vegetarian pizzas
WITH veg_orders AS (
  SELECT co.customer_id, COUNT(*) AS veg_count
  FROM customer_orders co
  JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
  WHERE pn.pizza_name = 'Vegetarian'
  GROUP BY co.customer_id
)
SELECT customer_id, veg_count
FROM (
  SELECT customer_id, veg_count,
         RANK() OVER (ORDER BY veg_count DESC) AS rnk
  FROM veg_orders
) ranked
WHERE rnk = 1;

#4. Runner’s total distance per month
WITH monthly_distance AS (
  SELECT runner_id,
         DATE_FORMAT(pickup_time, '%Y-%m') AS month,
         SUM(distance) AS total_distance
  FROM runner_orders
  WHERE cancellation IS NULL AND distance IS NOT NULL
  GROUP BY runner_id, DATE_FORMAT(pickup_time, '%Y-%m')
)
SELECT runner_id, month, total_distance,
       SUM(total_distance) OVER (PARTITION BY runner_id ORDER BY month) AS running_total
FROM monthly_distance;

#5. Identify duplicate customer orders
WITH order_dupes AS (
  SELECT order_id, customer_id, pizza_id, COUNT(*) AS cnt
  FROM customer_orders
  GROUP BY order_id, customer_id, pizza_id
)
SELECT * 
FROM order_dupes
WHERE cnt > 1;

#6. Fastest and slowest delivery per runner
WITH delivery_stats AS (
  SELECT runner_id, order_id, duration,
         ROW_NUMBER() OVER (PARTITION BY runner_id ORDER BY duration ASC) AS fastest,
         ROW_NUMBER() OVER (PARTITION BY runner_id ORDER BY duration DESC) AS slowest
  FROM runner_orders
  WHERE cancellation IS NULL AND duration IS NOT NULL
)
SELECT runner_id, order_id, duration,
       CASE WHEN fastest = 1 THEN 'Fastest'
            WHEN slowest = 1 THEN 'Slowest' END AS category
FROM delivery_stats
WHERE fastest = 1 OR slowest = 1;

#7. Rank customers by total spend (Meatlovers = 12, Vegetarian = 10)
WITH spend AS (
  SELECT co.customer_id,
         SUM(CASE WHEN pn.pizza_name = 'Meatlovers' THEN 12 ELSE 10 END) AS total_spent
  FROM customer_orders co
  JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
  GROUP BY co.customer_id
)
SELECT customer_id, total_spent,
       RANK() OVER (ORDER BY total_spent DESC) AS spend_rank
FROM spend;

#8. Runner utilization rate (completed vs assigned)
WITH runner_stats AS (
  SELECT runner_id,
         COUNT(order_id) AS total_assigned,
         SUM(CASE WHEN cancellation IS NULL THEN 1 ELSE 0 END) AS completed
  FROM runner_orders
  GROUP BY runner_id
)
SELECT runner_id, total_assigned, completed,
       ROUND(100 * completed / total_assigned, 2) AS utilization_pct
FROM runner_stats;

# 9. Find longest gap between deliveries for each runner
WITH ordered_times AS (
  SELECT runner_id, pickup_time,
         LAG(pickup_time) OVER (PARTITION BY runner_id ORDER BY pickup_time) AS prev_time
  FROM runner_orders
  WHERE pickup_time IS NOT NULL
)
SELECT runner_id, pickup_time, prev_time,
       TIMESTAMPDIFF(MINUTE, prev_time, pickup_time) AS gap_minutes
FROM ordered_times
WHERE prev_time IS NOT NULL;

#10. Customers who ordered both pizza types
WITH types AS (
  SELECT customer_id, pn.pizza_name
  FROM customer_orders co
  JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
  GROUP BY customer_id, pn.pizza_name
)
SELECT customer_id
FROM types
GROUP BY customer_id
HAVING COUNT(DISTINCT pizza_name) = 2;

#11. Top toppings frequency across all orders
WITH expanded AS (
  SELECT co.order_id, jt.topping_id
  FROM customer_orders co
  JOIN pizza_recipes pr ON co.pizza_id = pr.pizza_id
  JOIN JSON_TABLE(
    CONCAT('["', REPLACE(pr.toppings, ',', '","'), '"]'),
    "$[*]" COLUMNS (topping_id INT PATH "$")
  ) jt ON TRUE
)
SELECT t.topping_name, COUNT(*) AS used_count,
       RANK() OVER (ORDER BY COUNT(*) DESC) AS rank_num
FROM expanded e
JOIN pizza_toppings t ON e.topping_id = t.topping_id
GROUP BY t.topping_name;

#12. Average delivery speed per runner (km/h)
WITH speeds AS (
  SELECT runner_id,
         SUM(distance) / (SUM(duration) / 60.0) AS avg_kmh
  FROM runner_orders
  WHERE distance IS NOT NULL AND duration IS NOT NULL AND cancellation IS NULL
  GROUP BY runner_id
)
SELECT * FROM speeds;

#13. Weekday vs weekend order volume
WITH orders AS (
  SELECT DAYNAME(order_time) AS day_name,
         CASE WHEN DAYOFWEEK(order_time) IN (1,7) THEN 'Weekend' ELSE 'Weekday' END AS day_type
  FROM customer_orders
)
SELECT day_type, COUNT(*) AS total_orders
FROM orders
GROUP BY day_type;

#14. First and last order time per customer
WITH times AS (
  SELECT customer_id,
         MIN(order_time) AS first_order,
         MAX(order_time) AS last_order
  FROM customer_orders
  GROUP BY customer_id
)
SELECT * FROM times;

#15. Find pizzas that were never ordered
WITH ordered_pizzas AS (
  SELECT DISTINCT pizza_id FROM customer_orders
)
SELECT pn.pizza_name
FROM pizza_names pn
LEFT JOIN ordered_pizzas op ON pn.pizza_id = op.pizza_id
WHERE op.pizza_id IS NULL;


# Business Case & Real Interview SQL Problems
#=====================================================
#1. Top 5 customers by total pizzas ordered
SELECT customer_id, COUNT(*) AS total_orders
FROM customer_orders
GROUP BY customer_id
ORDER BY total_orders DESC
LIMIT 5;

#2. Runner with the most cancellations
SELECT runner_id, COUNT(*) AS cancellations
FROM runner_orders
WHERE cancellation IS NOT NULL
GROUP BY runner_id
ORDER BY cancellations DESC
LIMIT 1;

#3. Average delivery time per pizza type
SELECT pn.pizza_name, AVG(ro.duration) AS avg_duration
FROM runner_orders ro
JOIN customer_orders co ON ro.order_id = co.order_id
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
WHERE ro.duration IS NOT NULL AND ro.cancellation IS NULL
GROUP BY pn.pizza_name;

#4. Detect duplicate orders by same customer at same time
SELECT customer_id, order_time, COUNT(*) AS duplicate_count
FROM customer_orders
GROUP BY customer_id, order_time
HAVING duplicate_count > 1;

#5. Revenue per day (assume Meatlovers=$12, Vegetarian=$10)
SELECT DATE(order_time) AS order_date,
       SUM(CASE WHEN pn.pizza_name='Meatlovers' THEN 12 ELSE 10 END) AS daily_revenue
FROM customer_orders co
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
GROUP BY DATE(order_time)
ORDER BY order_date;

#6. Percentage of orders with extras
SELECT ROUND(100.0 * SUM(CASE WHEN extras IS NOT NULL THEN 1 ELSE 0 END)
/ COUNT(*), 2) AS pct_with_extras
FROM customer_orders;

#7. Top toppings used across all pizzas
WITH expanded AS (
  SELECT co.order_id, jt.topping_id
  FROM customer_orders co
  JOIN pizza_recipes pr ON co.pizza_id = pr.pizza_id
  JOIN JSON_TABLE(
    CONCAT('["', REPLACE(pr.toppings, ',', '","'), '"]'),
    "$[*]" COLUMNS (topping_id INT PATH "$")
  ) jt ON TRUE
)
SELECT pt.topping_name, COUNT(*) AS usage_count
FROM expanded e
JOIN pizza_toppings pt ON e.topping_id = pt.topping_id
GROUP BY pt.topping_name
ORDER BY usage_count DESC
LIMIT 5;

#8. Customers who ordered both pizza types
SELECT customer_id
FROM customer_orders co
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
GROUP BY customer_id
HAVING COUNT(DISTINCT pizza_name) = 2;

#9. Runner efficiency (avg duration per km)
SELECT runner_id, AVG(duration / distance) AS avg_min_per_km
FROM runner_orders
WHERE distance IS NOT NULL AND duration IS NOT NULL AND cancellation IS NULL
GROUP BY runner_id;

#10. Customers with no orders in last 30 days
SELECT DISTINCT customer_id
FROM customer_orders
WHERE order_time < NOW() - INTERVAL 30 DAY;

#11. Orders with both exclusions and extras
SELECT order_id, customer_id, exclusions, extras
FROM customer_orders
WHERE exclusions IS NOT NULL AND extras IS NOT NULL;

#12. Runners delivering fastest on average
SELECT runner_id, AVG(duration) AS avg_duration
FROM runner_orders
WHERE cancellation IS NULL AND duration IS NOT NULL
GROUP BY runner_id
ORDER BY avg_duration ASC
LIMIT 3;

#13. Busiest order hour
SELECT HOUR(order_time) AS hour, COUNT(*) AS total_orders
FROM customer_orders
GROUP BY HOUR(order_time)
ORDER BY total_orders DESC
LIMIT 1;

#14. Most popular pizza per day
WITH daily_orders AS (
  SELECT DATE(order_time) AS order_date, pn.pizza_name, COUNT(*) AS total_orders
  FROM customer_orders co
  JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
  GROUP BY DATE(order_time), pn.pizza_name
)
SELECT order_date, pizza_name, total_orders
FROM (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY order_date ORDER BY total_orders DESC) AS rn
  FROM daily_orders
) t
WHERE rn = 1;

#15. Identify orders not delivered
SELECT co.order_id, co.customer_id
FROM customer_orders co
LEFT JOIN runner_orders ro ON co.order_id = ro.order_id
WHERE ro.cancellation IS NOT NULL OR ro.pickup_time IS NULL;

#16. Total revenue per runner
SELECT ro.runner_id, 
       SUM(CASE WHEN pn.pizza_name='Meatlovers' THEN 12 ELSE 10 END) AS revenue
FROM runner_orders ro
JOIN customer_orders co ON ro.order_id = co.order_id
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
WHERE ro.cancellation IS NULL
GROUP BY ro.runner_id;

#17. Runners with the most orders per pizza type
SELECT ro.runner_id, pn.pizza_name, COUNT(*) AS total_delivered
FROM runner_orders ro
JOIN customer_orders co ON ro.order_id = co.order_id
JOIN pizza_names pn ON co.pizza_id = pn.pizza_id
WHERE ro.cancellation IS NULL
GROUP BY ro.runner_id, pn.pizza_name
ORDER BY total_delivered DESC;

#18. Days with no orders
WITH all_dates AS (
  SELECT MIN(DATE(order_time)) AS start_date, MAX(DATE(order_time)) AS end_date
  FROM customer_orders
)
SELECT d.date
FROM (
  SELECT start_date + INTERVAL seq DAY AS date
  FROM all_dates
  JOIN (SELECT 0 seq UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6) numbers
  WHERE start_date + INTERVAL seq DAY <= end_date
) d
LEFT JOIN customer_orders co ON d.date = DATE(co.order_time)
WHERE co.order_id IS NULL;

#19. Average toppings per pizza ordered
SELECT AVG(LENGTH(pr.toppings) - LENGTH(REPLACE(pr.toppings, ',', '')) + 1) AS avg_toppings
FROM customer_orders co
JOIN pizza_recipes pr ON co.pizza_id = pr.pizza_id;

#20. Most frequent cancellations reason
SELECT cancellation, COUNT(*) AS count
FROM runner_orders
WHERE cancellation IS NOT NULL
GROUP BY cancellation
ORDER BY count DESC
LIMIT 1;

