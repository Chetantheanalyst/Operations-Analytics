create database operations_db ;

use operations_db;

-----Let's import the data in SQL Server-------

Create table users (user_id int,	
					created_at varchar(max),	
					company_id int, 	
					language varchar(max), 
					activated_at	varchar (max), 
					state varchar (max)
					);


create table events (user_id int, 
					occurred_at varchar (max), 
					event_type varchar(max),	
					event_name varchar(max),	
					location varchar(max), 	
					device varchar(max)	, 
					user_type int
					);
				
create table email_events (user_id int,	
							occurred_at varchar(max), 
							action varchar(max),	
							user_type int
							);


----Importing the data using bulk query-----
----Users------

BULK INSERT users
FROM 'C:\Users\Chetan Vaishnav\Downloads\users.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
	MAXERRORS = 20
);

Select * from users;

----Events------

BULK INSERT events
FROM 'C:\Users\Chetan Vaishnav\Downloads\events.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
	MAXERRORS = 20
);


-----Email_events------

BULK INSERT email_events
FROM 'C:\Users\Chetan Vaishnav\Downloads\email_events.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
	MAXERRORS = 20
);


create table job_data (ds varchar(max),	
                       job_id int, 
					   actor_id int, 
					   event varchar(max), 
					   language varchar(max), 
					   time_spent int,	
					   org varchar(max)
);

bulk insert  job_data 
FROM 'C:\Users\Chetan Vaishnav\Downloads\job_data.csv'
WITH (
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '\n',
    FIRSTROW = 2,
	MAXERRORS = 20
);

use operations_db;

SELECT column_name, data_type
FROM INFORMATION_SCHEMA.Columns
WHERE TABLE_Name = 'users';

---Alteration of tables---
---Users (col - created_at)


Alter table users
add created_at_temp datetime;

UPDATE users
SET created_at_temp = TRY_PARSE(created_at AS datetime USING 'en-GB');


SELECT *
FROM users
WHERE created_at_temp IS NULL AND created_at IS NOT NULL AND created_at <> '';

ALTER TABLE users DROP COLUMN created_at;
EXEC sp_rename 'users.created_at_temp', 'created_at', 'COLUMN';

---Alteration of tables---
---Users (col - activated_at)

Alter table users
add activated_at_temp datetime;

UPDATE users
SET activated_at_temp = TRY_PARSE(activated_at AS datetime USING 'en-GB');

SELECT *
FROM users
WHERE activated_at_temp IS NULL AND activated_at IS NOT NULL AND created_at <> '';

ALTER TABLE users DROP COLUMN activated_at;
EXEC sp_rename 'users.activated_at_temp', 'created_at', 'COLUMN';

EXEC sp_rename 'users.activated_at_temp', 'activated_at', 'COLUMN';


Select * from users ;
select * from events;

SELECT column_name, data_type
FROM INFORMATION_SCHEMA.Columns
WHERE TABLE_Name = 'events';

Alter table events 
alter column occured_at datetime;

Alter table events 
add occured_at_new datetime ;

select * from events 


UPDATE events
SET occured_at_new = TRY_PARSE(occurred_at AS datetime USING 'en-GB');

SELECT *
FROM events
WHERE occured_at_new IS NULL AND occurred_at IS NOT NULL AND occurred_at <> '';

ALTER TABLE events DROP COLUMN occurred_at;
EXEC sp_rename 'events.occured_at_new', 'occurred_at', 'COLUMN';


----email events----

select * from email_events;

SELECT column_name, data_type
FROM INFORMATION_SCHEMA.Columns
WHERE TABLE_Name = 'email_events';

alter table email_events
alter column occurred_at datetime;

Alter table email_events 
add occurred_at_new datetime ;

select * from email_events


UPDATE email_events
SET occurred_at_new = TRY_PARSE(occurred_at AS datetime USING 'en-GB');

SELECT *
FROM email_events
WHERE occurred_at_new IS NULL AND occurred_at IS NOT NULL AND occurred_at <> '';

ALTER TABLE email_events DROP COLUMN occurred_at;
EXEC sp_rename 'email_events.occurred_at_new', 'occurred_at', 'COLUMN';


-----now we can begin the SQL Query analysis---

-----Weekly User Engagement:
-----Objective: Measure the activeness of users on a weekly basis.
-----Your Task: Write an SQL query to calculate the weekly user engagement.

select * from users
select * from events
select * from email_events



Select Datepart(week, e.occurred_at) as 'Week',
datename(month, e.occurred_at) as 'Month',

SELECT 
    DATEPART(week, occurred_at) AS 'Week',
    DATENAME(month, occurred_at) AS 'Month',
    COUNT(DISTINCT user_id) AS 'weekly_user_engagement'
FROM 
    events e
WHERE 
    event_type = 'engagement' 
    AND e.event_name = 'login'
GROUP BY 
    DATEPART(week, occurred_at), 
    DATENAME(month, occurred_at)
ORDER BY 
    Week;


----- (B) User Growth Analysis:
-----Objective: Analyze the growth of users over time for a product.
-----Your Task: Write an SQL query to calculate the user growth for the product


WITH monthly_users AS (
    SELECT 
        YEAR(CAST(activated_at AS DATE)) AS year,
        MONTH(CAST(activated_at AS DATE)) AS month,
        COUNT(DISTINCT user_id) AS new_users
    FROM 
        users
    WHERE 
        activated_at IS NOT NULL
    GROUP BY 
        YEAR(CAST(activated_at AS DATE)), 
        MONTH(CAST(activated_at AS DATE))
)
SELECT 
    year,
    month,
    new_users,
    SUM(new_users) OVER (ORDER BY year, month ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS Total_users
FROM 
    monthly_users
ORDER BY 
    year, 
    month;


-----(C) Weekly Retention Analysis:
-----Objective: Analyze the retention of users on a weekly basis after signing up for a product.
-----Your Task: Write an SQL query to calculate the weekly retention of users based on their sign-up cohort.



SELECT 
    DATEPART(WEEK, occurred_at) AS week,
    COUNT(CASE WHEN event_type = 'engagement' THEN user_id END) AS engagement,
    COUNT(CASE WHEN event_type = 'signup_flow' THEN user_id END) AS signup
FROM events
GROUP BY DATEPART(WEEK, occurred_at)
ORDER BY week;


-----(D) Weekly Engagement Per Device:
-----Objective: Measure the activeness of users on a weekly basis per device.
-----Your Task: Write an SQL query to calculate the weekly engagement per device.
SELECT 
    DATEPART(YEAR, occurred_at) AS year,
    DATEPART(WEEK, occurred_at) AS week_number,
    COUNT(DISTINCT CASE 
        WHEN device IN ('acer aspire notebook', 'dell inspiron notebook', 'asus chromebook', 'macbook air', 'macbook pro', 'lenovo thinkpad') 
        THEN user_id 
    END) AS laptop_engagement,
    COUNT(DISTINCT CASE 
        WHEN device IN ('acer aspire desktop', 'dell inspiron desktop', 'hp pavilion desktop', 'mac mini') 
        THEN user_id 
    END) AS desktop_engagement,
    COUNT(DISTINCT CASE 
        WHEN device IN ('iphone 4s', 'iphone 5', 'iphone 5s', 'nexus 5', 'htc one', 'nokia lumia 635', 'amazon fire phone', 'samsung galaxy note', 'samsung galaxy s4') 
        THEN user_id 
    END) AS mobile_engagement,
    COUNT(DISTINCT CASE 
        WHEN device IN ('nexus 7', 'nexus 10', 'ipad air', 'ipad mini', 'kindle fire', 'samsumg galaxy tablet', 'windows surface') 
        THEN user_id 
    END) AS tablet_engagement,
    COUNT(DISTINCT user_id) AS total_engagement
FROM events
WHERE event_type = 'engagement'
GROUP BY 
    DATEPART(YEAR, occurred_at),
    DATEPART(WEEK, occurred_at)
ORDER BY 
    year,
    week_number ;



----Email Engagement Analysis:
----Objective: Analyze how users are engaging with the email service.
----Your Task: Write an SQL query to calculate the email engagement metrics.

SELECT 
    CAST(occurred_at AS DATE) AS Date,
    COUNT(CASE WHEN action = 'email_clickthrough' THEN user_id ELSE NULL END) AS email_clickthrough,
    COUNT(CASE WHEN action = 'email_open' THEN user_id ELSE NULL END) AS email_open
FROM 
    email_events
GROUP BY 
    CAST(occurred_at AS DATE)
ORDER BY 
    Date;


----Jobs Reviewed Over Time:
----Objective: Calculate the number of jobs reviewed per hour for each day in November 2020.
----Your Task: Write an SQL query to calculate the number of jobs reviewed per hour for each day in November 2020.

SELECT column_name, data_type
FROM INFORMATION_SCHEMA.Columns
WHERE TABLE_Name = 'job_data';

Select * from job_data;

SELECT 
    ds  AS review_Date,
    CAST(SUM(time_spent) / 60.0 AS DECIMAL(10,2)) AS hours_spent,
    COUNT(job_id) AS jobs_reviewed
FROM job_data
GROUP BY 
    ds 
ORDER BY 
    review_Date;


----Throughput Analysis:
----Objective: Calculate the 7-day rolling average of throughput (number of events per second).
----Your Task: Write an SQL query to calculate the 7-day rolling average of throughput. 
----Additionally, explain whether you prefer using the daily metric or the 7-day rolling average for throughput, and why.
     

WITH FilteredEvents AS (
    SELECT 
        *,
        CAST(ds AS DATE) AS event_date
    FROM job_data
    WHERE event IN ('transfer', 'decision')
),
DailyThroughput AS (
    SELECT
        event_date,
        COUNT(*) * 1.0 / NULLIF(SUM(time_spent), 0) AS events_per_second
    FROM FilteredEvents
    GROUP BY event_date
),
RollingThroughput AS (
    SELECT
        d1.event_date,
       CAST(ROUND(AVG(d2.events_per_second), 2) AS DECIMAL(10,2)) AS rolling_avg_throughput
    FROM DailyThroughput d1
    JOIN DailyThroughput d2
      ON d2.event_date BETWEEN DATEADD(DAY, -6, d1.event_date) AND d1.event_date
    GROUP BY d1.event_date
)
SELECT 
    event_date,
    rolling_avg_throughput
FROM RollingThroughput
ORDER BY event_date;



-----Language Share Analysis:
-----Objective: Calculate the percentage share of each language in the last 30 days.
---- Your Task: Write an SQL query to calculate the percentage share of each language over the last 30 days.



WITH JD AS (
    SELECT 
        language,
        COUNT(job_id) AS num_jobs
    FROM job_data
    GROUP BY language
),
total AS (
    SELECT 
        COUNT(job_id) AS total_jobs
    FROM job_data
)
SELECT 
    JD.language,
    CONCAT(CAST((100.0 * JD.num_jobs / total.total_jobs) AS DECIMAL(5,2)),'%') AS percentage_share
FROM JD
CROSS JOIN total
ORDER BY percentage_share DESC;



----Duplicate Rows Detection:
----Objective: Identify duplicate rows in the data.
----Your Task: Write an SQL query to display duplicate rows from the job_data table.

SELECT *
FROM (
    SELECT *,
           ROW_NUMBER() OVER (PARTITION BY job_id ORDER BY job_id) AS row_num
    FROM job_data
) a
WHERE row_num > 1;


