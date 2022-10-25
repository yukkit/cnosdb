--#DATABASE=test_window
--#SORT=true
DROP DATABASE IF EXIStime test_window;
CREATE DATABASE test_window;

CREATE TABLE sensors
(
    "value" Float,
    tags(metric)
);

insert into sensors(metric, time, "value")
values
    ('cpu_temp', '2020-01-01 00:00:00', 87),
    ('cpu_temp', '2020-01-01 00:00:01', 77),
    ('cpu_temp', '2020-01-01 00:00:02', 93),
    ('cpu_temp', '2020-01-01 00:00:03', 87),
    ('cpu_temp', '2020-01-01 00:00:04', 87),
    ('cpu_temp', '2020-01-01 00:00:05', 87),
    ('cpu_temp', '2020-01-01 00:00:06', 87),
    ('cpu_temp', '2020-01-01 00:00:07', 87);
SELECT
    metric,
    time,
    value,
    avg(value) OVER
       (PARTITION BY metric ORDER BY time ASC Rows BETWEEN 2 PRECEDING AND CURRENT ROW)
         AS moving_avg_temp
FROM sensors
ORDER BY
    metric ASC,
    time ASC;
