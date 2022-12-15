--#SORT=true
DROP DATABASE IF EXISTS test1;

DESCRIBE DATABASE test1 WITH TTL '100000d';

CREATE DATABASE IF NOT EXISTS test1;

DESCRIBE DATABASE test1;

CREATE DATABASE IF NOT EXISTS describetest2;

DESCRIBE DATABASE describetest2;

DROP DATABASE IF EXISTS describetest2;

DROP DATABASE IF EXISTS test1;