--#SORT=true
--#DATABASE=createtable
DROP DATABASE IF EXISTS createtable;

CREATE DATABASE createtable WITH TTL '100000d';

DROP TABLE IF EXISTS test0;

CREATE TABLE test0(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

DESCRIBE TABLE test0;

CREATE TABLE test1(
    column1 BIGINT CODEC(DELTA),
    column2 STRING CODEC(GZIP),
    column3 BIGINT UNSIGNED CODEC(NULL),
    column4 BOOLEAN,
    column5 DOUBLE CODEC(GORILLA),
    TAGS(column6, column7));

DESCRIBE TABLE test1;

DROP TABLE IF EXISTS test2;

DESCRIBE TABLE test2;

DROP TABLE IF EXISTS test0;

DROP DATABASE createtable;