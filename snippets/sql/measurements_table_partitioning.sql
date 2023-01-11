create table omop_cdm.measurement_temp (
    like omop_cdm.measurement
    including defaults
    including constraints
    including indexes
);

-- DROP TABLE
DROP TABLE omop_cdm.measurement_1;
DROP TABLE omop_cdm.measurement_2;
DROP TABLE omop_cdm.measurement_3;
DROP TABLE omop_cdm.measurement_4;
DROP TABLE omop_cdm.measurement_5;
DROP TABLE omop_cdm.measurement_6;
DROP TABLE omop_cdm.measurement_7;
DROP TABLE omop_cdm.measurement_8;
DROP TABLE omop_cdm.measurement_9;
DROP TABLE omop_cdm.measurement_10;
DROP TABLE omop_cdm.measurement_11;
DROP TABLE omop_cdm.measurement_12;
DROP TABLE omop_cdm.measurement_13;
DROP TABLE omop_cdm.measurement_14;
DROP TABLE omop_cdm.measurement_15;
DROP TABLE omop_cdm.measurement_16;
DROP TABLE omop_cdm.measurement_17;
DROP TABLE omop_cdm.measurement_null;

-- CREATE TABLE
 CREATE TABLE omop_cdm.measurement_1 ( CHECK ( string_to_integer(measurement_source_value) >= 0 AND string_to_integer(measurement_source_value) < 127 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_2 ( CHECK ( string_to_integer(measurement_source_value) >= 127 AND string_to_integer(measurement_source_value) < 210 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_3 ( CHECK ( string_to_integer(measurement_source_value) >= 210 AND string_to_integer(measurement_source_value) < 425 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_4 ( CHECK ( string_to_integer(measurement_source_value) >= 425 AND string_to_integer(measurement_source_value) < 549 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_5 ( CHECK ( string_to_integer(measurement_source_value) >= 549 AND string_to_integer(measurement_source_value) < 643 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_6 ( CHECK ( string_to_integer(measurement_source_value) >= 643 AND string_to_integer(measurement_source_value) < 741 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_7 ( CHECK ( string_to_integer(measurement_source_value) >= 741 AND string_to_integer(measurement_source_value) < 1483 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_8 ( CHECK ( string_to_integer(measurement_source_value) >= 1483 AND string_to_integer(measurement_source_value) < 3458 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_9 ( CHECK ( string_to_integer(measurement_source_value) >= 3458 AND string_to_integer(measurement_source_value) < 3695 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_10 ( CHECK ( string_to_integer(measurement_source_value) >= 3695 AND string_to_integer(measurement_source_value) < 8440 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_11 ( CHECK ( string_to_integer(measurement_source_value) >= 8440 AND string_to_integer(measurement_source_value) < 8553 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_12 ( CHECK ( string_to_integer(measurement_source_value) >= 8553 AND string_to_integer(measurement_source_value) < 220274 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_13 ( CHECK ( string_to_integer(measurement_source_value) >= 220274 AND string_to_integer(measurement_source_value) < 223921 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_14 ( CHECK ( string_to_integer(measurement_source_value) >= 223921 AND string_to_integer(measurement_source_value) < 224085 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_15 ( CHECK ( string_to_integer(measurement_source_value) >= 224085 AND string_to_integer(measurement_source_value) < 224859 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_16 ( CHECK ( string_to_integer(measurement_source_value) >= 224859 AND string_to_integer(measurement_source_value) < 227629 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_17 ( CHECK ( string_to_integer(measurement_source_value) >= 227629 AND string_to_integer(measurement_source_value) < 999999999 )) INHERITS (omop_cdm.measurement_temp);
 CREATE TABLE omop_cdm.measurement_null ( CHECK ( string_to_integer(measurement_source_value) > 999999999 )) INHERITS (omop_cdm.measurement_temp);

CREATE OR REPLACE FUNCTION string_to_integer(s text) RETURNS INTEGER AS $$
BEGIN
    RETURN s::integer;
EXCEPTION WHEN OTHERS THEN
    RETURN 1000000000;
END; $$ LANGUAGE plpgsql STRICT;

-- CREATE TRIGGER
CREATE OR REPLACE FUNCTION measurement_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
IF ( string_to_integer(NEW.measurement_source_value) >= 0 AND string_to_integer(NEW.measurement_source_value) < 127 ) THEN INSERT INTO omop_cdm.measurement_1 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 127 AND string_to_integer(NEW.measurement_source_value) < 210 ) THEN INSERT INTO omop_cdm.measurement_2 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 210 AND string_to_integer(NEW.measurement_source_value) < 425 ) THEN INSERT INTO omop_cdm.measurement_3 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 425 AND string_to_integer(NEW.measurement_source_value) < 549 ) THEN INSERT INTO omop_cdm.measurement_4 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 549 AND string_to_integer(NEW.measurement_source_value) < 643 ) THEN INSERT INTO omop_cdm.measurement_5 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 643 AND string_to_integer(NEW.measurement_source_value) < 741 ) THEN INSERT INTO omop_cdm.measurement_6 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 741 AND string_to_integer(NEW.measurement_source_value) < 1483 ) THEN INSERT INTO omop_cdm.measurement_7 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 1483 AND string_to_integer(NEW.measurement_source_value) < 3458 ) THEN INSERT INTO omop_cdm.measurement_8 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 3458 AND string_to_integer(NEW.measurement_source_value) < 3695 ) THEN INSERT INTO omop_cdm.measurement_9 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 3695 AND string_to_integer(NEW.measurement_source_value) < 8440 ) THEN INSERT INTO omop_cdm.measurement_10 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 8440 AND string_to_integer(NEW.measurement_source_value) < 8553 ) THEN INSERT INTO omop_cdm.measurement_11 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 8553 AND string_to_integer(NEW.measurement_source_value) < 220274 ) THEN INSERT INTO omop_cdm.measurement_12 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 220274 AND string_to_integer(NEW.measurement_source_value) < 223921 ) THEN INSERT INTO omop_cdm.measurement_13 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 223921 AND string_to_integer(NEW.measurement_source_value) < 224085 ) THEN INSERT INTO omop_cdm.measurement_14 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 224085 AND string_to_integer(NEW.measurement_source_value) < 224859 ) THEN INSERT INTO omop_cdm.measurement_15 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 224859 AND string_to_integer(NEW.measurement_source_value) < 227629 ) THEN INSERT INTO omop_cdm.measurement_16 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 227629 AND string_to_integer(NEW.measurement_source_value) < 999999999 ) THEN INSERT INTO omop_cdm.measurement_17 VALUES (NEW.*);
ELSE
	INSERT INTO omop_cdm.measurement_null VALUES (NEW.*);
END IF;
RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_measurement_trigger
    BEFORE INSERT ON omop_cdm.measurement_temp
    FOR EACH ROW EXECUTE PROCEDURE measurement_insert_trigger();

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
WHERE measurement_source_value = '220045'
LIMIT 5
;

SELECT COUNT(*) FROM omop_cdm.measurement_1;
SELECT COUNT(*) FROM omop_cdm.measurement_2;
SELECT COUNT(*) FROM omop_cdm.measurement_3;
SELECT COUNT(*) FROM omop_cdm.measurement_4;
SELECT COUNT(*) FROM omop_cdm.measurement_5;
SELECT COUNT(*) FROM omop_cdm.measurement_6;
SELECT COUNT(*) FROM omop_cdm.measurement_7;
SELECT COUNT(*) FROM omop_cdm.measurement_8;
SELECT COUNT(*) FROM omop_cdm.measurement_9;
SELECT COUNT(*) FROM omop_cdm.measurement_10;
SELECT COUNT(*) FROM omop_cdm.measurement_11;
SELECT COUNT(*) FROM omop_cdm.measurement_12;
SELECT COUNT(*) FROM omop_cdm.measurement_13;
SELECT COUNT(*) FROM omop_cdm.measurement_14;
SELECT COUNT(*) FROM omop_cdm.measurement_15;
SELECT COUNT(*) FROM omop_cdm.measurement_16;
SELECT COUNT(*) FROM omop_cdm.measurement_17;
SELECT COUNT(*) FROM omop_cdm.measurement_null;
SELECT COUNT(*) FROM omop_cdm.measurement_temp;
SELECT COUNT(*) FROM omop_cdm.measurement;



DELETE FROM omop_cdm.measurement_temp;


INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 0 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 10000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 20000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 30000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 40000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 50000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 60000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 70000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 80000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 90000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 100000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 110000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 120000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 130000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 140000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 150000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 160000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 170000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 180000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 190000000 LIMIT 10000000
;

INSERT INTO omop_cdm.measurement_temp
SELECT * FROM omop_cdm.measurement
OFFSET 200000000 LIMIT 10000000
;


ALTER TABLE 
omop_cdm.measurement
RENAME TO 
measurement_bkp
;

ALTER TABLE 
omop_cdm.measurement_temp
RENAME TO 
measurement
;

SELECT COUNT(*) FROM omop_cdm.measurement_bkp;

SELECT COUNT(*) FROM omop_cdm.measurement;
