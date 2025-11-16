CREATE TABLE IF NOT EXISTS jdata.pg_attribute_rep_tmp (
	atthasdef bool NULL,
	attnotnull bool NULL,
	atttypmod int4 NULL,
	attnum int8 NULL,
	attrelid int8 NOT NULL,
	atttypid int8 NULL,
	attname varchar(255) NOT NULL,
	db varchar(255) NULL,
	CONSTRAINT pg_attribute_rep_tmp_pkey PRIMARY KEY (attrelid, attname)
);
