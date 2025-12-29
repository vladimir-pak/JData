CREATE TABLE IF NOT EXISTS jdata.pg_attrdef_rep_tmp (
	adrelid int8 NOT NULL,
	adnum int8 NOT NULL,
	adbin varchar(255) NULL,
	CONSTRAINT pg_attrdef_rep_tmp_pkey PRIMARY KEY (adrelid, adnum)
);