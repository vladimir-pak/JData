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

CREATE TABLE IF NOT EXISTS jdata.pg_class_rep_tmp (
	relnamespace numeric(38) NULL,
	"oid" int8 NOT NULL,
	relkind varchar(255) NULL,
	relname varchar(255) NULL,
	CONSTRAINT pg_class_rep_tmp_pkey PRIMARY KEY (oid)
);

CREATE TABLE IF NOT EXISTS jdata.pg_type_rep_tmp (
	"oid" int8 NOT NULL,
	typnamespace int8 NULL,
	db varchar(255) NULL,
	typname varchar(255) NULL,
	CONSTRAINT pg_type_rep_tmp_pkey PRIMARY KEY (oid)
);
