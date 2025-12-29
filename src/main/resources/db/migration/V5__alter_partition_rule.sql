DROP TABLE IF EXISTS jdata.pg_partition_rule_rep;

CREATE TABLE IF NOT EXISTS jdata.pg_partition_rule_rep (
	"oid" int8 NOT NULL,
	parchildrelid int8 NULL,
	paroid int8 NULL,
	parruleord int4,
	parrangeevery text,
	db varchar(255) NULL,
	CONSTRAINT pg_partition_rule_rep_pkey PRIMARY KEY (oid)
);