create column table "SYSTEM"."AMAZON_MOVIES"(
	"ID" INTEGER not null,
	"NAME" VARCHAR (500) not null default '',
	"ASIN" CHAR (10) not null default '',
primary key ("ID"))