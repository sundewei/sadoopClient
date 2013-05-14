create column table "SYSTEM"."AMAZON_ITEMS"(
	"ID" INTEGER not null,
	"ASIN" CHAR (10) not null,
	"TITLE" VARCHAR (500) default '',
	"GROUP" VARCHAR (500) default '',
primary key ("ASIN"));