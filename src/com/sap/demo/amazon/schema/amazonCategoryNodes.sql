create column table "SYSTEM"."AMAZON_CATEGORY_NODES"(
	"ID" INTEGER not null,
	"NAME" VARCHAR (200) not null default '',
	"PARENT_ID" INTEGER,
primary key ("ID"))