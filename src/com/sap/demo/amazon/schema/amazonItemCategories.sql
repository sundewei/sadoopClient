create column table "SYSTEM"."AMAZON_ITEM_CATEGORIES"(
	"ITEM_ASIN" CHAR (10) not null default '',
	"CATEGORY_NODE_ID" INTEGER not null,
primary key ("ITEM_ASIN","CATEGORY_NODE_ID"))