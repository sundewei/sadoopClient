create column table "SYSTEM"."AMAZON_ITEM_REVIEWS"(
	"ITEM_ASIN" CHAR (10) not null default '',
	"REVIEW_DATE" CHAR (10) not null default '',
	"CUSTOMER" VARCHAR (20) not null default '',
	"RATING" INTEGER not null,
	"VOTE" INTEGER not null default 0,
	"HELPFUL" INTEGER not null default 0,
primary key ("ITEM_ASIN","REVIEW_DATE","CUSTOMER", "RATING", "VOTE", "HELPFUL"))