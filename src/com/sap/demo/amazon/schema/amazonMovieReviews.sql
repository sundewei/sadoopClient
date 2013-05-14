create column table "SYSTEM"."AMAZON_MOVIE_REVIEWS"(
	"USER_ID" VARCHAR (200) not null default '',
	"MOVIE_ID" INTEGER not null,
	"RATING" INTEGER not null,
	"REVIEW_DATE" CHAR (10) not null)