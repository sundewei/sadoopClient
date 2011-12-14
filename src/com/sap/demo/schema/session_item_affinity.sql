create column table session_item_affinity (
calendar_date date not null,
item_lookup varchar(20) not null,
affinity_item_lookup varchar(20) not null,
affinity_count integer not null,
session_count integer not null,
primary key (calendar_date, item_lookup, affinity_item_lookup)
);