create column table session_item_affinity (
date_id int not null,
item_id int not null,
affinity_item_id int not null,
affinity_count int not null,
session_count int not null,
primary key (date_id, item_id, affinity_item_id)
);