-- Find shoes browsing in decembers
select year_month, item.category, sum(session_count)
from item_sessions s, dim_item item
where s.item_lookup = item.item_lookup
and item.category = 'SHOES'
and year_month like '%-12'
group by year_month, item.category
order by YEAR_MONTH asc

-- Creating distinct tables
create column table item_session_count
(calendar_date date not null,
 YEAR_MONTH char(7) not null,
 item_lookup varchar(20) not null,
 session_count integer not null,
 primary key (calendar_date, item_lookup));

insert into session_item_count
select distinct calendar_date, to_char(calendar_date, 'YYYY-MM'), item_lookup, session_count
    from session_item_affinity a

select * from session_item_count
order by item_lookup, calendar_date asc


create column table shopping_cart_count
(calendar_date date not null,
 YEAR_MONTH char(7) not null,
 item_lookup varchar(20) not null,
 cart_count integer not null,
 primary key (calendar_date, item_lookup));

 insert into shopping_cart_count
select distinct calendar_date, to_char(calendar_date, 'YYYY-MM'), item_lookup, total_affinity_count
    from shopping_cart a

select * from shopping_cart_count
order by cart_count desc;

create column table wishlist_item_count
(calendar_date date not null,
 YEAR_MONTH char(7) not null,
 item_lookup varchar(20) not null,
 wishlist_count integer not null,
 primary key (calendar_date, item_lookup));

 insert into wishlist_item_count
select distinct calendar_date, to_char(calendar_date, 'YYYY-MM'), item_lookup, total_affinity_count
    from wish_list a;

select * from wishlist_item_count
order by calendar_date, item_lookup  desc;

