CREATE TABLE IP_LOCATIONS (
	START_IP_NUM BIGINT NOT NULL,
	END_IP_NUM BIGINT NOT NULL,
	LOC_ID INTEGER NOT NULL,
	primary key (START_IP_NUM,END_IP_NUM)
);