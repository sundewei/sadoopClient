CREATE COLUMN TABLE POS_FACT (
  date_id INT NOT NULL,
  item_id VARCHAR(20) NOT NULL,
  store_id INT NOT NULL,
  location_id INT NOT NULL,
  promotion_id INT,
  customer_id INT,
  transaction_number BIGINT NOT NULL,
  sales_quantity INT NOT NULL,
  sales_dollars double NOT NULL,
  cost_dollars double NOT NULL,
  profit_dollars double NOT NULL
)