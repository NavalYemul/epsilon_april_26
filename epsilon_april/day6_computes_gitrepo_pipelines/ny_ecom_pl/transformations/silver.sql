create streaming table naval_silver.sales_cleaned_pl
(CONSTRAINT valid_order_id EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW)
as 
select distinct * except (_rescued_data,ingestion_date,path) from stream sales_pl;


CREATE OR REFRESH STREAMING TABLE naval_silver.products_cleaned_pl;

CREATE FLOW product_cdc AS AUTO CDC INTO
  naval_silver.products_cleaned_pl
FROM
  stream(naval_bronze.produts_pl)
KEYS
  (product_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  seqNum
COLUMNS * EXCEPT
  (operation,seqNum,_rescued_data,ingestion_date,path )
STORED AS
  SCD TYPE 1;



CREATE OR REFRESH STREAMING TABLE naval_silver.customers_cleaned_pl;

CREATE FLOW customer_cdc AS AUTO CDC INTO
  naval_silver.customers_cleaned_pl
FROM
  stream(naval_bronze.customers_pl)
KEYS
  (customer_id)
APPLY AS DELETE WHEN
  operation = "DELETE"
SEQUENCE BY
  sequenceNum
COLUMNS * EXCEPT
  (operation,sequenceNum,_rescued_data,ingestion_date,path )
STORED AS
  SCD TYPE 2;
