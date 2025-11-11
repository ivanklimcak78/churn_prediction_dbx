DROP TABLE IF EXISTS main.customer_churn.churn_labels
;

CREATE TABLE main.customer_churn.churn_labels 
(
  customer_id STRING NOT NULL,
  transaction_ts TIMESTAMP NOT NULL,
  split STRING NOT NULL,
  churn INTEGER NOT NULL,

  CONSTRAINT pk PRIMARY KEY(customer_id, transaction_ts)
)
COMMENT "Churn labels table. Contains ground truth for a customer. Primary key is (customer_id, transaction_ts)."
;

DROP TABLE IF EXISTS main.customer_churn.churn_features 
;