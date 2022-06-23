SET DATESTYLE TO PostgreSQL,European;

CREATE SEQUENCE IDTransaction;
CREATE TABLE Transactions(
  IDTransaction int default nextval('IDTransaction'::regclass) PRIMARY KEY,
  client_code Varchar(50),
  agency Varchar(50),
  operation_value Numeric(10,2),
  operation_type Varchar(50),
  transaction_date Varchar(50),
  account_balance Numeric(10,2),
  idaccount int,
  clientname Varchar(50),
  age int,
  accountmanager Varchar(50),
  accounttype Varchar(50),
  score Numeric(10,2),
  risk_operation BOOLEAN
);

