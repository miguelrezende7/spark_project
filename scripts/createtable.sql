SET DATESTYLE TO PostgreSQL,European;

CREATE SEQUENCE IDAccount;
CREATE TABLE Accounts(
  IDAccount int default nextval('IDAccount'::regclass) PRIMARY KEY,
  clientcode Varchar(50),
  clientname Varchar(50),
  age int,
  accountmanager Varchar(50),
  accounttype Varchar(50),
  score Numeric(10,2)
);

