CREATE TABLE IF NOT EXISTS TEST_TABLE (
  id INT64 NOT NULL,
  name STRING(MAX) NOT NULL
) PRIMARY KEY (id);

CREATE TABLE IF NOT EXISTS TEST_ITEMS (
  id INT64 NOT NULL,
  uid INT64 NOT NULL,
  iid INT64 NOT NULL,
  count INT64 NOT NULL
) PRIMARY KEY (id);

CREATE INDEX IF NOT EXISTS UID_INDEX ON TEST_ITEMS (uid);

CREATE TABLE IF NOT EXISTS FROM_TABLE (
  id INT64 NOT NULL,
  cost INT64 NOT NULL,
  description STRING(MAX) NOT NULL
) PRIMARY KEY(id);

CREATE TABLE IF NOT EXISTS MULTI_KEY (
  id1 INT64 NOT NULL,
  id2 INT64 NOT NULL,
  amount INT64 NOT NULL
) PRIMARY KEY(id1,id2);

CREATE TABLE IF NOT EXISTS BOOL_COLUMN (
  id INT64 NOT NULL,
  flag BOOL NOT NULL
) PRIMARY KEY(id);