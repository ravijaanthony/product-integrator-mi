-- Task monitoring tables (runtime coordinated-task duplication detection + forensic episodes).
-- Delivered separately as a patch; run against the WSO2 coordination DB (same DB as db2_cluster.sql).
-- NODE_ID is VARCHAR(508) so the (NODE_ID, TASK_NAME) primary key stays within DB2's index-key length
-- limit, mirroring the GROUP_ID(508) workaround used in db2_cluster.sql for CLUSTER_NODE_STATUS_TABLE.

CREATE TABLE IF NOT EXISTS RUNNING_TASK_OBSERVATION (
  NODE_ID VARCHAR(508) NOT NULL,
  TASK_NAME VARCHAR(512) NOT NULL,
  OBSERVED_AT BIGINT NOT NULL,
  PRIMARY KEY (NODE_ID, TASK_NAME)
);

CREATE TABLE IF NOT EXISTS TASK_DUPLICATION_EVENT (
  ID BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1, INCREMENT BY 1),
  TASK_NAME VARCHAR(512) NOT NULL,
  NODES VARCHAR(1024) NOT NULL,
  DESTINED_NODE VARCHAR(512),
  DETECTED_AT BIGINT NOT NULL,
  CLEARED_AT BIGINT,
  SEVERITY VARCHAR(16),
  TASK_KIND VARCHAR(16),
  PRIMARY KEY (ID)
);
