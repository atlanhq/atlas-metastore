
CREATE TABLE assets (
  asset_id STRING,
  PRIMARY KEY (asset_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'assets',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE relationships (
  parent_id STRING,
  child_id  STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
  PRIMARY KEY (parent_id, child_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'relationships',
  'key.format' = 'json',
  'value.format' = 'json'
);

CREATE TABLE asset_tags_direct (
  asset_id STRING,
  tag      STRING,
  event_time TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
  PRIMARY KEY (asset_id, tag) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'asset_tags_direct',
  'key.format' = 'json',
  'value.format' = 'json'
);

-- =========================================================
-- DOWNSTREAM TRANSITIVE CLOSURE (KAFKA FEEDBACK LOOP)
-- =========================================================

CREATE TABLE downstream (
  ancestor_id   STRING,
  descendant_id STRING,
  event_time    TIMESTAMP(3),
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND,
  PRIMARY KEY (ancestor_id, descendant_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'downstream',
  'key.format' = 'json',
  'value.format' = 'json'
);


-- =========================================================
-- TAG PROPAGATION
-- =========================================================

CREATE VIEW propagated_tags AS
SELECT
  d.descendant_id AS asset_id,
  t.tag
FROM asset_tags_direct t
JOIN downstream d
  ON t.asset_id = d.ancestor_id;

CREATE VIEW all_tags_flat AS
SELECT asset_id, tag FROM asset_tags_direct
UNION ALL
SELECT asset_id, tag FROM propagated_tags;

-- Deduplicate to clean changelog
CREATE VIEW asset_tag_closure AS
SELECT asset_id, tag
FROM (
  SELECT
    asset_id,
    tag,
    ROW_NUMBER() OVER (
      PARTITION BY asset_id, tag
      ORDER BY asset_id
    ) AS rn
  FROM all_tags_flat
)
WHERE rn = 1;

-- =========================================================
-- FINAL MATERIALIZED TAG TABLE (UPSERT FULL TAG SET)
-- =========================================================

CREATE TABLE asset_tags_materialized (
  asset_id STRING,
  tags MULTISET<STRING>,
  PRIMARY KEY (asset_id) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'asset_tags_materialized',
  'key.format' = 'json',
  'value.format' = 'json'
);

EXECUTE STATEMENT SET
BEGIN
-- Seed downstream with direct edges
INSERT INTO downstream
SELECT
  parent_id AS ancestor_id,
  child_id  AS descendant_id,
  event_time
FROM relationships;

-- Recursive expansion (fixpoint via Kafka)
INSERT INTO downstream
SELECT
  d.ancestor_id,
  r.child_id AS descendant_id,
  r.event_time
FROM downstream d
JOIN relationships r
  ON d.descendant_id = r.parent_id
WHERE d.ancestor_id <> r.child_id;

INSERT INTO asset_tags_materialized
SELECT
  asset_id,
  COLLECT(tag) AS tags
FROM asset_tag_closure
GROUP BY asset_id;
END;
