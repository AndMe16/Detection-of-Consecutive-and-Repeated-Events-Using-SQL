/* ============================================================
   CASE STUDY:
   Detection of Consecutive and Repeated Events Using SQL

   Description:
   - Stage 1: Identify consecutive event sequences per entity
   - Stage 2: Detect repeated metric values and coordinates
             within each consecutive sequence

   Engine: BigQuery
   ============================================================ */

-- ---------------------------
-- Stage 1: Base event filtering
-- ---------------------------
WITH base_events AS (
  SELECT
    entity_id,
    category_id,
    event_timestamp,
    coord_x,
    coord_y,
    metric_value,
    actor_id,
    extra_info,
    label
  FROM `events_table`
  WHERE
    DATE(event_timestamp) = DATE_SUB(CURRENT_DATE('America/Bogota'), INTERVAL 1 DAY)
    AND metric_value BETWEEN 56 AND 80
),

-- -------------------------------------------------
-- Stage 1: Calculate time gaps between events
-- -------------------------------------------------
events_with_time_gap AS (
  SELECT
    *,
    CASE
      WHEN TIMESTAMP_DIFF(
        event_timestamp,
        LAG(event_timestamp) OVER (PARTITION BY entity_id ORDER BY event_timestamp),
        SECOND
      ) > 300
      THEN NULL
      ELSE TIMESTAMP_DIFF(
        event_timestamp,
        LAG(event_timestamp) OVER (PARTITION BY entity_id ORDER BY event_timestamp),
        SECOND
      )
    END AS time_gap_seconds
  FROM base_events
),

-- -------------------------------------------------
-- Stage 1: Build consecutive event sequences
-- -------------------------------------------------
consecutive_sequences AS (
  SELECT
    *,
    SUM(sequence_reset)
      OVER (PARTITION BY entity_id ORDER BY event_timestamp) AS sequence_id
  FROM (
    SELECT
      *,
      IF(time_gap_seconds IS NULL, 1, 0) AS sequence_reset
    FROM events_with_time_gap
  )
),

-- -------------------------------------------------
-- Stage 1: Sequence metrics
-- -------------------------------------------------
sequence_metrics AS (
  SELECT
    *,
    TIMESTAMP_DIFF(
      LAST_VALUE(event_timestamp) OVER (
        PARTITION BY entity_id, sequence_id
        ORDER BY event_timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ),
      FIRST_VALUE(event_timestamp) OVER (
        PARTITION BY entity_id, sequence_id
        ORDER BY event_timestamp
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
      ),
      SECOND
    ) AS sequence_duration_seconds
  FROM consecutive_sequences
),

-- =================================================
-- Stage 2: Detect repeated values inside sequences
-- =================================================
metric_coord_comparison AS (
  SELECT
    *,
    -- Time gap when metric value remains the same
    CASE
      WHEN metric_value != LAG(metric_value)
           OVER (PARTITION BY entity_id, sequence_id ORDER BY event_timestamp)
      THEN NULL
      ELSE TIMESTAMP_DIFF(
        event_timestamp,
        LAG(event_timestamp)
          OVER (PARTITION BY entity_id, sequence_id ORDER BY event_timestamp),
        SECOND
      )
    END AS metric_time_gap,

    -- Time gap when coordinates remain the same
    CASE
      WHEN coord_x != LAG(coord_x)
           OVER (PARTITION BY entity_id, sequence_id ORDER BY event_timestamp)
        OR coord_y != LAG(coord_y)
           OVER (PARTITION BY entity_id, sequence_id ORDER BY event_timestamp)
      THEN NULL
      ELSE TIMESTAMP_DIFF(
        event_timestamp,
        LAG(event_timestamp)
          OVER (PARTITION BY entity_id, sequence_id ORDER BY event_timestamp),
        SECOND
      )
    END AS coord_time_gap
  FROM sequence_metrics
),

-- -------------------------------------------------
-- Stage 2: Group repeated patterns
-- -------------------------------------------------
repetition_groups AS (
  SELECT
    *,
    SUM(metric_reset)
      OVER (PARTITION BY entity_id, sequence_id ORDER BY event_timestamp)
      AS metric_group_id,
    SUM(coord_reset)
      OVER (PARTITION BY entity_id, sequence_id ORDER BY event_timestamp)
      AS coord_group_id
  FROM (
    SELECT
      *,
      IF(metric_time_gap IS NULL, 1, 0) AS metric_reset,
      IF(coord_time_gap IS NULL, 1, 0) AS coord_reset
    FROM metric_coord_comparison
  )
),

-- -------------------------------------------------
-- Stage 2: Repetition statistics
-- -------------------------------------------------
repetition_metrics AS (
  SELECT
    *,
    COUNT(*) OVER (
      PARTITION BY entity_id, sequence_id, metric_group_id
    ) AS metric_repeat_count,
    COUNT(*) OVER (
      PARTITION BY entity_id, sequence_id, coord_group_id
    ) AS coord_repeat_count
  FROM repetition_groups
)

-- =================================================
-- Final output
-- =================================================
SELECT
  entity_id,
  sequence_id,
  category_id,
  event_timestamp,
  coord_x,
  coord_y,
  metric_value,
  actor_id,
  extra_info,
  label,

  metric_repeat_count,
  coord_repeat_count,

  -- Flags indicating anomalous repetition
  metric_repeat_count > @min_repeated_events AS metric_repeated_flag,
  coord_repeat_count > @min_repeated_events AS coord_repeated_flag

FROM repetition_metrics
ORDER BY entity_id, event_timestamp;
