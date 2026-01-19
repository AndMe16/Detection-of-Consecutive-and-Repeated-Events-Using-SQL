# Detection of Consecutive and Repeated Events Using SQL

## Overview
This project demonstrates how to detect consecutive event sequences and identify repeated patterns within those sequences using advanced SQL techniques in BigQuery.

The logic is domain-agnostic and applicable to scenarios such as:
- IoT telemetry
- Monitoring systems
- Log analysis
- Sensor data processing

## Key Concepts
- Window functions (LAG, COUNT, LAST_VALUE)
- Temporal gap analysis
- Dynamic sequence grouping
- Repetition detection
- Parameterized thresholds

## Workflow
1. Filter and prepare base events
2. Detect time gaps to create consecutive sequences
3. Analyze repeated metric values and coordinates
4. Flag anomalous repetitions based on configurable thresholds

## Parameters
- `@min_repeated_events`: Minimum number of repeated events
- `@min_repeated_duration`: Minimum duration (seconds) of repetition

## Notes
All table and column names are anonymized.  
This project uses synthetic or generic data structures for demonstration purposes.
