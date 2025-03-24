# ⛴️ Ferry Pipeline Observability Overview

Ferry provides detailed observability into every stage of the data pipeline, allowing users to track, debug, and optimize the performance of their ETL/ELT processes.


## 📌 Pipeline Metadata

- **Pipeline Name:** Unique identifier for the pipeline.
- **Status:** Final status of the run (`completed`, `failed`, `running`, etc.).
- **Start Time / End Time:** Timestamps indicating when the pipeline started and ended.
- **Duration:** Automatically computed from start and end times.


## 📦 Extract Phase

### Metrics Tracked:
- **Status:** Phase-level success or failure.
- **Start/End Time:** Timing for extract phase.
- **Resources Extracted:**
  - Resource name
  - Row count extracted
  - File size (e.g., bytes transferred)
- **Errors:** Any issues or anomalies during extraction.


## 🧹 Normalize Phase

### Metrics Tracked:
- **Status:** Completion state of normalization.
- **Start/End Time:** Duration of normalization.
- **Resources Normalized:**
  - Name of resulting tables or entities
  - Row count per resource
  - File size of each output
- **Transform Errors:** Optional, if transformation failed for specific rows or records.


## 🔄 Load Phase

### Metrics Tracked:
- Table-wise load counts
- Write durations
- Retry attempts (if any)
- Load-specific errors


## ⚠️ Error Tracking

Each phase (Extract, Normalize, Load) captures:
- Error messages
- Affected tables/resources
- Retry or fallback behavior (if implemented)

---
