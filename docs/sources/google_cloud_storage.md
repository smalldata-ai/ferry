---
title: Using Google Cloud Storage as a Source in Ferry
---

# 🗄️ Using Google Cloud Storage as a Source in Ferry

Ferry allows you to ingest data **from an Google Cloud Storage bucket** and move it to different destinations like **data warehouses, databases, or APIs**.

## 📌 Prerequisites

Before using Google Cloud Storage as a source, ensure:
- You have an **Google Cloud Storage bucket** where the data is stored.
- You have a valid **Google Private Key**
- You have a valid **Google Client Email**
- The required file(s) exist in the GCS bucket.

## `source_uri` Format
To connect Ferry to an GCS bucket, use the following connection string format:

```plaintext
  gs://<bucket-name>/path/to/file.csv?project_id=<project-id>&private_key=<private-key>&client_email=<client-email>
```

### Parameters:
- **`bucket`**  - Name of the GCS bucket where data is stored.  
- **`project_id`** (String) – Name of the Google Console Project
- **`private_key`** (String) – Private Key to access the bucket
- **`client_email`** (String) – Client email id.
- **`path`** (String, Optional) – Path to a specific file or folder inside the S3 bucket.  


## `source_table_name` Format

```plaintext
    data/users.csv
```