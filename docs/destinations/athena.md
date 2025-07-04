---
title: Using Athena as a Destination in Ferry
---

# 🛢️ Using Athena as a Destination in Ferry

Ferry allows you to ingest data **into a Destination database** from variety of source

## 📌 Prerequisites

Before using Athena as a destination, ensure:
- You have an **AWS S3 bucket** where the data is stored.
- You have a valid **AWS Access Key ID** and **Secret Access Key**.
- You have a valid **Work Group**



## `destination_uri` Format
To connect Ferry to a Athena database, use the following connection string format:

```plaintext
  "athena://<bucket-name>?access_key_id=aa&access_key_secret=aa&region=<region>&work_group=<work-group>",        
```

### Parameters:
- **`bucket-name`**  - Name of the S3 bucket where data is stored.  
- **`access_key_id`** (String) – AWS Access Key ID for authentication.  
- **`access_key_secret`** (String) – AWS Secret Access Key for authentication.  
- **`region`** (String) – AWS region where the bucket is hosted.  
- **`work-group`** (String, Optional) – Path to a specific file or folder inside the S3 bucket.  

