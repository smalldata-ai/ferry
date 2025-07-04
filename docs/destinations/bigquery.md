---
title: Using BigQuery as a Destination in Ferry
---

# 🛢️ Using BigQuery as a Destination in Ferry

Ferry allows you to ingest data **into a Destination database** from variety of source

## 📌 Prerequisites

Before using BigQuery as a destination, ensure:
- You have Bigquery setup in your google console account
- You have a valid **Client Id**  **Client Secret** **Refresh Token**.


## `destination_uri` Format
To connect Ferry to a BigQuery database, use the following connection string format:

```plaintext
  bigquery://project_id?client_id=<clientId>&client_secret=<clientSecret>&refresh_token=<refreshToken>
```

### Parameters:
- **`<project_id>`** – Google Project Id.
- **`<clientId>`** – Google ClientId for the Project.
- **`<clientSecret>`** – Google clientSecret for the Project.
- **`<refreshToken>`** – refreshToken for the Project.

## `destination_table_name` Format

```plaintext
  public.transactions
```