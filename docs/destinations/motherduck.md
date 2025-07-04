---
title: Using MotherDuck as a Destination in Ferry
---

# 🛢️ Using MotherDuck as a Destination in Ferry

Ferry allows you to ingest data **into a Destination database** from variety of source

## 📌 Prerequisites

Before using MotherDuck as a destination, ensure:
- Motherduck Account is created.
- Token is generated and valid.


## `destination_uri` Format
To connect Ferry to a MotherDuck database, use the following connection string format:

```plaintext
  md://ferrytest?token=token
```

### Parameters:
- **`<account-identifier>`** – MotherDuck account Identifier.
- **`<token>`** – MotherDuck access token.

## `destination_table_name` Format

```plaintext
  public.logs
```