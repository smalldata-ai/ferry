# Incremental Loading
Incremental loading is a technique that allows Ferry to ingest only the new or updated data from a source, rather than reloading the entire dataset each time. This improves efficiency, reduces processing time, and minimizes resource consumption.

## How Incremental Loading Works
Incremental loading in Ferry works by tracking changes in the source data using specific mechanisms such as timestamps, sequence numbers. The process follows these key steps:

1. **Identify the Incremental Field**: Determine a column in the source data that can track changes, such as `updated_at`, `modified_timestamp`, or an auto-incrementing ID.
2. **Store Last Processed State**: Maintain a record of the last successfully processed value (e.g., the highest `updated_at` timestamp) to determine new or changed records in the next load.
3. **Extract Only New/Updated Data**: Query the source using a `WHERE` clause to filter records that have changed since the last recorded state.
4. **Load into Destination**: Append or merge the incremental data into the target system, depending on the processing logic.
5. **Update the State Store**: Persist the latest processed state for use in the next incremental load.


## Parameters Table


| Parameter         | Type                           | Description |
|------------------|--------------------------------|-------------|
| `incremental_key` | `string` | **(Required)** The key used to load new data from the source. |
| `start_position`  | `integer/datetime` | **(Optional)** The starting value for incremental loading. It can be an integer (e.g., an ID) or a datetime (e.g., a timestamp). |
| `end_position`    | `integer/datetime` | **(Optional)** The ending value for incremental loading, similar to `start_position`. |
| `lag_window`      | `float` | **(Optional)** A float value specifying a "look-back" window to include past data relative to the last fetched incremental data. Useful for handling late-arriving records. |
| `boundary_mode`   | `bool` | **(Optional)** Defines how to include/exclude the start and end cursor positions. Determines whether boundary values are inclusive or exclusive. |


