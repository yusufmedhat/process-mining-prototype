import polars as pl

def get_proprietary_dfg(df_pandas):
    # Convert to Polars for 10x speed
    df = pl.from_pandas(df_pandas)
    
    # Sort and create the 'Next' relationship
    df = df.sort(["case_id", "timestamp"])
    
    # Core Logic: Shift the activity within each case
    df_pairs = df.with_columns(
        pl.col("activity_name").shift(-1).over("case_id").alias("next_activity"),
        pl.col("timestamp").shift(-1).over("case_id").alias("next_timestamp")
    ).filter(pl.col("next_activity").is_not_null())

    # Calculate Edge Performance (Durations)
    df_edges = df_pairs.with_columns(
        (pl.col("next_timestamp") - pl.col("timestamp")).dt.total_seconds().alias("duration")
    ).group_by(["activity_name", "next_activity"]).agg(
        pl.count().alias("frequency"),
        pl.col("duration").median().alias("median_duration")
    )

    return df_edges.to_pandas()