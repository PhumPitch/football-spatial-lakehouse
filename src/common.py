"""
Common Pipeline Utilities & Cleaners
====================================
Shared transformation functions, DataFrame cleaners, schema flatteners,
and logging helpers used across Silver and Gold pipeline layers.

Key Capabilities:
- Recursive StructType flattener (flatten_dataframe).
- Spatial coordinate split and validation (end_location_split, clean_glitch_location).
- Boolean null-handling (fill_na_bool_columns).
- Standard event spine extraction (get_common_columns).
- Audit metadata enrichment (finalize_silver_dataframe).
"""

import logging
from typing import List
import pyspark.sql.functions as F
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, BooleanType

logger = logging.getLogger(__name__)


def setup_logger(name: str = "football_pipeline") -> logging.Logger:
    """
    Configures and returns an enterprise-standard formatted logger.
    """
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    return logging.getLogger(name)


def get_common_columns() -> List:
    """
    Returns standard spine expressions for all Silver event-level tables.
    Extracts primary keys, temporal chronology, player/team metadata, and match context.
    """
    return [
        # Composite Primary Keys & Lineage
        F.col("competition_id"),
        F.col("season_id"),
        F.col("match_id"),
        F.col("index"),
        F.col("id").alias("event_id"),

        # Chronology & Timing
        F.col("period"),
        F.col("timestamp"),
        F.col("minute"),
        F.col("second"),

        # Event & Player Context
        F.col("type.name").alias("event_type"),
        F.col("player.id").alias("player_id"),
        F.col("player.name").alias("player_name"),
        F.col("team.id").alias("team_id"),
        F.col("team.name").alias("team_name"),

        # Spatial Origin & Possession Context
        F.col("location").alias("start_location"),
        F.get(F.col("location"), 0).alias("start_x"),
        F.get(F.col("location"), 1).alias("start_y"),
        F.col("possession").alias("possession_id"),
        F.col("possession_team.id").alias("possession_team_id"),
        F.col("possession_team.name").alias("possession_team_name"),
        F.coalesce(F.col("under_pressure"), F.lit(False)).alias("is_under_pressure"),
        F.col("play_pattern.name").alias("play_pattern_name"),
        F.coalesce(F.col("duration"), F.lit(0.0)).alias("event_duration"),
        F.col("related_events").alias("related_events"),

        # Audit & Lineage Metadata
        F.col("_file_path").alias("_bronze_file_path"),
    ]


def flatten_dataframe(df: DataFrame) -> DataFrame:
    """
    Recursively unpacks and flattens all nested StructType columns.
    E.g., pass.body_part.name -> pass_body_part_name.
    """
    complex_fields = [f for f in df.schema.fields if isinstance(f.dataType, StructType)]

    if not complex_fields:
        return df

    select_expr = []
    for f in df.schema.fields:
        if isinstance(f.dataType, StructType):
            for child in f.dataType.fields:
                select_expr.append(f"{f.name}.{child.name} AS {f.name}_{child.name}")
        else:
            select_expr.append(f.name)

    return flatten_dataframe(df.selectExpr(*select_expr))


def end_location_split(df: DataFrame) -> DataFrame:
    """
    Splits any column ending with 'end_location' into explicit numeric columns:
    [prefix]end_x, [prefix]end_y, and [prefix]end_z (coalesced to 0.0 if absent).
    """
    exprs = []
    for col_name in df.columns:
        if col_name.endswith("end_location"):
            prefix = col_name.split("end_location")[0]
            exprs.append(F.col(col_name))
            exprs.append(F.get(F.col(col_name), 0).alias(f"{prefix}end_x"))
            exprs.append(F.get(F.col(col_name), 1).alias(f"{prefix}end_y"))
            exprs.append(
                F.coalesce(F.get(F.col(col_name), 2), F.lit(0.0)).alias(f"{prefix}end_z")
            )
        else:
            exprs.append(F.col(col_name))

    return df.select(*exprs)


def fill_na_bool_columns(df: DataFrame) -> DataFrame:
    """
    Finds all BooleanType columns in the schema and fills null values with False.
    Ensures safe predicate filtering (e.g. where is_pass_complete == True).
    """
    bool_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, BooleanType)]
    if bool_cols:
        return df.fillna(False, subset=bool_cols)
    return df


def clean_glitch_location(
    df: DataFrame,
    min_glitch_x: float = -5.0,
    max_glitch_x: float = 125.0,
    min_glitch_y: float = -5.0,
    max_glitch_y: float = 85.0
    ) -> DataFrame:
    """
    Sanitizes spatial coordinates. Values outside pitch tolerance bounds
    ([-5, 125] in X, [-5, 85] in Y) are filtered out and replaced with NULL.
    """
    exprs = []
    for col_name in df.columns:
        if col_name.endswith("_x"):
            exprs.append(
                F.when(F.col(col_name).isNull(), F.lit(None))
                .when((F.col(col_name) < min_glitch_x) | (F.col(col_name) > max_glitch_x), F.lit(None))
                .otherwise(F.col(col_name))
                .alias(col_name)
            )
        elif col_name.endswith("_y"):
            exprs.append(
                F.when(F.col(col_name).isNull(), F.lit(None))
                .when((F.col(col_name) < min_glitch_y) | (F.col(col_name) > max_glitch_y), F.lit(None))
                .otherwise(F.col(col_name))
                .alias(col_name)
            )
        else:
            exprs.append(F.col(col_name))

    return df.select(*exprs)


def clean_silver_dataframe(df: DataFrame) -> DataFrame:
    """
    Standard chaining suite for all Silver event tables:
    1. Flattens nested structs recursively.
    2. Splits end locations into X, Y, Z.
    3. Cleans out-of-bounds coordinate sensor glitches.
    4. Replaces null booleans with False.
    """
    return (
        df
        .transform(flatten_dataframe)
        .transform(end_location_split)
        .transform(clean_glitch_location)
        .transform(fill_na_bool_columns)
    )


def finalize_silver_dataframe(df: DataFrame, pipeline_name: str = "silver_transformation") -> DataFrame:
    """
    Appends audit and lineage timestamps, then orders domain columns before metadata.
    """
    df_metadata = (
        df
        .withColumn("_silver_inserted_at", F.current_timestamp())
        .withColumn("_silver_updated_at", F.current_timestamp())
        .withColumn("_source_pipeline", F.lit(pipeline_name))
    )

    common_cols = [c for c in df_metadata.columns if not c.startswith("_")]
    metadata_cols = [c for c in df_metadata.columns if c.startswith("_")]

    return df_metadata.select(*common_cols, *metadata_cols)
