"""
Silver Layer Transformation Engine
==================================
Transforms raw Bronze event streams, lineups, and 360 freeze frame tracking into
typed, cleansed, and domain-enriched Silver Delta Lake tables.

Silver Tables Produced:
1. silver_base_events: Universal spine of all match events.
2. silver_pass: Passes with tactical completion flags and final third penetration.
3. silver_ball_receipt: Ball control and receipt attempts.
4. silver_shot: Shots with calculated distance, xG, and goal flags.
5. silver_shot_freeze_frame: Exploded defensive/keeper positioning at moment of shot.
6. silver_carry: Ball carries with Euclidean displacement distance.
7. silver_defensive_action: Tackles, interceptions, clearances, blocks, and pressures.
8. silver_match_lineups: Squad rosters, jersey numbers, and starter status.
9. silver_match_player_positions: Time-interval position tracking per player.
10. silver_360_frames: Exploded player tracking telemetry coordinates (X, Y, Actor, Keeper).
"""

import logging
from typing import List, Dict, Union, Tuple
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable

from src.common import (
    get_common_columns,
    clean_silver_dataframe,
    clean_glitch_location,
    fill_na_bool_columns,
    finalize_silver_dataframe,
)

logger = logging.getLogger(__name__)

DEFENSIVE_ACTIONS = [
    "50/50",
    "Block",
    "Clearance",
    "Duel",
    "Foul Committed",
    "Interception",
    "Pressure",
]


def upsert_delta_table(
    df: DataFrame,
    table_name: str,
    join_keys: Union[List[str], Dict[str, str]],
    partition_cols: Union[str, List[str]],
    spark: SparkSession,
    ) -> None:
    """
    Universal Delta Lake Upsert (ACID MERGE INTO).
    Prevents duplicates on re-runs while maintaining partition layout.
    """
    if isinstance(join_keys, dict):
        join_condition = " AND ".join(
            [f"target.{t} = source.{s}" for t, s in join_keys.items()]
        )
    elif isinstance(join_keys, list):
        join_condition = " AND ".join([f"target.{k} = source.{k}" for k in join_keys])
    else:
        raise ValueError("join_keys must be a list or dict")

    if spark.catalog.tableExists(table_name):
        (
            DeltaTable.forName(spark, table_name)
            .alias("target")
            .merge(df.alias("source"), join_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"Successfully upserted data into Delta table: {table_name}")
    else:
        (
            df.write.format("delta")
            .mode("overwrite")
            .partitionBy(partition_cols)
            .saveAsTable(table_name)
        )
        logger.info(f"Created new Delta table: {table_name} (partitioned by '{partition_cols}')")


def transform_silver_base_events(df_bronze_events: DataFrame) -> DataFrame:
    """Extracts and standardizes the universal base event table."""
    return (
        df_bronze_events
        .select(*get_common_columns())
        .transform(clean_silver_dataframe)
        .transform(lambda df: finalize_silver_dataframe(df, "silver_base_events"))
    )


def transform_silver_pass(df_bronze_events: DataFrame) -> DataFrame:
    """
    Transforms passes with tactical domain features:
    - is_pass_complete (Pass outcome null check)
    - is_pass_to_final_third (X crosses into pitch >= 80 yards)
    - is_pass_into_penalty_box (X >= 102, 18 <= Y <= 62)
    """
    return (
        df_bronze_events
        .filter(F.col("type.name") == "Pass")
        .select(*get_common_columns(), F.col("pass"))
        .transform(clean_silver_dataframe)
        .drop("pass_end_z")
        .withColumn("is_pass_complete", F.col("pass_outcome_name").isNull())
        .fillna({"pass_outcome_name": "Complete"})
        .withColumn(
            "is_pass_to_final_third",
            (F.col("start_x") < 80.0) & (F.col("pass_end_x") >= 80.0),
        )
        .withColumn(
            "is_pass_into_penalty_box",
            (F.col("pass_end_x") >= 102.0)
            & (F.col("pass_end_y") >= 18.0)
            & (F.col("pass_end_y") <= 62.0),
        )
        .transform(lambda df: finalize_silver_dataframe(df, "silver_pass"))
    )


def transform_silver_ball_receipt(df_bronze_events: DataFrame) -> DataFrame:
    """Extracts ball receipts and sets receipt outcome completion."""
    return (
        df_bronze_events
        .filter(F.col("type.name") == "Ball Receipt*")
        .select(*get_common_columns(), F.col("ball_receipt").alias("receipt"))
        .transform(clean_silver_dataframe)
        .withColumn("is_receipt_complete", F.col("receipt_outcome_name").isNull())
        .fillna({"receipt_outcome_name": "Complete"})
        .transform(lambda df: finalize_silver_dataframe(df, "silver_ball_receipt"))
    )


def transform_silver_shot(df_bronze_events: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """
    Extracts shots with calculated Euclidean shot distance and goal status.
    Returns:
        Tuple[DataFrame, DataFrame]:
            - df_shot: Flat shots without nested freeze frames.
            - df_shot_freeze_frame: Exploded player coordinates at shot instant.
    """
    df_raw_shot = (
        df_bronze_events
        .filter(F.col("type.name") == "Shot")
        .select(*get_common_columns(), F.col("shot"))
        .transform(clean_silver_dataframe)
        .withColumn("is_goal", F.col("shot_outcome_name") == "Goal")
        .withColumn(
            "shot_distance",
            F.sqrt(
                F.pow(F.col("shot_end_x") - F.col("start_x"), 2)
                + F.pow(F.col("shot_end_y") - F.col("start_y"), 2)
            ),
        )
    )

    df_shot = (
        df_raw_shot
        .drop("shot_freeze_frame")
        .transform(lambda df: finalize_silver_dataframe(df, "silver_shot"))
    )

    df_shot_freeze_frame = (
        df_raw_shot
        .withColumn("ff", F.explode_outer("shot_freeze_frame"))
        .select(
            F.col("competition_id"),
            F.col("match_id"),
            F.col("index"),
            F.col("event_id"),
            F.col("ff.player.id").alias("freeze_player_id"),
            F.col("ff.player.name").alias("freeze_player_name"),
            F.col("ff.position.id").alias("freeze_position_id"),
            F.col("ff.position.name").alias("freeze_position_name"),
            F.col("ff.teammate").alias("is_teammate"),
            (F.col("ff.position.id") == 1).alias("is_keeper"),
            F.get(F.col("ff.location"), 0).alias("freeze_x"),
            F.get(F.col("ff.location"), 1).alias("freeze_y"),
        )
        .transform(lambda df: finalize_silver_dataframe(df, "silver_shot_freeze_frame"))
    )

    return df_shot, df_shot_freeze_frame


def transform_silver_carry(df_bronze_events: DataFrame) -> DataFrame:
    """Calculates ball carry Euclidean distance displacement."""
    return (
        df_bronze_events
        .filter(F.col("type.name") == "Carry")
        .select(*get_common_columns(), F.col("carry"))
        .transform(clean_silver_dataframe)
        .withColumn(
            "carry_distance",
            F.sqrt(
                F.pow(F.col("carry_end_x") - F.col("start_x"), 2)
                + F.pow(F.col("carry_end_y") - F.col("start_y"), 2)
            ),
        )
        .drop("carry_end_z")
        .transform(lambda df: finalize_silver_dataframe(df, "silver_carry"))
    )


def transform_silver_defensive_action(df_bronze_events: DataFrame) -> DataFrame:
    """Standardizes defensive interventions (pressures, tackles, blocks, clearances)."""
    return (
        df_bronze_events
        .filter(F.col("type.name").isin(DEFENSIVE_ACTIONS))
        .select(
            *get_common_columns(),
            F.col("duel"),
            F.col("interception"),
            F.col("clearance"),
            F.col("block"),
            F.col("50_50").alias("fifty_fifty"),
            F.col("foul_committed"),
        )
        .transform(clean_silver_dataframe)
        .transform(lambda df: finalize_silver_dataframe(df, "silver_defensive_action"))
    )


def transform_silver_match_lineups(df_bronze_lineups: DataFrame) -> DataFrame:
    """Unpacks match rosters, starter indicator, and jersey numbers."""
    return (
        df_bronze_lineups
        .select(
            F.col("competition_id"),
            F.col("match_id"),
            F.col("team_id"),
            F.col("team_name"),
            F.explode_outer("lineup").alias("player"),
            F.col("_file_path"),
        )
        .select(
            F.col("competition_id"),
            F.col("match_id"),
            F.col("team_id"),
            F.col("team_name"),
            F.col("player.player_id").alias("player_id"),
            F.col("player.player_name").alias("player_name"),
            F.col("player.player_nickname").alias("player_nickname"),
            F.col("player.jersey_number").alias("jersey_number"),
            F.col("player.country.name").alias("country_name"),
            (F.size(F.col("player.positions")) > 0).alias("has_played"),
            (
                F.coalesce(
                    F.get(F.col("player.positions"), 0).getField("start_reason"),
                    F.lit(""),
                )
                == "Starting XI"
            ).alias("is_starter"),
            F.get(F.col("player.positions"), 0).getField("position_id").alias("starting_position_id"),
            F.get(F.col("player.positions"), 0).getField("position").alias("starting_position_name"),
            F.col("_file_path"),
        )
        .transform(clean_silver_dataframe)
        .transform(lambda df: finalize_silver_dataframe(df, "silver_match_lineups"))
    )


def transform_silver_match_player_positions(df_bronze_lineups: DataFrame) -> DataFrame:
    """Explodes tactical position shift intervals across match halves."""
    base_cols = [
        "competition_id",
        "match_id",
        "team_id",
        "team_name",
        "player_id",
        "player_name",
        "jersey_number",
    ]

    return (
        df_bronze_lineups
        .select(
            F.col("competition_id"),
            F.col("match_id"),
            F.col("team_id"),
            F.col("team_name"),
            F.explode_outer("lineup").alias("player"),
        )
        .filter(F.size(F.col("player.positions")) > 0)
        .select(
            F.col("competition_id"),
            F.col("match_id"),
            F.col("team_id"),
            F.col("team_name"),
            F.col("player.player_id").alias("player_id"),
            F.col("player.player_name").alias("player_name"),
            F.col("player.jersey_number").alias("jersey_number"),
            F.explode_outer(F.col("player.positions")).alias("pp"),
        )
        .select(
            *base_cols,
            F.col("pp.position_id").alias("position_id"),
            F.col("pp.position").alias("position_name"),
            F.col("pp.from").alias("from_time"),
            F.col("pp.to").alias("to_time"),
            F.col("pp.from_period").alias("from_period"),
            F.col("pp.to_period").alias("to_period"),
            F.col("pp.start_reason").alias("start_reason"),
            F.col("pp.end_reason").alias("end_reason"),
        )
        .transform(clean_silver_dataframe)
        .dropDuplicates(["match_id", "player_id", "position_id", "from_time", "from_period"])
        .transform(lambda df: finalize_silver_dataframe(df, "silver_match_player_positions"))
    )


def transform_silver_360_frames(df_bronze_360: DataFrame) -> DataFrame:
    """
    Unpacks high-frequency 360 freeze frame tracking coordinates.
    Extracts individual player X, Y coordinates, keeper flags, and actor status.
    """
    return (
        df_bronze_360
        .select(
            F.col("competition_id"),
            F.col("match_id"),
            F.col("event_uuid").alias("event_id"),
            F.col("visible_area"),
            F.posexplode_outer("freeze_frame").alias("player_track_id", "ff"),
            F.col("_file_path"),
        )
        .select(
            F.col("competition_id"),
            F.col("match_id"),
            F.col("event_id"),
            F.col("visible_area"),
            F.col("player_track_id"),
            F.col("ff.teammate").alias("is_teammate"),
            F.col("ff.actor").alias("is_actor"),
            F.col("ff.keeper").alias("is_keeper"),
            F.get(F.col("ff.location"), 0).alias("player_x"),
            F.get(F.col("ff.location"), 1).alias("player_y"),
            F.col("_file_path"),
        )
        .transform(clean_glitch_location)
        .transform(fill_na_bool_columns)
        .transform(lambda df: finalize_silver_dataframe(df, "silver_360_frames"))
    )


def run_silver_pipeline(spark: SparkSession, dest_schema: str = "football_project") -> None:
    """
    Executes the end-to-end Silver Layer pipeline:
    Ingests all Bronze tables, runs domain transformations, and upserts Delta tables.
    """
    logger.info("==================================================")
    logger.info("Starting Silver Layer Lakehouse Pipeline...")
    logger.info("==================================================")

    # 1. Load Bronze Source Tables
    df_bronze_events = spark.table(f"workspace.{dest_schema}.bronze_events")
    df_bronze_lineups = spark.table(f"workspace.{dest_schema}.bronze_lineups")
    df_bronze_360 = spark.table(f"workspace.{dest_schema}.bronze_three_sixty")

    # 2. Base Events
    df_silver_base = transform_silver_base_events(df_bronze_events)
    upsert_delta_table(df_silver_base, f"workspace.{dest_schema}.silver_base_events", ["match_id", "event_id"], "competition_id", spark)

    # 3. Categorized Event Tables
    df_pass = transform_silver_pass(df_bronze_events)
    upsert_delta_table(df_pass, f"workspace.{dest_schema}.silver_pass", ["match_id", "event_id"], "competition_id", spark)

    df_receipt = transform_silver_ball_receipt(df_bronze_events)
    upsert_delta_table(df_receipt, f"workspace.{dest_schema}.silver_ball_receipt", ["match_id", "event_id"], "competition_id", spark)

    df_shot, df_shot_ff = transform_silver_shot(df_bronze_events)
    upsert_delta_table(df_shot, f"workspace.{dest_schema}.silver_shot", ["match_id", "event_id"], "competition_id", spark)
    upsert_delta_table(df_shot_ff, f"workspace.{dest_schema}.silver_shot_freeze_frame", ["match_id", "event_id", "freeze_player_id"], "competition_id", spark)

    df_carry = transform_silver_carry(df_bronze_events)
    upsert_delta_table(df_carry, f"workspace.{dest_schema}.silver_carry", ["match_id", "event_id"], "competition_id", spark)

    df_defend = transform_silver_defensive_action(df_bronze_events)
    upsert_delta_table(df_defend, f"workspace.{dest_schema}.silver_defensive_action", ["match_id", "event_id"], "competition_id", spark)

    # 4. Lineup Tables
    df_lineups = transform_silver_match_lineups(df_bronze_lineups)
    upsert_delta_table(df_lineups, f"workspace.{dest_schema}.silver_match_lineups", ["match_id", "player_id"], "competition_id", spark)

    df_positions = transform_silver_match_player_positions(df_bronze_lineups)
    upsert_delta_table(df_positions, f"workspace.{dest_schema}.silver_match_player_positions", ["match_id", "player_id", "position_id", "from_time", "from_period"], "competition_id", spark)

    # 5. High-Frequency Tracking Telemetry (360)
    df_360 = transform_silver_360_frames(df_bronze_360)
    upsert_delta_table(df_360, f"workspace.{dest_schema}.silver_360_frames", ["match_id", "event_id", "player_track_id"], "competition_id", spark)

    logger.info("==================================================")
    logger.info("🏆 Silver Layer Pipeline Completed Successfully!")
    logger.info("==================================================")
