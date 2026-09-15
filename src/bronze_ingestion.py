"""
Bronze Layer Ingestion Engine
=============================
Ingests raw multi-line JSON telemetry and event feeds from StatsBomb Open Data
into partitioned Delta Lake tables with idempotent replaceWhere guarantees.

Layers Handled:
- bronze_matches: Match metadata, competition info, season lookup.
- bronze_events: Raw pitch event logs.
- bronze_lineups: Squad and starter rosters.
- bronze_three_sixty: High-frequency 360-degree freeze frame tracking coordinates.
"""

import os
import logging
from typing import List, Optional
import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)


def get_standard_match_columns() -> List:
    """
    Returns standard column expressions for extracting match metadata.
    Includes lineage and audit columns (_file_name, _load_dt, _load_dttm).
    """
    return [
        # Match identifiers & scores
        F.col("match_id").cast("string").alias("match_id"),
        F.col("match_date").cast("date").alias("match_date"),
        F.col("kick_off").alias("kick_off"),
        F.col("home_score").cast("int").alias("home_score"),
        F.col("away_score").cast("int").alias("away_score"),

        # Competition & Season
        F.col("competition.competition_id").cast("int").alias("competition_id"),
        F.col("competition.competition_name").alias("competition_name"),
        F.col("competition.country_name").alias("country_name"),
        F.col("season.season_id").cast("int").alias("season_id"),
        F.col("season.season_name").alias("season_name"),

        # Teams
        F.col("home_team.home_team_id").alias("home_team_id"),
        F.col("home_team.home_team_name").alias("home_team_name"),
        F.col("away_team.away_team_id").alias("away_team_id"),
        F.col("away_team.away_team_name").alias("away_team_name"),

        # Status
        F.col("match_status").alias("match_status"),
        F.col("match_status_360").alias("match_status_360"),

        # Audit & Lineage Metadata
        F.col("_metadata.file_name").alias("_file_name"),
        F.col("_metadata.file_path").alias("_file_path"),
        F.current_date().alias("_load_dt"),
        F.current_timestamp().alias("_load_dttm")
    ]


def ingest_matches_bronze(
    spark: SparkSession,
    competition_id: int,
    raw_base_dir: str,
    dest_schema: str = "football_project",
    force_refresh: bool = False
) -> Optional[DataFrame]:
    """
    Ingests match metadata JSON files for a given competition into bronze_matches.
    Uses Delta Lake partition-level overwrite (replaceWhere) for idempotency.
    """
    comp_dir = os.path.join(raw_base_dir, "matches", str(competition_id))
    if not os.path.exists(comp_dir):
        logger.error(f"Competition directory does not exist: {comp_dir}")
        return None

    matches_table = f"workspace.{dest_schema}.bronze_matches"
    
    # Check if competition already ingested
    if spark.catalog.tableExists(matches_table) and not force_refresh:
        existing_count = (
            spark.table(matches_table)
            .filter(F.col("competition_id") == competition_id)
            .limit(1)
            .count()
        )
        if existing_count > 0:
            logger.info(f"Competition {competition_id} already in Bronze. Skipping.")
            return spark.table(matches_table).filter(F.col("competition_id") == competition_id)

    season_files = [
        os.path.join(comp_dir, f) for f in os.listdir(comp_dir) if f.endswith(".json")
    ]
    if not season_files:
        logger.error(f"No season JSON files found in {comp_dir}")
        return None

    logger.info(f"Ingesting {len(season_files)} season files for Competition ID: {competition_id}")

    matches_df = (
        spark.read
        .option("multiLine", True)
        .json(season_files)
        .select(*get_standard_match_columns())
    )

    # Write to Delta table with partition pruning and schema merging
    (
        matches_df.write
        .format("delta")
        .mode("overwrite")
        .partitionBy("competition_id")
        .option("replaceWhere", f"competition_id = {competition_id}")
        .option("mergeSchema", "true")
        .saveAsTable(matches_table)
    )
    logger.info(f"Saved {matches_table} for Competition {competition_id}")

    return matches_df


def ingest_feeds_bronze(
    spark: SparkSession,
    competition_id: int,
    matches_df: DataFrame,
    raw_base_dir: str,
    dest_schema: str = "football_project",
    feeds: Optional[List[str]] = None
) -> None:
    """
    Ingests event, lineup, and 360 freeze frame feeds matching the discovered match IDs.
    Applies Broadcast Hash Join to attach competition/season lineage with 0 MB network shuffle.
    """
    if feeds is None:
        feeds = ["events", "lineups", "three_sixty"]

    match_lookup_df = matches_df.select("match_id", "competition_id", "season_id").distinct()
    match_ids = [row.match_id for row in match_lookup_df.collect()]
    logger.info(f"Discovered {len(match_ids)} total matches to ingest.")

    for feed in feeds:
        feed_files = [
            os.path.join(raw_base_dir, feed, f"{m_id}.json")
            for m_id in match_ids
            if os.path.exists(os.path.join(raw_base_dir, feed, f"{m_id}.json"))
        ]

        if not feed_files:
            logger.warning(f"No files found for feed '{feed}'. Skipping.")
            continue

        logger.info(f"Ingesting {len(feed_files)} files for feed: {feed.upper()}...")

        feed_df = (
            spark.read
            .option("multiLine", True)
            .json(feed_files)
            .withColumn("match_id", F.regexp_extract(F.col("_metadata.file_name"), r"(\d+)", 1))
            .withColumn("_file_name", F.col("_metadata.file_name"))
            .withColumn("_file_path", F.col("_metadata.file_path"))
            .withColumn("_load_dttm", F.current_timestamp())
            .withColumn("_load_dt", F.current_date())
        )

        # Broadcast join lookup metadata (zero shuffle overhead)
        broadcast_feed_df = feed_df.join(
            F.broadcast(match_lookup_df),
            on="match_id",
            how="inner"
        )

        target_table = f"workspace.{dest_schema}.bronze_{feed}"
        (
            broadcast_feed_df.write
            .format("delta")
            .mode("overwrite")
            .partitionBy("competition_id")
            .option("replaceWhere", f"competition_id = {competition_id}")
            .option("mergeSchema", "true")
            .saveAsTable(target_table)
        )
        logger.info(f"Successfully ingested {target_table} [Competition: {competition_id}]")


def ingest_competition_bronze(
    spark: SparkSession,
    competition_id: int,
    raw_base_dir: str = "/Volumes/workspace/football_project/raw_data",
    dest_schema: str = "football_project",
    force_refresh: bool = False
) -> None:
    """
    Master pipeline orchestrator for Bronze Layer ingestion.
    Coordinates matches discovery, Delta upsert, and multi-feed broadcast ingestion.
    """
    logger.info(f"==================================================")
    logger.info(f"Starting Bronze Ingestion: Competition ID {competition_id}")
    logger.info(f"==================================================")

    matches_df = ingest_matches_bronze(
        spark=spark,
        competition_id=competition_id,
        raw_base_dir=raw_base_dir,
        dest_schema=dest_schema,
        force_refresh=force_refresh
    )

    if matches_df is None:
        logger.error(f"Aborting feed ingestion: Failed to load matches for Competition {competition_id}")
        return

    ingest_feeds_bronze(
        spark=spark,
        competition_id=competition_id,
        matches_df=matches_df,
        raw_base_dir=raw_base_dir,
        dest_schema=dest_schema
    )

    logger.info(f"Bronze Ingestion Pipeline Finished for Competition {competition_id}!")
