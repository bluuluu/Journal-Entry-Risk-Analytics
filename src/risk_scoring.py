from __future__ import annotations

import argparse
from pathlib import Path

import numpy as np
import pandas as pd


KEYWORDS = ["gift", "manual", "write-off", "write off"]


def load_data(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, parse_dates=["posting_date", "posting_timestamp"])
    expected_cols = {
        "entry_id",
        "entity",
        "je_number",
        "line_num",
        "account",
        "offset_account",
        "description",
        "amount",
        "currency",
        "debit_credit",
        "posting_date",
        "posting_timestamp",
        "time_zone",
        "created_by",
        "source",
        "approval_status",
    }
    missing = expected_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    return df


def add_time_features(df: pd.DataFrame) -> pd.DataFrame:
    df["local_hour"] = df.apply(
        lambda r: pd.Timestamp(r["posting_timestamp"]).tz_localize(str(r["time_zone"])).hour,
        axis=1,
    )
    df["dow"] = df["posting_date"].dt.dayofweek
    return df


def compute_amount_stats(df: pd.DataFrame) -> pd.DataFrame:
    df["abs_amount"] = df["amount"].abs()
    stats = (
        df.groupby("account")["abs_amount"]
        .agg(["mean", "std"])
        .rename(columns={"mean": "avg_abs_amount", "std": "std_abs_amount"})
    )
    df = df.join(stats, on="account")
    df["amount_z"] = (df["abs_amount"] - df["avg_abs_amount"]) / df["std_abs_amount"].replace(0, np.nan)
    df["amount_z"] = df["amount_z"].fillna(0)
    df["amount_outlier_flag"] = (df["amount_z"] >= 2).astype(int)
    df["round_dollar_flag"] = ((df["abs_amount"] % 100) == 0).astype(int)
    return df


def compute_user_volume(df: pd.DataFrame) -> pd.DataFrame:
    df["month_start"] = df["posting_date"].values.astype("datetime64[M]")
    per_user_month = df.groupby(["created_by", "month_start"])["entry_id"].nunique()
    stats = per_user_month.groupby("created_by").agg(["mean", "std"]).rename(
        columns={"mean": "avg_entries", "std": "std_entries"}
    )
    z_scores = (
        per_user_month.reset_index()
        .join(stats, on="created_by")
        .assign(entry_volume_z=lambda x: (x["entry_id"] - x["avg_entries"]) / x["std_entries"].replace(0, np.nan))
    )
    z_scores["entry_volume_z"] = z_scores["entry_volume_z"].fillna(0)
    df = df.merge(
        z_scores[["created_by", "month_start", "entry_volume_z"]],
        on=["created_by", "month_start"],
        how="left",
    )
    df["user_volume_outlier_flag"] = (df["entry_volume_z"] >= 2).astype(int)
    return df


def compute_pair_rarity(df: pd.DataFrame) -> pd.DataFrame:
    pair_counts = df.groupby(["account", "offset_account"]).size().rename("pair_count")
    df = df.join(pair_counts, on=["account", "offset_account"])
    # Flag rare pairs (single occurrence within account or bottom quartile).
    df["rare_pair_flag"] = 0
    for account, subset in df.groupby("account"):
        counts = subset["pair_count"]
        threshold = np.percentile(counts, 25)
        df.loc[subset.index, "rare_pair_flag"] = ((counts <= threshold) | (counts == 1)).astype(int)
    return df


def compute_rule_flags(df: pd.DataFrame) -> pd.DataFrame:
    entity_hours = {
        "US": {"start": 8, "end": 18, "weekend_days": {5, 6}},
        "UK": {"start": 8, "end": 18, "weekend_days": {5, 6}},
        "APAC": {"start": 9, "end": 18, "weekend_days": {5, 6}},
    }
    def hours_for_entity(entity: str) -> dict:
        return entity_hours.get(entity, {"start": 8, "end": 18, "weekend_days": {5, 6}})

    df["after_hours_flag"] = df.apply(
        lambda row: int(
            (row["local_hour"] < hours_for_entity(row["entity"])["start"])
            or (row["local_hour"] >= hours_for_entity(row["entity"])["end"])
        ),
        axis=1,
    )

    df["weekend_flag"] = df.apply(
        lambda row: int(row["dow"] in hours_for_entity(row["entity"])["weekend_days"]), axis=1
    )
    df["period_close_flag"] = (
        df["posting_date"] >= (df["posting_date"].values.astype("datetime64[M]") + np.timedelta64(25, "D"))
    ).astype(int)
    df["approval_pending_flag"] = (~df["approval_status"].str.lower().isin(["approved", "posted"])).astype(int)
    df["keyword_flag"] = df["description"].str.lower().apply(
        lambda x: int(any(k in x for k in KEYWORDS))
    )
    return df


def compute_risk_score(df: pd.DataFrame) -> pd.DataFrame:
    weights = {
        "round_dollar_flag": 10,
        "after_hours_flag": 15,
        "weekend_flag": 10,
        "period_close_flag": 10,
        "approval_pending_flag": 10,
        "keyword_flag": 5,
        "rare_pair_flag": 15,
        "amount_outlier_flag": 20,
        "user_volume_outlier_flag": 15,
    }
    score = np.zeros(len(df))
    for flag, weight in weights.items():
        score += df.get(flag, 0) * weight
    df["risk_score"] = np.clip(score, 0, 100)
    return df


def run(input_path: Path, output_path: Path) -> None:
    df = load_data(input_path)
    df = add_time_features(df)
    df = compute_amount_stats(df)
    df = compute_user_volume(df)
    df = compute_pair_rarity(df)
    df = compute_rule_flags(df)
    df = compute_risk_score(df)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"Wrote risk-scored entries to {output_path}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Journal Entry Risk Scoring")
    parser.add_argument("--input", type=Path, required=True, help="Path to GL CSV input")
    parser.add_argument("--output", type=Path, required=True, help="Path for risk score CSV output")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.input, args.output)
