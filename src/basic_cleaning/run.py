#!/usr/bin/env python
"""
Performs basic cleaning on the data and save the results in Weights & Biases
"""
import argparse
import logging
import wandb
import os
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    logger.info("Downloading artifact")
    artifact_path = run.use_artifact(args.input_artifact).file()

    df = pd.read_csv(artifact_path)

    logger.info("fillna for 'reviews_per_month'")
    df['reviews_per_month'].fillna(value=0, inplace=True)

    logger.info(f"Drop outliers 'price' outside of {args.min_price} and {args.max_price}")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    logger.info("Convert 'last_review' to datetime")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # test_proper_boundaries
    idx = df['longitude'].between(-74.25, -73.50) & df['latitude'].between(40.5, 41.2)
    df = df[idx].copy()

    filename = "processed_data.csv"
    df.to_csv(filename, index=False)

    artifact = wandb.Artifact(
        name=args.artifact_name,
        type=args.artifact_type,
        description=args.artifact_description,
    )

    artifact.add_file(filename)

    logger.info("Logging artifact")
    run.log_artifact(artifact)

    os.remove(filename)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Mandated step to clean data")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Fully-qualified name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--artifact_name", type=str, help="Name for the artifact", required=True
    )

    parser.add_argument(
        "--artifact_type", type=str, help="Type for the artifact", required=True
    )

    parser.add_argument(
        "--artifact_description",
        type=str,
        help="Description for the artifact",
        required=True,
    )

    parser.add_argument(
        "--min_price",
        type=int,
        help='Minimum price of the rental',
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=int,
        help='Maximum price of the rental',
        required=True
    )

    call_args = parser.parse_args()

    go(call_args)
