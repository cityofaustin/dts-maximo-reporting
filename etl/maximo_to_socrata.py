import argparse
from datetime import datetime, timedelta
import os
import logging

import dateutil.parser
import snowflake.connector
from snowflake.connector import DictCursor
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend
from sodapy import Socrata
from tqdm import tqdm

from queries import QUERIES, maximo_url_search_params
import utils

# Snowflake DB Credentials (replaces Maximo Oracle DB credentials)
SF_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SF_USER = os.getenv("SNOWFLAKE_USER")
# Full PEM text of the private key (-----BEGIN ... -----END)
SF_PRIVATE_KEY = os.getenv("SNOWFLAKE_PRIVATE_KEY")
SF_PRIVATE_KEY_PASSPHRASE = os.getenv("SNOWFLAKE_PRIVATE_KEY_PASSPHRASE")
SF_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SF_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SF_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")
SF_ROLE = os.getenv("SNOWFLAKE_ROLE")

BASE_URL = os.getenv("MAXIMO_BASE_URL")

# Socrata Secrets
SO_WEB = os.getenv("SO_WEB")
SO_TOKEN = os.getenv("SO_TOKEN")
SO_KEY = os.getenv("SO_KEY")
SO_SECRET = os.getenv("SO_SECRET")

SOCRATA_DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def process_date_arguments(args):
    if args.start:
        start = dateutil.parser.parse(args.start)
    if not args.start:
        # default to 7 days ago
        start = datetime.now() - timedelta(days=7)

    if args.end:
        end = dateutil.parser.parse(args.end)
    if not args.end:
        # default to today
        end = datetime.now()

    return datetime.strftime(start, "%m/%d/%Y"), datetime.strftime(end, "%m/%d/%Y")


def load_private_key(key_content, passphrase=None):
    """
    Load an RSA private key from PEM text (e.g. pulled from 1Password into
    an env var) and return it in the DER/PKCS8 format the Snowflake
    connector expects. The key is never written to disk.

    Parameters
    ----------
    key_content : str, full PEM text of the private key
    passphrase : str or None, passphrase the key was encrypted with

    Returns
    -------
    bytes: DER-encoded, unencrypted private key
    """
    if not key_content:
        raise ValueError("SNOWFLAKE_PRIVATE_KEY env var is not set or is empty")

    # Some secret injectors flatten newlines into literal "\n" sequences.
    # Restore real line breaks so the PEM parses.
    key_content = key_content.replace("\\n", "\n")

    p_key = serialization.load_pem_private_key(
        key_content.encode(),
        password=passphrase.encode() if passphrase else None,
        backend=default_backend(),
    )
    return p_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )


def get_conn():
    """
    Get connected to the Snowflake data warehouse using key-pair auth.

    Returns
    -------
    snowflake.connector Connection Object

    """
    try:
        pkb = load_private_key(SF_PRIVATE_KEY, SF_PRIVATE_KEY_PASSPHRASE)
        return snowflake.connector.connect(
            account=SF_ACCOUNT,
            user=SF_USER,
            private_key=pkb,
            warehouse=SF_WAREHOUSE,
            database=SF_DATABASE,
            schema=SF_SCHEMA,
            role=SF_ROLE,
        )
    except Exception as e:
        # Don't fail silently -- this runs unattended.
        logger.error(f"Failed to connect to Snowflake: {e}")
        raise


def transform_datetime_columns(data):
    """
    Transform datetime columns to the format expected by Socrata.
    Parameters
    ----------
    data: list of dicts of the data fetched from Snowflake.

    Returns
    -------
    data: list of dicts with the dates modified.

    """
    for row in data:
        for key in row:
            # converts all datetime objects to the correct format.
            if isinstance(row[key], datetime):
                row[key] = row[key].strftime(SOCRATA_DATE_FORMAT)

    return data


def cleanup_work_order_urls(data):
    """
    Replaces spaces with the proper URL encoding from work order URLs.
    Socrata will silently reject URLs with spaces.
    ----------
    data: list of dicts of the data fetched from Snowflake.

    Returns
    -------
    data: list of dicts with the urls cleaned up.

    """
    for row in data:
        # Only a few work orders have spaces in them.
        if " " in row["WONUM"]:
            row["WO_LINK"] = row["WO_LINK"].replace(" ", "%20")

    return data


def data_to_socrata(soda, data, dataset, batch_size=1000, show_progress=False):
    """
    Replaces all the data in the socrata dataset with data in the dataframe,
    uploading in batches to avoid oversized payloads/timeouts.

    Parameters
    ----------
    soda : sodapy client object
    data : list of dicts from Snowflake
    dataset : str, Socrata dataset (four-by-four) ID
    batch_size : int, number of rows per batch (default 1000)
    show_progress : bool, whether to display a tqdm progress bar (default False).
        When False, batch progress is logged instead via the `logging` module.

    Returns
    -------
    list of response dicts, one per batch
    """
    results = []
    total_batches = (len(data) + batch_size - 1) // batch_size

    logger.info(
        f"Uploading {len(data)} rows to Socrata dataset {dataset} in {total_batches} batch(es) of {batch_size}"
    )

    iterator = range(0, len(data), batch_size)
    if show_progress:
        iterator = tqdm(iterator, total=total_batches, desc="Uploading to Socrata")

    for batch_num, i in enumerate(iterator, start=1):
        batch = data[i : i + batch_size]
        res = soda.upsert(dataset, batch)
        results.append(res)

        if not show_progress:
            logger.info(
                f"Batch {batch_num}/{total_batches} uploaded ({len(batch)} rows)"
            )

    logger.info(f"Finished uploading {len(data)} rows to Socrata dataset {dataset}")

    return results


def main(args):
    # process CLI args
    start, end = process_date_arguments(args)

    # Connect to Snowflake
    conn = get_conn()
    cursor = conn.cursor(DictCursor)

    query_template = QUERIES[args.query]["template"]
    query_params = QUERIES[args.query]["query_params"]
    socrata_resource_id = QUERIES[args.query]["dataset_resource_id"]

    if query_params is None:
        query = query_template
    elif "base_url" in query_params:
        # Building the direct url for work orders
        work_order_base_url = BASE_URL + maximo_url_search_params
        query = query_template.format(
            base_url=work_order_base_url, start=start, end=end
        )
        logger.info(f"Getting data with start: {start}, end: {end}")
    elif "start" in query_params and "end" in query_params:
        query = query_template.format(start=start, end=end)
        logger.info(f"Getting data with start: {start}, end: {end}")
    else:
        raise ValueError("Invalid query parameters")

    # Execute query
    cursor.execute(query)
    rows = cursor.fetchall()

    if rows:
        logger.info(f"{len(rows)} records found")
        # Convert datetime fields to format accepted by Socrata
        rows = transform_datetime_columns(rows)

        # Cleaning up URL encoding (only for work orders)
        if args.query == "work_orders":
            rows = cleanup_work_order_urls(rows)

        # Upsert to Socrata
        soda = Socrata(
            SO_WEB,
            SO_TOKEN,
            username=SO_KEY,
            password=SO_SECRET,
            timeout=30,
        )
        res = data_to_socrata(
            soda, rows, socrata_resource_id, show_progress=args.progress
        )
    else:
        logger.info("No records found")
    conn.close()


# CLI argument definition
parser = argparse.ArgumentParser()

parser.add_argument(
    "--start",
    type=str,
    required=False,
    help="Start modified date of the work orders to download, defaults to 7 days ago.",
)

parser.add_argument(
    "--end",
    type=str,
    required=False,
    help="End changed date of the work orders to download, defaults to now.",
)

parser.add_argument(
    "--query",
    choices=list(QUERIES.keys()),
    required=True,
    help="Name of the query defined in queries.py. Ex: work_orders",
)

parser.add_argument(
    "-p",
    "--progress",
    action="store_true",
    help="Show a progress bar while uploading to Socrata",
)

args = parser.parse_args()

logger = utils.get_logger(
    __name__,
    level=logging.INFO,
)

if __name__ == "__main__":
    main(args)
