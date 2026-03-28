"""
stage03_transform_sgolla.py
(EDIT YOUR COPY OF THIS FILE)

Source: validated JSON object
Sink: Polars DataFrame

Purpose

  Transform validated JSON data into a structured format.

Analytical Questions

- Which fields are needed from the JSON data?
- How can records be normalized into tabular form?
- What derived fields would support analysis?

Notes

Following our process, do NOT edit this _case file directly,
keep it as a working example.

In your custom project, copy this _case.py file and
append with _yourname.py instead.

Then edit your copied Python file to:
- extract the fields needed for your analysis,
- normalize records into a consistent structure,
- create any derived fields required.
"""

# ============================================================
# Section 1. Setup and Imports
# ============================================================

import logging
from typing import Any

import polars as pl

# ============================================================
# Section 2. Define Run Transform Function
# ============================================================


def run_transform(
    json_data: list[dict[str, Any]],
    LOG: logging.Logger,
) -> pl.DataFrame:
    """Transform JSON into a structured DataFrame.

    Args:
        json_data (list[dict[str, Any]]): Validated JSON data.
        LOG (logging.Logger): The logger instance.

    Returns:
        pl.DataFrame: The transformed dataset.
    """
    LOG.info("========================")
    LOG.info("STAGE 03: TRANSFORM starting...")
    LOG.info("========================")
    LOG.info(
        "Selecting fields: user_id, name, username, email, city, company_name, website, full_address"
    )
    LOG.info(f"Transforming {len(json_data)} records into a structured DataFrame")

    records: list[dict[str, Any]] = []

    for record in json_data:
        address = record["address"]
        company = record["company"]
        full_address = ", ".join(
            [
                address["suite"],
                address["street"],
                address["city"],
                address["zipcode"],
            ]
        )

        records.append(
            {
                "user_id": record["id"],
                "name": record["name"],
                "username": record["username"],
                "email": record["email"],
                "city": address["city"],
                "company_name": company["name"],
                "website": record["website"],
                "full_address": full_address,
                "name_length": len(record["name"]),
            }
        )

    df: pl.DataFrame = pl.DataFrame(records)

    LOG.info(
        f"Transformation complete: {df.height} rows and {df.width} columns created"
    )
    LOG.info(f"DataFrame preview:\n{df.head()}")
    LOG.info("Sink: Polars DataFrame created")

    # Return the transformed DataFrame for use in the next stage.
    return df
