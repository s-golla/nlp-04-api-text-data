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
        "Selecting fields: post_id, comment_id, commenter_name, commenter_email, comment_text"
    )
    LOG.info(f"Transforming {len(json_data)} records into a structured DataFrame")

    records: list[dict[str, Any]] = []

    for record in json_data:
        comment_text = record["body"]
        records.append(
            {
                "post_id": record["postId"],
                "comment_id": record["id"],
                "commenter_name": record["name"],
                "commenter_email": record["email"],
                "comment_text": comment_text,
                "comment_text_length": len(comment_text),
                "comment_text_word_count": len(comment_text.split()),
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
