"""
Recording pipeline — fetches the call recording from Exotel and uploads to S3.

How Exotel works:
  After a call ends, Exotel processes the audio and makes a recording URL
  available via their REST API. The time between call-end and URL availability
  varies: typically 10–30 seconds, but can be 60–90s under load on their end.

  The URL is fetched via:
      GET /v1/Accounts/{account_sid}/Calls/{call_sid}/Recording
  Returns 200 + recording_url if ready, 404 if not yet available.

Current approach:
  Wait 45 seconds. Try once. If it's not there, give up silently.

This means:
  - Recordings ready in 10s: we waste 35 seconds of wall time
  - Recordings ready in 60s: we miss them entirely, no retry, no alert
  - We have no idea how many recordings we're silently missing

The Exotel API is poll-friendly — they don't rate-limit the status endpoint.
The information needed to fix this is already available: try, check, sleep
a bit, try again. How many times and with what interval is worth thinking about.

Note: recording upload and LLM analysis are completely independent. The LLM
reads the transcript text, not the audio. There's no reason they have to run
sequentially. What would need to change for them to run in parallel?
"""

import asyncio
import logging
from typing import Optional

import httpx

from src.config import settings

logger = logging.getLogger(__name__)


async def fetch_and_upload_recording(
    interaction_id: str,
    call_sid: str,
    exotel_account_id: str,
) -> Optional[str]:
    """
    Attempt to fetch the Exotel recording and upload it to S3.

    Uses exponential backoff retry loop to handle delayed recording availability.
    Returns the S3 key on success, None on failure or timeout.
    """
    backoff_schedule = [5, 10, 20, 40, 60, 60]
    max_attempts = len(backoff_schedule) + 1

    for attempt in range(1, max_attempts + 1):
        try:
            recording_url = await _fetch_exotel_recording_url(call_sid, exotel_account_id)

            if recording_url:
                s3_key = await _upload_to_s3(recording_url, interaction_id)
                logger.info(
                    "recording_success",
                    extra={
                        "interaction_id": interaction_id,
                        "call_sid": call_sid,
                        "attempt": attempt,
                        "s3_key": s3_key,
                    }
                )
                return s3_key

            # None returned means 404 (not ready yet)
            logger.info(
                "recording_attempt_failed",
                extra={
                    "interaction_id": interaction_id,
                    "call_sid": call_sid,
                    "attempt": attempt,
                    "status": "not_ready",
                }
            )

        except Exception as e:
            # Connection error, HTTP status error, S3 upload error, etc.
            logger.warning(
                "recording_attempt_error",
                extra={
                    "interaction_id": interaction_id,
                    "call_sid": call_sid,
                    "attempt": attempt,
                    "error": str(e),
                }
            )

        if attempt < max_attempts:
            wait_seconds = backoff_schedule[attempt - 1]
            logger.info(
                "recording_retry_waiting",
                extra={
                    "interaction_id": interaction_id,
                    "attempt": attempt,
                    "wait_seconds": wait_seconds,
                }
            )
            await asyncio.sleep(wait_seconds)

    logger.error(
        "recording_failed_permanently",
        extra={
            "interaction_id": interaction_id,
            "call_sid": call_sid,
            "total_attempts": max_attempts,
        }
    )
    return None


async def _fetch_exotel_recording_url(
    call_sid: str, account_id: str
) -> Optional[str]:
    """
    Hit the Exotel API to get the recording URL for a completed call.

    Returns the recording URL if available, None if not yet ready (404 status).
    Raises httpx.HTTPError for general errors or raise_for_status for other statuses.
    """
    url = f"https://api.exotel.com/v1/Accounts/{account_id}/Calls/{call_sid}/Recording"

    async with httpx.AsyncClient(timeout=10) as client:
        resp = await client.get(url)
        if resp.status_code == 200:
            data = resp.json()
            return data.get("recording_url")
        elif resp.status_code == 404:
            return None
        else:
            resp.raise_for_status()


async def _upload_to_s3(recording_url: str, interaction_id: str) -> str:
    """
    Download the recording from Exotel's URL and upload to S3.

    In production: stream from recording_url → boto3 upload to S3_BUCKET.
    S3 key format: recordings/{interaction_id}.mp3

    The interaction's recording_s3_key column gets updated after this succeeds.
    If this crashes after the upload but before the DB write, the file is in S3
    but the interaction row doesn't know about it. Currently no reconciliation job.
    """
    s3_key = f"recordings/{interaction_id}.mp3"

    logger.info(
        "recording_uploaded",
        extra={"interaction_id": interaction_id, "s3_key": s3_key},
    )
    return s3_key
