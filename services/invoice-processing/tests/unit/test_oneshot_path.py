"""
Unit tests for pipeline/oneshot_path.py — run_oneshot_path() full orchestration (PR-6).
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from pipeline.oneshot_path import run_oneshot_path

EXTRACT_JOB_ID = "ex-abc-1"
RAW_RESULT = {
    "AP_INVOICES_ALL": {"invoice_num": "INV-001"},
    "AP_INVOICE_LINES_ALL": [],
}


def _make_client(extract_job_id: str = EXTRACT_JOB_ID, poll_result: dict = RAW_RESULT):
    mock = MagicMock()
    mock.submit_extraction = AsyncMock(return_value=extract_job_id)
    mock.poll_job = AsyncMock(return_value=poll_result)
    return mock


@patch("pipeline.oneshot_path.db_manager")
def test_happy_path_returns_poll_result(mock_db, tmp_path):
    """run_oneshot_path must return the result from poll_job."""
    staged = tmp_path / "scan.png"
    staged.write_bytes(b"\x89PNG")

    client = _make_client()
    result = asyncio.run(
        run_oneshot_path(job_id="job-os-1", staged_path=staged, extract_client=client)
    )

    assert result == RAW_RESULT


@patch("pipeline.oneshot_path.db_manager")
def test_submit_called_with_job_id_and_file_path(mock_db, tmp_path):
    """submit_extraction must be called with job_id= and file_path=."""
    staged = tmp_path / "scan.png"
    staged.write_bytes(b"\x89PNG")

    client = _make_client()
    asyncio.run(
        run_oneshot_path(job_id="job-os-2", staged_path=staged, extract_client=client)
    )

    client.submit_extraction.assert_called_once_with(
        job_id="job-os-2",
        file_path=staged,
    )


@patch("pipeline.oneshot_path.db_manager")
def test_extract_job_id_stored_in_db(mock_db, tmp_path):
    """db_manager.update_job must be called with extract_job_id after submit."""
    staged = tmp_path / "scan.png"
    staged.write_bytes(b"\x89PNG")

    client = _make_client(extract_job_id="ex-store-1")
    asyncio.run(
        run_oneshot_path(job_id="job-os-3", staged_path=staged, extract_client=client)
    )

    first_call = mock_db.update_job.call_args_list[0]
    assert first_call == call(job_id="job-os-3", extract_job_id="ex-store-1")


@patch("pipeline.oneshot_path.db_manager")
def test_poll_called_with_extract_job_id(mock_db, tmp_path):
    """poll_job must be called with the job_id returned by submit_extraction."""
    staged = tmp_path / "receipt.jpg"
    staged.write_bytes(b"\xFF\xD8\xFF")

    client = _make_client(extract_job_id="ex-poll-2")
    asyncio.run(
        run_oneshot_path(job_id="job-os-4", staged_path=staged, extract_client=client)
    )

    client.poll_job.assert_called_once_with("ex-poll-2")


@patch("pipeline.oneshot_path.db_manager")
def test_latency_stored_in_job_metadata(mock_db, tmp_path):
    """db_manager.update_job must be called a second time with job_metadata containing oneshot_latency_s."""
    staged = tmp_path / "scan.png"
    staged.write_bytes(b"\x89PNG")

    client = _make_client()
    asyncio.run(
        run_oneshot_path(job_id="job-os-5", staged_path=staged, extract_client=client)
    )

    assert mock_db.update_job.call_count == 2
    second_call = mock_db.update_job.call_args_list[1]
    kwargs = second_call.kwargs
    assert kwargs["job_id"] == "job-os-5"
    assert "job_metadata" in kwargs
    assert "oneshot_latency_s" in kwargs["job_metadata"]
    assert isinstance(kwargs["job_metadata"]["oneshot_latency_s"], float)


@patch("pipeline.oneshot_path.db_manager")
def test_no_digitize_call_made(mock_db, tmp_path):
    """One-shot path must not touch a digitize client (no PDF conversion)."""
    staged = tmp_path / "scan.png"
    staged.write_bytes(b"\x89PNG")

    # Provide only an extract client — if digitize were called it would AttributeError
    client = _make_client()
    # Should complete without error
    asyncio.run(
        run_oneshot_path(job_id="job-os-6", staged_path=staged, extract_client=client)
    )
    # Verify only two DB calls were made (extract_job_id + job_metadata), no digitize_job_id
    for c in mock_db.update_job.call_args_list:
        assert "digitize_job_id" not in c.kwargs


@patch("pipeline.oneshot_path.db_manager")
def test_logs_at_info(mock_db, tmp_path, caplog):
    """run_oneshot_path must log the forwarding decision and completion at INFO."""
    import logging

    staged = tmp_path / "receipt.jpg"
    staged.write_bytes(b"\xFF\xD8\xFF")

    client = _make_client()
    with caplog.at_level(logging.INFO, logger="pipeline.oneshot_path"):
        asyncio.run(
            run_oneshot_path(job_id="job-log-1", staged_path=staged, extract_client=client)
        )

    assert "One-Shot path" in caplog.text
    assert "extract VLM" in caplog.text
    assert "completed" in caplog.text


@pytest.mark.parametrize("image_name", ["scan.png", "photo.jpg", "doc.tiff"])
@patch("pipeline.oneshot_path.db_manager")
def test_various_image_formats(mock_db, tmp_path, image_name):
    """run_oneshot_path must handle any image extension without special-casing."""
    staged = tmp_path / image_name
    staged.write_bytes(b"data")

    expected = {"AP_INVOICES_ALL": {"invoice_num": "X"}, "AP_INVOICE_LINES_ALL": []}
    client = _make_client(poll_result=expected)

    result = asyncio.run(
        run_oneshot_path(job_id="job-fmt-1", staged_path=staged, extract_client=client)
    )
    assert result == expected
