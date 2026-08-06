"""Job-related API endpoints.

Handles extraction (sync) and job CRUD.

Exposes one router:
- ``router`` → mounted at ``/v1/extract``
"""

import asyncio
import json
import re
import time
from typing import Any, Dict, List, Optional

import requests
from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from jsonschema import Draft202012Validator

from common.error_utils import http_error_responses
from common.llm_utils import get_vllm_headers
from common.misc_utils import get_llm_endpoint, get_logger
import common.misc_utils as misc_utils

from extract.db.manager import db_repo
from extract.models import ExtractionRequest, ExtractionResponse
from extract.settings import settings
from extract.utils.schema import (
    ExtractException,
    _tokenize,
    check_extraction_budget,
)

router = APIRouter()
logger = get_logger("jobs_router")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _strip_markdown_fences(text: str) -> str:
    """Remove accidental ```json … ``` or ``` … ``` fences from model output."""
    text = text.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text, flags=re.IGNORECASE)
    text = re.sub(r"\s*```$", "", text)
    return text.strip()


def _render_few_shot_block(examples: Optional[List[Dict[str, Any]]]) -> str:
    """Render the few-shot block from stored example rows."""
    if not examples:
        return ""
    parts: List[str] = []
    for ex in examples:
        parts.append(
            f"Example text:\n{ex['text']}\n"
            f"Example JSON:\n{json.dumps(ex['output'], ensure_ascii=False)}"
        )
    return "\n\n".join(parts)


def _build_messages(
    normalized_schema: Dict[str, Any],
    few_shot_block: str,
    input_text: str,
    custom_prompt: Optional[str],
) -> List[Dict[str, str]]:
    """Assemble the chat messages list for the extraction prompt."""
    system_content = settings.extract.extraction_system_prompt.format(
        custom_prompt=custom_prompt if custom_prompt else ""
    )
    user_content = settings.extract.extraction_user_prompt.format(
        normalized_json_schema=json.dumps(normalized_schema, ensure_ascii=False),
        few_shot_block=few_shot_block,
        input_text=input_text,
    )
    return [
        {"role": "system", "content": system_content},
        {"role": "user", "content": user_content},
    ]


def _call_vllm(
    messages: List[Dict[str, str]],
    max_tokens: int,
    normalized_schema: Dict[str, Any],
    llm_endpoint: str,
    llm_model: str,
) -> Dict[str, Any]:
    """
    Execute a single blocking /v1/chat/completions call.

    Adds ``guided_json`` when ``GUIDED_DECODING_ENABLED`` is true.
    Returns the raw response JSON dict.
    Raises ``requests.exceptions.ConnectionError`` / ``HTTPError`` on failure.
    """
    if misc_utils.SESSION is None:
        raise RuntimeError("LLM session not initialized.")

    payload: Dict[str, Any] = {
        "model": llm_model,
        "messages": messages,
        "max_tokens": max_tokens,
        "temperature": settings.extract.extraction_temperature,
        "stream": False,
    }

    if settings.extract.guided_decoding_enabled:
        payload["extra_body"] = {"guided_json": normalized_schema}

    headers = get_vllm_headers(settings.common.llm.api_key)
    response = misc_utils.SESSION.post(
        f"{llm_endpoint}/v1/chat/completions",
        json=payload,
        headers=headers,
        stream=False,
    )
    response.raise_for_status()
    return response.json()


def _validate_output(raw_text: str, normalized_schema: Dict[str, Any]) -> Dict[str, Any]:
    """
    Parse and validate the model output against *normalized_schema*.

    Returns the parsed dict on success.
    Raises ``ValueError`` whose message lists all schema errors (used as
    the retry-append payload).
    """
    cleaned = _strip_markdown_fences(raw_text)
    try:
        parsed = json.loads(cleaned)
    except json.JSONDecodeError as exc:
        raise ValueError(f"Output is not valid JSON: {exc}") from exc

    validator = Draft202012Validator(normalized_schema)
    errors = list(validator.iter_errors(parsed))
    if errors:
        raise ValueError("; ".join(e.message for e in errors))

    return parsed


# ---------------------------------------------------------------------------
# POST /v1/extract — Synchronous extraction
# ---------------------------------------------------------------------------

@router.post(
    "",
    response_model=ExtractionResponse,
    status_code=200,
    tags=["extraction"],
    summary="Synchronous extraction",
    description=(
        "Extract structured data from plain text against a registered schema in a "
        "single blocking call.  Returns validated, schema-conformant JSON.\n\n"
        "**Processing steps:**\n"
        "1. Enforce request-body size limit (413 `REQUEST_TOO_LARGE`) before tokenisation.\n"
        "2. Validate `schema_id` (404) and `text` non-empty (400).\n"
        "3. Acquire the global vLLM concurrency slot (429 if saturated).\n"
        "4. Tokenise `text` via vLLM `/tokenize` (exact count).\n"
        "5. Hard context-window guard — 413 `CONTEXT_LIMIT_EXCEEDED` with full token "
        "diagnostics (`input_tokens`, `schema_tokens`, `examples_tokens`, "
        "`custom_prompt_tokens`, `prompt_overhead_tokens`, `reserved_output_tokens`) "
        "on breach.\n"
        "6. Build prompt: system prompt + `{custom_prompt}` + schema + few-shot block + text.\n"
        "7. Call vLLM with `guided_json` (when `GUIDED_DECODING_ENABLED=true`) and "
        "`temperature=0.0`. `finish_reason=length` → 413 `OUTPUT_BUDGET_EXCEEDED` "
        "(no validation retry burned).\n"
        "8. Server-side `jsonschema` validation; one bounded retry on failure (validation "
        "errors appended to prompt). 422 `EXTRACTION_VALIDATION_FAILED` if retry also fails.\n"
        "9. Return extraction + token usage + timing.\n\n"
        "The semaphore slot is held across both the initial call and the retry."
    ),
    responses={
        400: http_error_responses[400],
        404: http_error_responses[404],
        413: http_error_responses[413],
        422: {"description": "Extraction output failed schema validation after retry"},
        429: http_error_responses[429],
        500: http_error_responses[500],
        503: http_error_responses[503],
    },
    include_in_schema=True,
)
async def extract_sync(request: Request) -> JSONResponse:
    """Synchronous entity extraction — blocking call with schema-validated JSON output."""
    t_start = time.monotonic()

    # ------------------------------------------------------------------
    # 0. Request-body size guard — before any parsing or tokenisation
    # ------------------------------------------------------------------
    content_length = request.headers.get("content-length")
    if content_length is not None and int(content_length) > settings.extract.max_request_body_bytes:
        raise ExtractException(
            "REQUEST_TOO_LARGE",
            f"Request body exceeds the maximum allowed size of "
            f"{settings.extract.max_request_body_bytes} bytes.",
            status=413,
            details={"max_request_body_bytes": settings.extract.max_request_body_bytes},
        )

    try:
        raw_body = await request.body()
    except Exception:
        raise ExtractException(
            "INVALID_REQUEST", "Failed to read request body.", status=400
        )

    if len(raw_body) > settings.extract.max_request_body_bytes:
        raise ExtractException(
            "REQUEST_TOO_LARGE",
            f"Request body exceeds the maximum allowed size of "
            f"{settings.extract.max_request_body_bytes} bytes.",
            status=413,
            details={"max_request_body_bytes": settings.extract.max_request_body_bytes},
        )

    # ------------------------------------------------------------------
    # 1. Parse & basic field validation
    # ------------------------------------------------------------------
    try:
        body_dict = json.loads(raw_body)
    except Exception:
        raise ExtractException(
            "INVALID_JSON", "Request body is not valid JSON.", status=400
        )

    try:
        body = ExtractionRequest(**body_dict)
    except Exception as exc:
        raise ExtractException(
            "INVALID_REQUEST", f"Invalid request body: {exc}", status=400
        )

    # Validate schema exists
    schema_row = db_repo.get_schema_by_id(body.schema_id)
    if schema_row is None:
        raise ExtractException(
            "SCHEMA_NOT_FOUND",
            f"No schema with id {body.schema_id!r}.",
            status=404,
        )

    # ------------------------------------------------------------------
    # 2. Semaphore check (non-blocking — reject immediately if saturated)
    # ------------------------------------------------------------------
    from extract.app import concurrency_limiter  # deferred to avoid circular import

    if concurrency_limiter.locked():
        raise ExtractException(
            "RATE_LIMIT_EXCEEDED",
            "Server is at maximum vLLM concurrency. Please retry later.",
            status=429,
        )

    # ------------------------------------------------------------------
    # 3–8. Core extraction
    #       One semaphore slot held across BOTH the initial call and the
    #       validation retry so a second attempt cannot be starved.
    # ------------------------------------------------------------------
    llm_model_dict = get_llm_endpoint()
    llm_endpoint: str = llm_model_dict.get("llm_endpoint", "")
    llm_model: str = llm_model_dict.get("llm_model", "")
    max_model_len: int = settings.common.llm.max_model_len

    async with concurrency_limiter:

        # ── 3. Exact input token count via /tokenize ─────────────────────
        try:
            input_tokens: int = await asyncio.to_thread(
                _tokenize, body.text, llm_endpoint
            )
        except Exception as exc:
            logger.error(f"Tokenisation failed: {exc}", exc_info=True)
            raise ExtractException(
                "TOKENIZATION_ERROR",
                "Failed to tokenise the input text. "
                "Ensure the vLLM /tokenize endpoint is reachable.",
                status=503,
            )

        # ── 4. Hard context-window guard (raises ExtractException 413) ───
        #       check_extraction_budget raises SchemaValidationError(413);
        #       re-raise as ExtractException to keep the error surface uniform.
        try:
            reserved_output = check_extraction_budget(
                input_tokens=input_tokens,
                schema_tokens=schema_row.schema_tokens,
                examples_tokens=schema_row.examples_tokens,
                custom_prompt_tokens=schema_row.custom_prompt_tokens,
                max_model_len=max_model_len,
            )
        except Exception as budget_exc:
            # Re-raise preserving code / message / status / details.
            raise ExtractException(
                budget_exc.code,
                budget_exc.message,
                status=budget_exc.status,
                details=budget_exc.details,
            ) from budget_exc

        # ── 5. Prompt assembly ────────────────────────────────────────────
        few_shot_block = _render_few_shot_block(schema_row.examples)
        messages = _build_messages(
            normalized_schema=schema_row.json_schema,
            few_shot_block=few_shot_block,
            input_text=body.text,
            custom_prompt=schema_row.custom_prompt,
        )

        # ── 6. First vLLM call ────────────────────────────────────────────
        try:
            vllm_resp = await asyncio.to_thread(
                _call_vllm, messages, reserved_output,
                schema_row.json_schema, llm_endpoint, llm_model,
            )
        except requests.exceptions.ConnectionError as exc:
            logger.error(f"vLLM unreachable: {exc}")
            raise ExtractException(
                "LLM_UNAVAILABLE", "The AI service is unreachable.", status=503
            )
        except requests.exceptions.HTTPError as exc:
            logger.error(f"vLLM HTTP error: {exc}")
            raise ExtractException(
                "LLM_ERROR",
                f"The AI service returned an error: {exc.response.status_code}",
                status=500,
            )
        except Exception as exc:
            logger.error(f"Unexpected error calling vLLM: {exc}", exc_info=True)
            raise ExtractException(
                "LLM_ERROR", "Unexpected error during LLM call.", status=500
            )

        choices = vllm_resp.get("choices", [])
        if not choices:
            raise ExtractException(
                "LLM_ERROR", "vLLM returned an empty choices list.", status=500
            )

        choice = choices[0]
        finish_reason: str = choice.get("finish_reason", "")

        # ── 7. Output-budget exceeded — fail fast, do NOT burn the retry ─
        if finish_reason == "length":
            raise ExtractException(
                "OUTPUT_BUDGET_EXCEEDED",
                "The model output was truncated because it reached the reserved "
                "output token limit. Adjust OUTPUT_TOKEN_FACTOR, MIN_OUTPUT_TOKENS, "
                "or MAX_OUTPUT_TOKENS via environment configuration.",
                status=413,
                details={
                    "reserved_output_tokens": reserved_output,
                    "finish_reason": "length",
                },
            )

        raw_output: str = choice.get("message", {}).get("content", "") or ""
        usage = vllm_resp.get("usage", {})
        total_prompt_tokens: int = usage.get("prompt_tokens", 0)
        total_completion_tokens: int = usage.get("completion_tokens", 0)

        # ── 8. Server-side validation + one bounded retry ─────────────────
        validation_attempts = 1
        try:
            parsed_output = _validate_output(raw_output, schema_row.json_schema)
        except ValueError as val_err:
            # Build the retry prompt with validation errors appended.
            validation_attempts = 2
            retry_messages = messages + [
                {"role": "assistant", "content": raw_output},
                {
                    "role": "user",
                    "content": (
                        "Your previous output failed validation with these errors:\n"
                        f"{val_err}\n\n"
                        f"Previous output:\n{raw_output}\n\n"
                        "Return a corrected JSON object that fixes ALL listed errors. "
                        "Output ONLY the JSON."
                    ),
                },
            ]

            try:
                retry_resp = await asyncio.to_thread(
                    _call_vllm, retry_messages, reserved_output,
                    schema_row.json_schema, llm_endpoint, llm_model,
                )
            except requests.exceptions.ConnectionError as exc:
                logger.error(f"vLLM unreachable on retry: {exc}")
                raise ExtractException(
                    "LLM_UNAVAILABLE", "The AI service is unreachable.", status=503
                )
            except requests.exceptions.HTTPError as exc:
                logger.error(f"vLLM HTTP error on retry: {exc}")
                raise ExtractException(
                    "LLM_ERROR",
                    f"The AI service returned an error on retry: {exc.response.status_code}",
                    status=500,
                )
            except Exception as exc:
                logger.error(f"Unexpected error on retry vLLM call: {exc}", exc_info=True)
                raise ExtractException(
                    "LLM_ERROR", "Unexpected error during retry LLM call.", status=500
                )

            retry_choices = retry_resp.get("choices", [])
            if not retry_choices:
                raise ExtractException(
                    "LLM_ERROR",
                    "vLLM returned an empty choices list on retry.",
                    status=500,
                )

            retry_choice = retry_choices[0]
            if retry_choice.get("finish_reason") == "length":
                raise ExtractException(
                    "OUTPUT_BUDGET_EXCEEDED",
                    "The model output was truncated on the validation retry.",
                    status=413,
                    details={
                        "reserved_output_tokens": reserved_output,
                        "finish_reason": "length",
                    },
                )

            raw_retry_output: str = retry_choice.get("message", {}).get("content", "") or ""
            retry_usage = retry_resp.get("usage", {})
            total_prompt_tokens += retry_usage.get("prompt_tokens", 0)
            total_completion_tokens += retry_usage.get("completion_tokens", 0)

            try:
                parsed_output = _validate_output(raw_retry_output, schema_row.json_schema)
            except ValueError as retry_err:
                raise ExtractException(
                    "EXTRACTION_VALIDATION_FAILED",
                    "Model output failed schema validation after one retry.",
                    status=422,
                    details={
                        "validation_errors": str(retry_err),
                        "raw_output": raw_retry_output,
                    },
                )

    # ------------------------------------------------------------------
    # 9. Return response
    # ------------------------------------------------------------------
    processing_time_ms = int((time.monotonic() - t_start) * 1000)

    return JSONResponse(
        status_code=200,
        content={
            "data": {
                "extraction": parsed_output,
                "schema_id": body.schema_id,
                "source": {
                    "input_type": "text",
                    "input_tokens": input_tokens,
                },
            },
            "meta": {
                "model": llm_model,
                "processing_time_ms": processing_time_ms,
                "validation_attempts": validation_attempts,
            },
            "usage": {
                "input_tokens": total_prompt_tokens,
                "output_tokens": total_completion_tokens,
                "total_tokens": total_prompt_tokens + total_completion_tokens,
            },
        },
    )


# ---------------------------------------------------------------------------
# Extraction jobs CRUD stubs — implemented separately
# ---------------------------------------------------------------------------

@router.post("/jobs", status_code=202, tags=["jobs"], include_in_schema=True)
async def create_extract_job():
    """Async extraction job — implementation in follow-up iteration."""
    raise ExtractException("NOT_IMPLEMENTED", "POST /v1/extract/jobs not yet implemented.", status=501)


@router.get("/jobs", tags=["jobs"], include_in_schema=True)
async def list_extract_jobs():
    raise ExtractException("NOT_IMPLEMENTED", "GET /v1/extract/jobs not yet implemented.", status=501)


@router.get("/jobs/{job_id}", tags=["jobs"], include_in_schema=True)
async def get_extract_job(job_id: str):
    raise ExtractException("NOT_IMPLEMENTED", "GET /v1/extract/jobs/{job_id} not yet implemented.", status=501)


@router.get("/jobs/{job_id}/result", tags=["jobs"], include_in_schema=True)
async def get_extract_job_result(job_id: str):
    raise ExtractException("NOT_IMPLEMENTED", "GET /v1/extract/jobs/{job_id}/result not yet implemented.", status=501)


@router.delete("/jobs/{job_id}", status_code=204, tags=["jobs"], include_in_schema=True)
async def delete_extract_job(job_id: str):
    raise ExtractException("NOT_IMPLEMENTED", "DELETE /v1/extract/jobs/{job_id} not yet implemented.", status=501)


@router.delete("/jobs", status_code=204, tags=["jobs"], include_in_schema=True)
async def bulk_delete_extract_jobs():
    raise ExtractException("NOT_IMPLEMENTED", "DELETE /v1/extract/jobs not yet implemented.", status=501)
