import os
import logging
from dataclasses import dataclass

from common.misc_utils import set_log_level

log_level = logging.INFO
level = os.getenv("LOG_LEVEL", "").removeprefix("--").lower()
if level != "":
    if "debug" in level:
        log_level = logging.DEBUG
    elif not "info" in level:
        raise Exception(f"Unknown LOG_LEVEL passed: '{level}'")
set_log_level(log_level)


from flask import Flask, request, jsonify, Response, stream_with_context
import json
from threading import BoundedSemaphore
from functools import wraps

import common.db_utils as db
from common.llm_utils import create_llm_session, query_vllm_stream, query_vllm_non_stream, query_vllm_models, lang_de
from common.misc_utils import get_model_endpoints, set_log_level
from common.settings import get_settings
from retrieve.backend_utils import search_only
from lingua import Language, LanguageDetectorBuilder


vectorstore = None
_detector = None
# Globals to be set dynamically
emb_model_dict = {}
llm_model_dict = {}
reranker_model_dict = {}

settings = get_settings()
concurrency_limiter = BoundedSemaphore(settings.max_concurrent_requests)

def initialize_models():
    global emb_model_dict, llm_model_dict, reranker_model_dict
    emb_model_dict, llm_model_dict, reranker_model_dict = get_model_endpoints()

def initialize_vectorstore():
    global vectorstore
    vectorstore = db.get_vector_store()

def init_detector():
    """Call once at app startup, before serving requests."""
    global _detector
    _detector = (
        LanguageDetectorBuilder
        .from_languages(*SUPPORTED_LANGUAGES)
        .with_preloaded_language_models()
        .build()
    )

app = Flask(__name__)

# Setting 32 to fully utilse the vLLM's Max Batch Size
POOL_SIZE = 32

create_llm_session(pool_maxsize=POOL_SIZE)

@dataclass
class DetectionResult:
    language: str       # e.g. "FRENCH"
    iso_code: str       # e.g. "FR"
    confidence: float   # e.g. 0.9834



def detect_language(text: str, min_confidence: float = 0.6) -> DetectionResult | None:
    """
    Detect the language of a text string.

    Returns a DetectionResult if confidence >= min_confidence, else None.
    Thread-safe — can be called from any endpoint or background task.
    """
    if not _detector:
        raise RuntimeError("Lingua detector not initialized. Call init_detector() at startup.")

    confidences = _detector.compute_language_confidence_values(text)
    if confidences and confidences[0].value >= min_confidence:
        top = confidences[0]
        return DetectionResult(
            language=top.language.name,
            iso_code=top.language.iso_code_639_1.name,
            confidence=round(top.value, 4),
        )
    return None


def limit_concurrency(f):
    @wraps(f)
    def wrapper(*args, **kwargs):
        if not concurrency_limiter.acquire(blocking=False):
            return jsonify({"error": "Server busy. Try again shortly."}), 429
        try:
            return f(*args, **kwargs)
        finally:
            concurrency_limiter.release()
    return wrapper


@app.post("/reference")
def get_reference_docs():
    data = request.get_json()
    query = data.get("prompt", "")
    try:
        emb_model = emb_model_dict['emb_model']
        emb_endpoint = emb_model_dict['emb_endpoint']
        emb_max_tokens = emb_model_dict['max_tokens']
        reranker_model = reranker_model_dict['reranker_model']
        reranker_endpoint = reranker_model_dict['reranker_endpoint']

        docs = search_only(
            query,
            emb_model, emb_endpoint, emb_max_tokens,
            reranker_model,
            reranker_endpoint,
            settings.num_chunks_post_search,
            settings.num_chunks_post_reranker,
            vectorstore=vectorstore
        )
    except db.get_vector_store_not_ready() as e:
        return jsonify({"error": str(e)}), 503   # Service unavailable
    except Exception as e:
        return jsonify({"error": repr(e)})
    return Response(
        json.dumps({"documents": docs}, default=str),
        mimetype="application/json"
    )


@app.get("/v1/models")
def list_models():
    logging.debug("List models..")
    try:
        llm_endpoint = llm_model_dict['llm_endpoint']
        return query_vllm_models(llm_endpoint)
    except Exception as e:
        return jsonify({"error": repr(e)})


def locked_stream(stream_g):
    try:
        for chunk in stream_g:
            yield chunk
    finally:
        concurrency_limiter.release()


@app.post("/v1/chat/completions")
def chat_completion():
    data = request.get_json()
    if data and len(data.get("messages", [])) == 0:
        return jsonify({"error": "messages can't be empty"})
    msgs = data.get("messages")[0]
    query = msgs.get("content")
    max_tokens = data.get("max_tokens", settings.llm_max_tokens)
    max_tokens_de = data.get("max_tokens", settings.llm_max_tokens_de)
    temperature = data.get("temperature", settings.temperature)
    stop_words = data.get("stop")
    stream = data.get("stream", False)
    lang = detect_language(query).iso_code
    if lang == "DE":
        max_tokens = max_tokens_de

    try:
        emb_model = emb_model_dict['emb_model']
        emb_endpoint = emb_model_dict['emb_endpoint']
        emb_max_tokens = emb_model_dict['max_tokens']
        llm_model = llm_model_dict['llm_model']
        llm_endpoint = llm_model_dict['llm_endpoint']
        reranker_model = reranker_model_dict['reranker_model']
        reranker_endpoint = reranker_model_dict['reranker_endpoint']
        docs = search_only(
            query,
            emb_model, emb_endpoint, emb_max_tokens,
            reranker_model,
            reranker_endpoint,
            settings.num_chunks_post_search,
            settings.num_chunks_post_reranker,
            vectorstore=vectorstore
        )
    except db.get_vector_store_not_ready() as e:
        return jsonify({"error": str(e)}), 503   # Service unavailable
    except Exception as e:
        return jsonify({"error": repr(e)})

    resp_text = None

    if docs:
        if not concurrency_limiter.acquire(blocking=False):
            return jsonify({"error": "Server busy. Try again shortly."}), 429

        try:
            if stream:
                vllm_stream = query_vllm_stream(query, docs, llm_endpoint, llm_model, stop_words, max_tokens, temperature, lang)
                resp_text = stream_with_context(locked_stream(vllm_stream))           
            else:
                vllm_non_stream = query_vllm_non_stream(query, docs, llm_endpoint, llm_model, stop_words, max_tokens, temperature, lang)
                resp_text = json.dumps(vllm_non_stream, indent=None, separators=(',', ':'))
                # release semaphore lock because its non-stream request
                concurrency_limiter.release()
        except Exception as e:
            concurrency_limiter.release()
            return jsonify({"error": repr(e)}), 500

    else:
        resp_text = stream_with_context(stream_docs_not_found(lang))

    if stream:
        return Response(resp_text,
                content_type='text/event-stream',
                mimetype='text/event-stream', headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Access-Control-Allow-Headers': 'Content-Type'
            })
    return Response(resp_text,
                content_type='application/json',
                mimetype='application/json', headers={
                'Cache-Control': 'no-cache',
                'Connection': 'keep-alive',
                'Access-Control-Allow-Headers': 'Content-Type'
                })

@app.get("/db-status")
def db_status():
    try:
        emb_model = emb_model_dict['emb_model']
        emb_endpoint = emb_model_dict['emb_endpoint']
        emb_max_tokens = emb_model_dict['max_tokens']
        status = vectorstore.check_db_populated(emb_model, emb_endpoint, emb_max_tokens)
        if status==True:
            return jsonify({"ready": True}), 200
        else:
            return jsonify({"ready": False, "message": "No data ingested"}), 200
        
    except Exception as e:
        return jsonify({"ready": False, "message": str(e)}), 500


def stream_docs_not_found(lang):
    message = "No documents found in the knowledge base for this query."
    if lang == lang_de:
        message = "Für diese Anfrage wurden keine Dokumente in der Wissensdatenbank gefunden."
    yield f"data: {json.dumps({'choices': [{'delta': {'content': message}}]})}\n\n"


@app.get("/health")
def health():
    return jsonify({"status": "ok"}), 200


if __name__ == "__main__":
    initialize_models()
    initialize_vectorstore()
    port = int(os.getenv("PORT", "5000"))
    app.run(host="0.0.0.0", port=port)
