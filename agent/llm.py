"""Client of the reasoning model: Qwen served by Ollama (`/api/chat`).

Python standard library only, like qwen_classifier/classifier.py. The reply is
constrained to a JSON schema (Ollama structured output), and anything that is not a
complete JSON object raises LLMError: the caller never sees half an answer.
"""
import json
import re
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from agent import config

UNAVAILABLE = "UNAVAILABLE"        # model not reachable, not pulled, or too slow
INVALID_OUTPUT = "INVALID_OUTPUT"  # it answered, but not with the JSON asked for

_THINKING = re.compile(r"<think>.*?</think>", re.DOTALL)


class LLMError(Exception):
    def __init__(self, status: str, detail: str):
        super().__init__(f"{status}: {detail}")
        self.status, self.detail = status, detail


def _post(payload: dict) -> bytes:
    request = Request(config.DIAGNOSE_BASE_URL.rstrip("/") + "/api/chat",
                      data=json.dumps(payload).encode(),
                      headers={"Content-Type": "application/json"}, method="POST")
    with urlopen(request, timeout=config.DIAGNOSE_TIMEOUT_SECONDS) as response:
        return response.read(1024 * 1024)


def _error_text(exc: HTTPError) -> str:
    """Ollama's own error message; read once, because the body is a stream."""
    if not hasattr(exc, "ollama_error"):
        try:
            exc.ollama_error = json.loads(exc.read(4096)).get("error") or ""
        except (ValueError, AttributeError, OSError):
            exc.ollama_error = ""
    return exc.ollama_error


def chat_json(system: str, user: str, schema: dict) -> dict:
    """One chat turn; returns the model's reply parsed as a JSON object."""
    payload = {
        "model": config.DIAGNOSE_MODEL, "stream": False, "format": schema,
        "options": {"temperature": 0, "num_ctx": config.DIAGNOSE_NUM_CTX,
                    "num_predict": config.DIAGNOSE_NUM_PREDICT},
        "messages": [{"role": "system", "content": system}, {"role": "user", "content": user}],
    }
    if config.DIAGNOSE_THINK is not None:
        payload["think"] = config.DIAGNOSE_THINK
    try:
        try:
            raw = _post(payload)
        except HTTPError as exc:
            # A model without a thinking mode can refuse the `think` field itself.
            if exc.code != 400 or "think" not in payload or "thinking" not in _error_text(exc):
                raise
            del payload["think"]
            raw = _post(payload)
    except HTTPError as exc:  # e.g. 404: the model was never pulled
        detail = _error_text(exc)
        raise LLMError(UNAVAILABLE, f"HTTP {exc.code} from {config.DIAGNOSE_BASE_URL} for model "
                                    f"{config.DIAGNOSE_MODEL}" + (f": {detail}" if detail else "")) from exc
    except OSError as exc:    # refused, DNS, timeout
        raise LLMError(UNAVAILABLE, f"{config.DIAGNOSE_BASE_URL} not reachable ({exc})") from exc

    try:
        body = json.loads(raw)
        if not isinstance(body, dict) or body.get("done") is not True:
            raise ValueError("incomplete response")
        if body.get("done_reason") == "length":
            raise ValueError("reply was cut off (raise DIAGNOSE_NUM_PREDICT, or set DIAGNOSE_THINK=false)")
        reply = json.loads(_THINKING.sub("", body["message"]["content"]).strip())
        if not isinstance(reply, dict):
            raise ValueError("reply is not a JSON object")
    except (ValueError, TypeError, KeyError) as exc:
        raise LLMError(INVALID_OUTPUT, str(exc)) from exc
    return reply
