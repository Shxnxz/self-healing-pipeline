"""The Ollama client: python -m pytest agent/tests   (against a local fake of /api/chat)"""
import json
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import pytest

from agent import config, llm

pytestmark = pytest.mark.real_io

SCHEMA = {"type": "object", "properties": {"action": {"type": "string"}}, "required": ["action"]}


@pytest.fixture
def ollama(monkeypatch):
    """A local server that answers /api/chat with whatever the test puts in `server.answer`."""

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            server.requests.append((self.path, json.loads(self.rfile.read(int(self.headers["Content-Length"])))))
            answer = server.answer
            status, body = answer(server.requests[-1][1]) if callable(answer) else answer
            self.send_response(status)
            self.end_headers()
            self.wfile.write(body if isinstance(body, bytes) else json.dumps(body).encode())

        def log_message(self, *args):
            pass

    server = HTTPServer(("127.0.0.1", 0), Handler)
    server.requests, server.answer = [], (200, {})
    threading.Thread(target=server.serve_forever, daemon=True).start()
    monkeypatch.setattr(config, "DIAGNOSE_BASE_URL", f"http://127.0.0.1:{server.server_port}/")
    monkeypatch.setattr(config, "DIAGNOSE_MODEL", "qwen3:test")
    yield server
    server.shutdown()


def reply(content, **extra):
    return 200, {"done": True, "message": {"role": "assistant", "content": content}, **extra}


def test_asks_ollama_for_schema_constrained_json(ollama):
    ollama.answer = reply('{"action": "ESCALATE"}')
    assert llm.chat_json("system text", "user text", SCHEMA) == {"action": "ESCALATE"}
    (path, body), = ollama.requests
    assert path == "/api/chat" and body["model"] == "qwen3:test" and body["format"] == SCHEMA
    assert body["stream"] is False and body["options"]["temperature"] == 0
    assert body["messages"] == [{"role": "system", "content": "system text"},
                                {"role": "user", "content": "user text"}]


def test_thinking_written_into_the_content_is_ignored(ollama):
    ollama.answer = reply('<think>June or July?\nThe dates say July.</think>\n{"action": "ADD_VARIANT"}')
    assert llm.chat_json("s", "u", SCHEMA) == {"action": "ADD_VARIANT"}


@pytest.mark.parametrize("answer, problem", [
    (reply("The value is a typo of July."), ""),
    (reply('["ADD_VARIANT"]'), "not a JSON object"),
    (reply('{"action": "ADD_VAR', done_reason="length"), "cut off"),
    ((200, {"done": False, "message": {"content": "{}"}}), "incomplete"),
    ((200, {"done": True}), ""),
    ((200, b"<html>gateway</html>"), ""),
])
def test_anything_but_a_complete_json_object_is_invalid_output(ollama, answer, problem):
    ollama.answer = answer
    with pytest.raises(llm.LLMError) as error:
        llm.chat_json("s", "u", SCHEMA)
    assert error.value.status == llm.INVALID_OUTPUT and problem in error.value.detail


def test_a_model_that_was_never_pulled_is_unavailable(ollama):
    ollama.answer = (404, {"error": "model 'qwen3:test' not found"})
    with pytest.raises(llm.LLMError) as error:
        llm.chat_json("s", "u", SCHEMA)
    assert error.value.status == llm.UNAVAILABLE and "HTTP 404" in error.value.detail
    assert "model 'qwen3:test' not found" in error.value.detail and len(ollama.requests) == 1


def test_no_server_is_unavailable(monkeypatch):
    monkeypatch.setattr(config, "DIAGNOSE_BASE_URL", "http://127.0.0.1:9")   # nothing listens there
    with pytest.raises(llm.LLMError) as error:
        llm.chat_json("s", "u", SCHEMA)
    assert error.value.status == llm.UNAVAILABLE and "not reachable" in error.value.detail


# --- thinking (Qwen3 thinks by default, and the thinking counts against num_predict) ------
def test_thinking_is_switched_off_by_default(ollama):
    ollama.answer = reply('{"action": "ESCALATE"}')
    llm.chat_json("s", "u", SCHEMA)
    assert ollama.requests[0][1]["think"] is False


@pytest.mark.parametrize("setting, sent", [(True, True), (None, "absent")])
def test_thinking_follows_the_setting(ollama, monkeypatch, setting, sent):
    monkeypatch.setattr(config, "DIAGNOSE_THINK", setting)
    ollama.answer = reply('{"action": "ESCALATE"}', )
    llm.chat_json("s", "u", SCHEMA)
    assert ollama.requests[0][1].get("think", "absent") == sent


def test_thinking_returned_in_its_own_field_is_not_the_answer(ollama, monkeypatch):
    monkeypatch.setattr(config, "DIAGNOSE_THINK", True)
    ollama.answer = (200, {"done": True, "message": {"role": "assistant", "thinking": "June or {July}?",
                                                     "content": '{"action": "ADD_VARIANT"}'}})
    assert llm.chat_json("s", "u", SCHEMA) == {"action": "ADD_VARIANT"}


def test_a_model_without_a_thinking_mode_is_asked_again_without_the_field(ollama):
    def answer(body):
        if "think" in body:
            return 400, {"error": '"qwen3:test" does not support thinking'}
        return reply('{"action": "ESCALATE"}')

    ollama.answer = answer
    assert llm.chat_json("s", "u", SCHEMA) == {"action": "ESCALATE"}
    assert ["think" in body for _, body in ollama.requests] == [True, False]


def test_any_other_bad_request_is_reported_once_with_what_ollama_said(ollama):
    ollama.answer = (400, {"error": "invalid format: schema too deep"})
    with pytest.raises(llm.LLMError) as error:
        llm.chat_json("s", "u", SCHEMA)
    assert error.value.status == llm.UNAVAILABLE and len(ollama.requests) == 1
    assert "HTTP 400" in error.value.detail and "schema too deep" in error.value.detail


def test_a_cut_off_reply_says_which_settings_help(ollama):
    ollama.answer = reply("", done_reason="length")
    with pytest.raises(llm.LLMError) as error:
        llm.chat_json("s", "u", SCHEMA)
    assert error.value.status == llm.INVALID_OUTPUT
    assert "DIAGNOSE_NUM_PREDICT" in error.value.detail and "DIAGNOSE_THINK" in error.value.detail
