"""Optional embedding function for incident recall (Ollama /api/embed).

Off unless LOOKUP_EMBED_MODEL is set. Its vector size must match
`vector.dimensions` of the incident_embedding index in lineage/schema.cypher
(768 for nomic-embed-text), and the Learn node must embed with the same model.
"""
import json
from typing import Callable, List, Optional
from urllib.request import Request, urlopen

import config


def make_embedder() -> Optional[Callable[[str], List[float]]]:
    if not config.EMBED_MODEL:
        return None
    url = config.EMBED_BASE_URL.rstrip("/") + "/api/embed"

    def embed(text: str) -> List[float]:
        body = json.dumps({"model": config.EMBED_MODEL, "input": text}).encode()
        request = Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
        with urlopen(request, timeout=30) as response:
            return json.loads(response.read())["embeddings"][0]

    return embed
