"""
serializer.py - Empacotamento/desempacotamento de mensagens para transporte TCP.

Formato de frame (length-prefixed):
  [4 bytes big-endian uint32 = tamanho do payload JSON] [payload JSON em UTF-8]

Para SendBestModel (que carrega bytes binários), o campo model_bytes é
codificado em base64 dentro do JSON e decodificado na outra ponta.
"""

import base64
import json
import struct
from typing import Any, Dict

HEADER_SIZE = 4


def encode(data: Dict[str, Any]) -> bytes:
    """Serialize dict into socket-ready bytes."""
    payload = _to_json_safe(data)
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    header = struct.pack(">I", len(body))
    return header + body


def decode_header(raw_header: bytes) -> int:
    """Read payload size from header."""
    if len(raw_header) < HEADER_SIZE:
        raise ValueError("Cabeçalho incompleto")
    (size,) = struct.unpack(">I", raw_header)
    return size


def decode_body(raw_body: bytes) -> Dict[str, Any]:
    """Deserialize JSON payload."""
    data = json.loads(raw_body.decode("utf-8"))
    return _from_json_safe(data)


# helpers ─

def _to_json_safe(obj: Any) -> Any:
    """Troca bytes por base64."""
    if isinstance(obj, bytes):
        return {"__bytes__": base64.b64encode(obj).decode("ascii")}

    if isinstance(obj, dict):
        return {k: _to_json_safe(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_to_json_safe(i) for i in obj]

    return obj


def _from_json_safe(obj: Any) -> Any:
    """Reconstrói bytes do base64."""
    if isinstance(obj, dict):
        if "__bytes__" in obj:
            return base64.b64decode(obj["__bytes__"])

        return {k: _from_json_safe(v) for k, v in obj.items()}

    if isinstance(obj, list):
        return [_from_json_safe(i) for i in obj]

    return obj