"""
Voice transcription routes — Azure GPT-4o with Web Speech API fallback.
"""
import logging
import os
import re

from flask import Blueprint, jsonify, request

logger = logging.getLogger(__name__)

voice_bp = Blueprint("voice", __name__)

# Azure OpenAI configuration — all models on the same endpoint
AZURE_ENDPOINT = os.environ.get("AZURE_o1_API_BASE", "")
AZURE_API_KEY = os.environ.get("AZURE_o1_API_KEY", "")
AZURE_API_VERSION = os.environ.get("AZURE_API_VERSION", "2024-12-01-preview")

# Model deployments
TRANSCRIBE_DEPLOYMENT = os.environ.get("AZURE_TRANSCRIBE_DEPLOYMENT", "gpt-4o-transcribe")
CHAT_DEPLOYMENT = os.environ.get("AZURE_CHAT_DEPLOYMENT", "gpt-4.1")


# (connect, read) — a hung Azure endpoint would otherwise pin a Flask worker forever.
_TIMEOUT = (5, 30)


def _azure_available():
    return bool(AZURE_ENDPOINT and AZURE_API_KEY)


def _is_non_english(text):
    non_ascii = sum(1 for c in text if ord(c) > 127)
    return non_ascii > len(text) * 0.05


def _azure_url(deployment, path):
    return (
        f"{AZURE_ENDPOINT.rstrip('/')}/openai/deployments/"
        f"{deployment}/{path}"
        f"?api-version={AZURE_API_VERSION}"
    )


def _chat(system_prompt: str, user_text: str) -> str | None:
    """Single-turn chat completion. Returns the message text, or None if the call failed."""
    import requests as req

    resp = req.post(
        _azure_url(CHAT_DEPLOYMENT, "chat/completions"),
        headers={"api-key": AZURE_API_KEY, "Content-Type": "application/json"},
        json={
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_text},
            ],
            "temperature": 0,
        },
        timeout=_TIMEOUT,
    )

    if resp.status_code == 200:
        return resp.json()["choices"][0]["message"]["content"].strip()
    return None


def _translate_to_english(text):
    return _chat(
        "Translate the following text to English. Keep scientific terms, instrument names, "
        "and technical vocabulary as-is. Return only the translation, nothing else.",
        text,
    )


def extract_keywords(comments: str, instrument_name: str = "") -> list[str]:
    """Extract scientifically relevant keywords from session comments using GPT-4.1."""
    if not comments.strip() or not _azure_available():
        return []

    prompt = (
        "Extract scientifically relevant keywords from the following TEM session notes. "
        "The notes may be in any language — always return keywords in English. "
        "Return keywords as a JSON array of short strings (1-3 words each). "
        "Focus on: techniques (e.g. STEM, HAADF, EELS, EDS, diffraction), "
        "materials, sample properties, experimental conditions, "
        "and measurement parameters. Exclude generic words. "
        "Return ONLY the JSON array, no other text."
    )

    try:
        content = _chat(prompt, f"Instrument: {instrument_name}\nNotes: {comments}")
        if content:
            import json
            keywords = json.loads(content)
            if isinstance(keywords, list):
                return [str(k).strip() for k in keywords if k]
    except Exception as e:
        logger.warning(f"Keyword extraction failed: {e}")

    return []


@voice_bp.post("/api/keywords/extract")
def keywords_extract():
    data = request.json or {}
    comments = data.get("comments", "").strip()
    instrument_name = data.get("instrument_name", "")
    if not comments:
        return jsonify({"keywords": []})
    keywords = extract_keywords(comments, instrument_name)
    return jsonify({"keywords": keywords})


@voice_bp.get("/api/voice/status")
def voice_status():
    return jsonify({"available": _azure_available()})


@voice_bp.post("/api/voice/transcribe")
def voice_transcribe():
    if not _azure_available():
        return jsonify({"error": "Azure transcription not configured"}), 503

    audio_file = request.files.get("audio")
    if not audio_file:
        return jsonify({"error": "No audio file provided"}), 400

    try:
        import requests as req

        resp = req.post(
            _azure_url(TRANSCRIBE_DEPLOYMENT, "audio/transcriptions"),
            headers={"api-key": AZURE_API_KEY},
            files={"file": ("recording.webm", audio_file.stream, "audio/webm")},
            timeout=_TIMEOUT,
        )

        if resp.status_code != 200:
            return jsonify({"error": f"Azure API error: {resp.status_code} {resp.text}"}), 502

        text = resp.json().get("text", "")
        translation = None

        if text and _is_non_english(text):
            try:
                translation = _translate_to_english(text)
            except Exception as e:
                logger.warning(f"Translation failed: {e}")

        return jsonify({"text": text, "translation": translation})
    except Exception as e:
        return jsonify({"error": str(e)}), 500
