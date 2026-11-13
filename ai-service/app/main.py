"""
BodhNet AI Service — main.py
Automated Knowledge Management and Query System
------------------------------------------------------
FastAPI application that acts as a bridge between the
Java Servlet layer (Tomcat 11) and the local Ollama
instance running qwen3:4b on Apple Silicon.

Endpoints:
  GET  /health                            — liveness probe
  POST /v1/chat                           — RAG-augmented SSE streaming chat proxy
  GET  /v1/chat/history/{conversation_id} — retrieve persisted message history from Oracle
"""

import asyncio
import json
import logging
import re
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import AsyncGenerator

import chromadb
import httpx
from chromadb.config import Settings
from fastapi import BackgroundTasks, FastAPI, HTTPException, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer
from sqlalchemy import select

from app.database import close_db, get_db_session, init_db
from app.models import ChatMessage, ChatSession

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("bodhnet.ai")

# ---------------------------------------------------------------------------
# Configuration (override via env-vars if needed)
# ---------------------------------------------------------------------------
OLLAMA_BASE_URL: str = "http://localhost:11434"
OLLAMA_MODEL: str = "qwen3:4b"
OLLAMA_TIMEOUT_SECS: float = 120.0   # generous budget for local Silicon inference

# ---------------------------------------------------------------------------
# RAG configuration
# ---------------------------------------------------------------------------
_SERVICE_ROOT    = Path(__file__).resolve().parent.parent   # ~/BodhNet/ai-service/
CHROMA_DIR       = _SERVICE_ROOT / "chroma_db"
COLLECTION_NAME  = "bodhnet_knowledge"
EMBED_MODEL_NAME = "all-MiniLM-L6-v2"
RAG_TOP_K        = 3     # number of chunks retrieved per query

# ---------------------------------------------------------------------------
# Persona — injected as the `system` field in every Ollama request.
# Strict constraints eliminate generic AI filler and enforce a grounded,
# direct human-expert tone.
# ---------------------------------------------------------------------------
SYSTEM_PROMPT: str = """You are BodhNet — a razor-sharp technical and academic knowledge assistant.

Tone & Behavior Rules (Non-Negotiable):
- Speak like a senior engineer or professor explaining something to a sharp colleague. Direct, clear, no fluff.
- Never open with pleasantries or affirmations. Get straight to the substance.
- Use concrete examples, precise terminology, and real code when the question warrants it.
- If a question is ambiguous, state your interpretation briefly and answer it. Don't ask for clarification unless genuinely blocked.
- Match the depth of the answer to the complexity of the question. Short questions get tight answers. Deep questions get thorough ones.
- When writing code, always use a fenced code block with the correct language tag (e.g. ```python, ```java, ```sql).

Absolutely Banned Phrases and Patterns — never use these under any circumstances:
- "As an AI language model" or any variant
- "Certainly!", "Absolutely!", "Of course!", "Sure!", "Great question!"
- "Here is a breakdown of...", "Here's a summary of...", "Let me explain..."
- "In conclusion", "To summarize", "In summary"
- "I hope this helps", "I hope that clarifies", "Feel free to ask"
- "It's important to note that", "It's worth mentioning that"
- Excessive apologies or self-deprecation of any kind
- Filler transitions like "Now, let's dive into..." or "Moving on..."

Output Format:
- Use markdown: headers, bullet points, numbered steps, and code blocks where they genuinely improve clarity.
- Do NOT use markdown for conversational one-liners — plain prose is fine there.
- Never pad the answer to seem more thorough. Brevity is quality."""

# ---------------------------------------------------------------------------
# FastAPI application
# ---------------------------------------------------------------------------
app = FastAPI(
    title="BodhNet AI Service",
    description=(
        "Local AI inference bridge for BodhNet: "
        "Automated Knowledge Management and Query System. "
        "Routes chat traffic from the Jakarta Servlet layer to a local "
        "Ollama instance running qwen3:4b on Apple Silicon."
    ),
    version="2.0.0",
)

# Allow the Tomcat-served frontend (localhost:8080) to call this service
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080", "http://127.0.0.1:8080"],
    allow_methods=["GET", "POST", "OPTIONS"],
    allow_headers=["Content-Type", "Accept"],
)

# ---------------------------------------------------------------------------
# Shared async HTTP client — must use stream-capable timeout config
# ---------------------------------------------------------------------------
_ollama_client: httpx.AsyncClient | None = None

# ---------------------------------------------------------------------------
# RAG — ChromaDB collection and embedding model (loaded once at startup)
# ---------------------------------------------------------------------------
_chroma_collection = None
_embedder: SentenceTransformer | None = None

# ---------------------------------------------------------------------------
# DB — flag set to False if Oracle is unreachable at startup
# ---------------------------------------------------------------------------
_db_available: bool = False


@app.on_event("startup")
async def _startup() -> None:
    global _ollama_client, _chroma_collection, _embedder, _db_available

    # The streaming client must NOT use response.json() — it streams raw bytes.
    # We set no top-level read timeout here; the per-request stream timeout
    # is controlled by the timeout= on each request.build_request() call.
    _ollama_client = httpx.AsyncClient(
        base_url=OLLAMA_BASE_URL,
        timeout=httpx.Timeout(connect=10.0, read=None, write=10.0, pool=5.0),
        headers={"Content-Type": "application/json"},
    )

    # Initialise ChromaDB — gracefully degrade if the DB doesn't exist yet
    if CHROMA_DIR.exists():
        try:
            chroma_client = chromadb.PersistentClient(
                path=str(CHROMA_DIR),
                settings=Settings(anonymized_telemetry=False),
            )
            _chroma_collection = chroma_client.get_or_create_collection(
                name=COLLECTION_NAME,
                metadata={"hnsw:space": "cosine"},
            )
            _embedder = SentenceTransformer(EMBED_MODEL_NAME)
            log.info(
                "RAG ready — collection '%s' has %d chunk(s). Embedding model: %s",
                COLLECTION_NAME,
                _chroma_collection.count(),
                EMBED_MODEL_NAME,
            )
        except Exception as exc:
            log.warning("RAG initialisation failed — running without retrieval: %s", exc)
            _chroma_collection = None
            _embedder = None
    else:
        log.warning(
            "chroma_db/ not found at %s — RAG disabled. "
            "Run `python -m app.ingest` to build the vector store.",
            CHROMA_DIR,
        )

    # Initialise Oracle schema — gracefully degrade if Oracle is not running
    try:
        await init_db()
        _db_available = True
        log.info("Oracle persistence: ENABLED")
    except Exception as exc:
        _db_available = False
        log.warning(
            "Oracle persistence: DISABLED (DB unreachable at startup). "
            "Chat will work but messages will NOT be saved. Error: %s",
            exc,
        )

    log.info("BodhNet AI Service started — Ollama target: %s  model: %s", OLLAMA_BASE_URL, OLLAMA_MODEL)


@app.on_event("shutdown")
async def _shutdown() -> None:
    if _ollama_client:
        await _ollama_client.aclose()
    await close_db()
    log.info("BodhNet AI Service shut down cleanly.")


# ---------------------------------------------------------------------------
# Pydantic request / response models
# ---------------------------------------------------------------------------
class HistoryEntry(BaseModel):
    role: str = Field(..., description="'user' or 'assistant'")
    content: str = Field(..., description="Message content")


class ChatRequest(BaseModel):
    conversation_id: str | None = Field(
        default=None,
        description="Optional UUID that tracks the conversation. Auto-generated if absent.",
    )
    message: str = Field(..., min_length=1, description="The user's current message.")
    history: list[HistoryEntry] = Field(
        default_factory=list,
        description="Recent conversation turns for multi-turn context (newest last).",
    )


class HealthResponse(BaseModel):
    status: str
    service: str
    model: str
    ollama_url: str
    rag_chunks: int
    db_connected: bool


# ---------------------------------------------------------------------------
# Pydantic schemas for history endpoint responses
# ---------------------------------------------------------------------------
class MessageRecord(BaseModel):
    id: int
    role: str
    content: str
    timestamp: datetime

    class Config:
        from_attributes = True


class HistoryResponse(BaseModel):
    conversation_id: str
    message_count: int
    messages: list[MessageRecord]


# ---------------------------------------------------------------------------
# RAG — retrieve relevant context chunks for a query
# ---------------------------------------------------------------------------
def _retrieve_context(query: str, top_k: int = RAG_TOP_K) -> list[str]:
    """
    Embed the user query and fetch the top-k most semantically similar
    document chunks from ChromaDB.  Returns an empty list when the vector
    store is unavailable or contains no data.
    """
    if _chroma_collection is None or _embedder is None:
        return []

    if _chroma_collection.count() == 0:
        return []

    try:
        query_embedding = _embedder.encode(
            [query],
            convert_to_numpy=True,
            normalize_embeddings=True,
        ).tolist()

        results = _chroma_collection.query(
            query_embeddings=query_embedding,
            n_results=min(top_k, _chroma_collection.count()),
            include=["documents", "metadatas", "distances"],
        )

        chunks: list[str] = results.get("documents", [[]])[0]
        sources: list[dict] = results.get("metadatas", [[]])[0]

        labelled: list[str] = []
        for chunk, meta in zip(chunks, sources):
            src = meta.get("source", "unknown")
            labelled.append(f"[Source: {src}]\n{chunk}")

        return labelled

    except Exception as exc:
        log.warning("RAG retrieval failed — proceeding without context: %s", exc)
        return []


# ---------------------------------------------------------------------------
# Helper — build a prompt that gives Ollama the conversation context
# ---------------------------------------------------------------------------
def _build_prompt(history: list[HistoryEntry], user_message: str) -> str:
    """
    Constructs a plain-text prompt for Ollama's /api/generate endpoint.
    The system persona is injected separately via the top-level `system`
    field in the Ollama payload — this function handles only the
    conversational context (history turns + current user message).
    """
    parts: list[str] = []

    if history:
        parts.append("\n--- Conversation History ---")
        for turn in history:
            role_label = "User" if turn.role == "user" else "Assistant"
            parts.append(f"{role_label}: {turn.content}")
        parts.append("--- End of History ---")

    parts.append(f"\nUser: {user_message}\nAssistant:")

    return "\n".join(parts)


# ---------------------------------------------------------------------------
# Oracle background task — called after stream completes
# ---------------------------------------------------------------------------
async def _persist_to_oracle(
    conversation_id: str,
    user_message: str,
    assistant_answer: str,
) -> None:
    """
    Saves the user message and assistant answer to Oracle.
    Designed to run as a FastAPI BackgroundTask after the SSE stream ends,
    so it never delays the client receiving tokens.
    """
    if not _db_available or not assistant_answer.strip():
        return
    try:
        async with get_db_session() as db:
            existing_session = await db.get(ChatSession, conversation_id)
            if existing_session is None:
                db.add(ChatSession(id=conversation_id))

            db.add(ChatMessage(
                session_id=conversation_id,
                role="user",
                content=user_message,
            ))
            db.add(ChatMessage(
                session_id=conversation_id,
                role="assistant",
                content=assistant_answer,
            ))
        log.info("conv=%s  Messages persisted to Oracle (%d chars).", conversation_id, len(assistant_answer))
    except Exception as exc:
        log.error("conv=%s  Oracle persistence failed (stream was still delivered): %s", conversation_id, exc)


# ---------------------------------------------------------------------------
# SSE streaming generator — core of Phase 3C
# ---------------------------------------------------------------------------
async def _stream_ollama(
    conversation_id: str,
    ollama_body: dict,
    background_tasks: BackgroundTasks,
    user_message: str,
) -> AsyncGenerator[str, None]:
    """
    Streams token chunks from Ollama's /api/generate (stream:true) as
    Server-Sent Events.  Implements a stateful buffer to swallow any
    <think>...</think> block that arrives across chunk boundaries, so
    the internal chain-of-thought is never visible in the UI.

    SSE wire format used:
      data: <token_text>\\n\\n          — a token chunk for the browser
      data: [DONE]\\n\\n                — signals end of stream to the servlet + JS
      data: [ERROR] <message>\\n\\n     — signals a fatal error mid-stream
    """
    accumulated:   list[str] = []   # builds the full answer for Oracle persistence
    think_buffer:  str        = ""  # collects partial <think> tag across chunks
    inside_think:  bool       = False

    try:
        async with _ollama_client.stream(
            "POST",
            "/api/generate",
            json=ollama_body,
            timeout=httpx.Timeout(connect=10.0, read=OLLAMA_TIMEOUT_SECS, write=10.0, pool=5.0),
        ) as ollama_response:

            if ollama_response.status_code != 200:
                err_body = await ollama_response.aread()
                yield f"data: [ERROR] Ollama returned HTTP {ollama_response.status_code}\n\n"
                log.error("conv=%s  Ollama HTTP error %d: %s", conversation_id, ollama_response.status_code, err_body)
                return

            async for raw_line in ollama_response.aiter_lines():
                if not raw_line.strip():
                    continue

                # Each line from Ollama stream is a JSON object
                try:
                    chunk_json: dict = json.loads(raw_line)
                except json.JSONDecodeError:
                    continue

                token: str = chunk_json.get("response", "")
                done: bool = chunk_json.get("done", False)

                # -------------------------------------------------------
                # Stateful <think> tag filter — works across chunk boundaries
                # -------------------------------------------------------
                if token:
                    think_buffer += token
                    output_text = ""

                    while think_buffer:
                        if inside_think:
                            # Looking for the closing </think>
                            end_idx = think_buffer.find("</think>")
                            if end_idx != -1:
                                # Found the close tag — discard everything up to and including it
                                think_buffer = think_buffer[end_idx + len("</think>"):]
                                inside_think = False
                            else:
                                # Closing tag not in buffer yet — wait for more chunks
                                break
                        else:
                            # Looking for an opening <think>
                            start_idx = think_buffer.find("<think>")
                            if start_idx != -1:
                                # Everything before <think> is safe to emit
                                output_text += think_buffer[:start_idx]
                                think_buffer = think_buffer[start_idx + len("<think>"):]
                                inside_think = True
                            else:
                                # No <think> tag anywhere in buffer — check for partial tag at end
                                # Keep the last 7 chars buffered in case "<think>" spans two chunks
                                safe_len = max(0, len(think_buffer) - 7)
                                output_text += think_buffer[:safe_len]
                                think_buffer = think_buffer[safe_len:]
                                break

                    if output_text:
                        accumulated.append(output_text)
                        # SSE: escape newlines inside data field per spec
                        sse_data = output_text.replace("\n", "\\n")
                        yield f"data: {sse_data}\n\n"

                if done:
                    break

    except httpx.ConnectError as exc:
        log.error("conv=%s  Ollama unreachable: %s", conversation_id, exc)
        yield f"data: [ERROR] Cannot connect to Ollama at {OLLAMA_BASE_URL}. Is 'ollama serve' running?\n\n"
        return
    except httpx.ReadTimeout as exc:
        log.error("conv=%s  Ollama stream timed out: %s", conversation_id, exc)
        yield f"data: [ERROR] Ollama did not respond within {OLLAMA_TIMEOUT_SECS}s.\n\n"
        return
    except Exception as exc:
        log.exception("conv=%s  Unexpected streaming error: %s", conversation_id, exc)
        yield f"data: [ERROR] Internal streaming error: {exc}\n\n"
        return

    # Flush any remaining safe buffer content that wasn't emitted
    if think_buffer and not inside_think:
        accumulated.append(think_buffer)
        sse_data = think_buffer.replace("\n", "\\n")
        yield f"data: {sse_data}\n\n"

    # Signal end-of-stream to the Java servlet and browser client
    yield "data: [DONE]\n\n"

    # Schedule Oracle persistence as a background task — runs after response is sent
    full_answer = "".join(accumulated).strip()
    log.info("conv=%s  Stream complete — %d chars. Scheduling Oracle persistence.", conversation_id, len(full_answer))
    background_tasks.add_task(_persist_to_oracle, conversation_id, user_message, full_answer)


# ---------------------------------------------------------------------------
# GET /health
# ---------------------------------------------------------------------------
@app.get(
    "/health",
    response_model=HealthResponse,
    summary="Liveness probe",
    tags=["Ops"],
)
async def health() -> HealthResponse:
    """Returns a JSON status confirming the BodhNet AI Service is running."""
    return HealthResponse(
        status="UP",
        service="BodhNet AI Service",
        model=OLLAMA_MODEL,
        ollama_url=OLLAMA_BASE_URL,
        rag_chunks=_chroma_collection.count() if _chroma_collection else 0,
        db_connected=_db_available,
    )


# ---------------------------------------------------------------------------
# POST /v1/chat  — SSE Streaming
# ---------------------------------------------------------------------------
@app.post(
    "/v1/chat",
    summary="Send a message to the local LLM via Ollama (SSE streaming)",
    tags=["Chat"],
    response_class=StreamingResponse,
)
async def chat(payload: ChatRequest, background_tasks: BackgroundTasks) -> StreamingResponse:
    """
    Receives a chat request from the Java ChatServlet, performs RAG retrieval,
    then streams the Ollama response token-by-token as Server-Sent Events.
    <think> tags are swallowed in the stream generator.
    Oracle persistence is scheduled as a BackgroundTask after the stream ends.
    """
    conversation_id = payload.conversation_id or str(uuid.uuid4())

    # ------------------------------------------------------------------
    # RAG — retrieve relevant document context before building the prompt
    # ------------------------------------------------------------------
    context_chunks = _retrieve_context(payload.message)

    if context_chunks:
        context_block = "\n\n".join(context_chunks)
        rag_system = (
            SYSTEM_PROMPT
            + "\n\n"
            + "--- Relevant Knowledge Base Context ---\n"
            + "Use the following retrieved excerpts as your primary factual source "
            "when answering. Cite the source filename in brackets when referencing them.\n\n"
            + context_block
            + "\n--- End of Context ---"
        )
        log.info("conv=%s  RAG injected %d chunk(s) into system prompt.", conversation_id, len(context_chunks))
    else:
        rag_system = SYSTEM_PROMPT
        log.info("conv=%s  No RAG context (vector store empty or unavailable).", conversation_id)

    prompt = _build_prompt(payload.history, payload.message)

    ollama_body = {
        "model": OLLAMA_MODEL,
        "system": rag_system,
        "prompt": prompt,
        "stream": True,              # ← KEY CHANGE: streaming mode
        "options": {
            "temperature": 0.3,
            "top_k": 40,
            "top_p": 0.9,
            "num_ctx": 8192,         # ← expanded context window (was default 2048)
            "num_predict": -1,       # ← unlimited generation length (was capped at 1024)
        },
    }

    log.info(
        "conv=%s  Starting SSE stream to Ollama (model=%s, history_turns=%d)",
        conversation_id,
        OLLAMA_MODEL,
        len(payload.history),
    )

    headers = {
        "X-Conversation-Id": conversation_id,   # lets the Java servlet extract the ID
        "Cache-Control": "no-cache",
        "X-Accel-Buffering": "no",               # disables Nginx proxy buffering if in use
    }

    return StreamingResponse(
        content=_stream_ollama(
            conversation_id=conversation_id,
            ollama_body=ollama_body,
            background_tasks=background_tasks,
            user_message=payload.message,
        ),
        media_type="text/event-stream",
        headers=headers,
    )


# ---------------------------------------------------------------------------
# GET /v1/chat/history/{conversation_id}
# ---------------------------------------------------------------------------
@app.get(
    "/v1/chat/history/{conversation_id}",
    response_model=HistoryResponse,
    summary="Retrieve persisted message history for a conversation",
    tags=["Chat"],
)
async def get_history(conversation_id: str) -> HistoryResponse:
    """
    Fetches all stored messages for the given conversation_id from Oracle,
    ordered chronologically (oldest first).  Returns 404 if the session
    does not exist, and 503 if Oracle is unavailable.
    """
    if not _db_available:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Oracle persistence is not available. Check DB connection and restart the service.",
        )

    async with get_db_session() as db:
        session_row = await db.get(ChatSession, conversation_id)
        if session_row is None:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"No conversation found with id '{conversation_id}'.",
            )

        result = await db.execute(
            select(ChatMessage)
            .where(ChatMessage.session_id == conversation_id)
            .order_by(ChatMessage.timestamp)
        )
        messages: list[ChatMessage] = list(result.scalars().all())

    return HistoryResponse(
        conversation_id=conversation_id,
        message_count=len(messages),
        messages=[
            MessageRecord(
                id=m.id,
                role=m.role,
                content=m.content,
                timestamp=m.timestamp,
            )
            for m in messages
        ],
    )


# ---------------------------------------------------------------------------
# Generic exception handlers — ensures JSON error responses always
# ---------------------------------------------------------------------------
@app.exception_handler(Exception)
async def _unhandled_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    log.exception("Unhandled exception on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"error": "Internal server error.", "details": str(exc)},
    )
