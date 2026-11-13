"""
BodhNet AI Service — app/ingest.py
Automated Knowledge Management and Query System
------------------------------------------------------
Standalone document ingestion script.

Run this script once (or whenever your document library changes) to:
  1. Read all PDF and TXT files from ~/BodhNet/ai-service/docs/
  2. Chunk each document into overlapping text segments
  3. Generate dense vector embeddings using a local sentence-transformers model
     (all-MiniLM-L6-v2 — 22MB, fast on Apple Silicon, zero network calls after
     the first automatic download from HuggingFace)
  4. Store the chunks + embeddings in a persistent ChromaDB collection at
     ~/BodhNet/ai-service/chroma_db/

Usage:
    cd ~/BodhNet/ai-service
    source .venv/bin/activate
    python -m app.ingest                  # ingest everything in docs/
    python -m app.ingest --reset          # wipe the collection and re-ingest
"""

import argparse
import logging
import os
import sys
from pathlib import Path

import chromadb
from chromadb.config import Settings
from pypdf import PdfReader
from sentence_transformers import SentenceTransformer

# ---------------------------------------------------------------------------
# Paths  (resolved relative to this file so the script works from any cwd)
# ---------------------------------------------------------------------------
_SERVICE_ROOT = Path(__file__).resolve().parent.parent   # ~/BodhNet/ai-service/
DOCS_DIR      = _SERVICE_ROOT / "docs"
CHROMA_DIR    = _SERVICE_ROOT / "chroma_db"

# ---------------------------------------------------------------------------
# Ingestion configuration
# ---------------------------------------------------------------------------
COLLECTION_NAME:   str = "bodhnet_knowledge"
EMBED_MODEL_NAME:  str = "all-MiniLM-L6-v2"   # 22MB — fast, local, Apple Silicon friendly
CHUNK_SIZE:        int = 500     # characters per chunk
CHUNK_OVERLAP:     int = 100     # characters of overlap between consecutive chunks

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("bodhnet.ingest")


# ---------------------------------------------------------------------------
# Text extraction helpers
# ---------------------------------------------------------------------------

def _extract_text_from_pdf(path: Path) -> str:
    """Extract plain text from a PDF file using pypdf."""
    reader = PdfReader(str(path))
    pages: list[str] = []
    for page in reader.pages:
        text = page.extract_text()
        if text:
            pages.append(text)
    return "\n".join(pages)


def _extract_text_from_txt(path: Path) -> str:
    """Read a plain-text file, trying UTF-8 then latin-1 as fallback."""
    try:
        return path.read_text(encoding="utf-8")
    except UnicodeDecodeError:
        return path.read_text(encoding="latin-1")


def _load_document(path: Path) -> str | None:
    """Dispatch to the correct extractor based on file extension."""
    suffix = path.suffix.lower()
    if suffix == ".pdf":
        log.info("  Extracting PDF  : %s", path.name)
        return _extract_text_from_pdf(path)
    if suffix == ".txt":
        log.info("  Extracting TXT  : %s", path.name)
        return _extract_text_from_txt(path)
    log.warning("  Skipping unsupported file type: %s", path.name)
    return None


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def _chunk_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> list[str]:
    """
    Split text into overlapping fixed-size character chunks.

    Overlap ensures that sentences split across a boundary are still
    semantically retrievable from either neighbouring chunk.
    """
    chunks: list[str] = []
    start = 0
    text_len = len(text)

    while start < text_len:
        end = min(start + chunk_size, text_len)
        chunk = text[start:end].strip()
        if chunk:
            chunks.append(chunk)
        start += chunk_size - overlap   # slide forward with overlap

    return chunks


# ---------------------------------------------------------------------------
# Main ingestion routine
# ---------------------------------------------------------------------------

def ingest(reset: bool = False) -> None:
    """
    Full ingestion pipeline:
      docs/ → extract → chunk → embed → store in ChromaDB
    """
    # -- Validate docs directory -------------------------------------------
    if not DOCS_DIR.exists():
        log.error("docs/ directory not found at %s — create it and add documents.", DOCS_DIR)
        sys.exit(1)

    doc_files = [
        p for p in sorted(DOCS_DIR.iterdir())
        if p.is_file() and p.suffix.lower() in {".pdf", ".txt"}
    ]

    if not doc_files:
        log.warning("No PDF or TXT files found in %s. Nothing to ingest.", DOCS_DIR)
        sys.exit(0)

    log.info("Found %d document(s) to ingest in %s", len(doc_files), DOCS_DIR)

    # -- Initialise ChromaDB -----------------------------------------------
    CHROMA_DIR.mkdir(parents=True, exist_ok=True)
    chroma_client = chromadb.PersistentClient(
        path=str(CHROMA_DIR),
        settings=Settings(anonymized_telemetry=False),
    )

    if reset:
        try:
            chroma_client.delete_collection(COLLECTION_NAME)
            log.info("Existing collection '%s' deleted (--reset flag).", COLLECTION_NAME)
        except Exception:
            pass   # collection didn't exist yet — that's fine

    collection = chroma_client.get_or_create_collection(
        name=COLLECTION_NAME,
        metadata={"hnsw:space": "cosine"},   # cosine similarity for semantic search
    )

    log.info("ChromaDB collection '%s' ready at %s", COLLECTION_NAME, CHROMA_DIR)

    # -- Load embedding model ----------------------------------------------
    log.info("Loading embedding model '%s' (downloads once, ~22MB) ...", EMBED_MODEL_NAME)
    embedder = SentenceTransformer(EMBED_MODEL_NAME)
    log.info("Embedding model loaded.")

    # -- Process each document ---------------------------------------------
    total_chunks_added = 0

    for doc_path in doc_files:
        raw_text = _load_document(doc_path)
        if not raw_text or not raw_text.strip():
            log.warning("  No extractable text in %s — skipping.", doc_path.name)
            continue

        chunks = _chunk_text(raw_text)
        log.info("  Chunked into %d segment(s).", len(chunks))

        if not chunks:
            continue

        # Build unique IDs — source_filename + chunk index
        doc_stem = doc_path.stem.replace(" ", "_")
        ids = [f"{doc_stem}_chunk_{i}" for i in range(len(chunks))]

        # Check which IDs already exist so we never double-ingest
        existing = set(collection.get(ids=ids)["ids"])
        new_indices = [i for i, cid in enumerate(ids) if cid not in existing]

        if not new_indices:
            log.info("  All chunks already present in ChromaDB — skipping %s.", doc_path.name)
            continue

        new_chunks = [chunks[i] for i in new_indices]
        new_ids    = [ids[i]    for i in new_indices]

        log.info(
            "  Embedding %d new chunk(s) (skipping %d already stored) ...",
            len(new_chunks),
            len(chunks) - len(new_chunks),
        )

        # Generate embeddings — SentenceTransformer handles batching internally
        embeddings = embedder.encode(
            new_chunks,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=True,   # required for cosine similarity to work correctly
        ).tolist()

        metadatas = [{"source": doc_path.name, "chunk_index": new_indices[j]} for j in range(len(new_chunks))]

        collection.add(
            ids=new_ids,
            documents=new_chunks,
            embeddings=embeddings,
            metadatas=metadatas,
        )

        total_chunks_added += len(new_chunks)
        log.info("  ✓ Stored %d chunk(s) from '%s'.", len(new_chunks), doc_path.name)

    log.info(
        "Ingestion complete. %d new chunk(s) added. "
        "Total collection size: %d chunk(s).",
        total_chunks_added,
        collection.count(),
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="BodhNet document ingestion — loads docs/ into ChromaDB."
    )
    parser.add_argument(
        "--reset",
        action="store_true",
        help="Delete and fully rebuild the ChromaDB collection before ingesting.",
    )
    args = parser.parse_args()
    ingest(reset=args.reset)

