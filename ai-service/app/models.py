"""
BodhNet AI Service — app/models.py
Automated Knowledge Management and Query System
------------------------------------------------------
SQLAlchemy ORM models for Oracle persistence (Phase 3A).

Tables:
  BODHNET_SESSIONS  — one row per unique conversation_id
  BODHNET_MESSAGES  — one row per user/assistant turn, FK → SESSIONS
"""

import uuid
from datetime import datetime, timezone

from sqlalchemy import (
    DateTime,
    ForeignKey,
    Identity,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


# ---------------------------------------------------------------------------
# Declarative base — all models inherit from this
# ---------------------------------------------------------------------------
class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# ChatSession
# ---------------------------------------------------------------------------
class ChatSession(Base):
    """
    Represents a logical conversation identified by a UUID string.
    Created lazily on first message of a new conversation_id.
    """
    __tablename__ = "BODHNET_SESSIONS"

    # Oracle VARCHAR2 primary key — UUID stored as 36-char string
    id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        comment="Conversation UUID (36-char string)",
    )

    # TIMESTAMP WITH TIME ZONE — server-side default so the DB clock is used
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.current_timestamp(),
        nullable=False,
        comment="UTC timestamp when the session was first created",
    )

    # Reverse relationship for convenience
    messages: Mapped[list["ChatMessage"]] = relationship(
        "ChatMessage",
        back_populates="session",
        cascade="all, delete-orphan",
        order_by="ChatMessage.timestamp",
    )

    def __repr__(self) -> str:
        return f"<ChatSession id={self.id!r} created_at={self.created_at!r}>"


# ---------------------------------------------------------------------------
# ChatMessage
# ---------------------------------------------------------------------------
class ChatMessage(Base):
    """
    Stores a single conversational turn — either a 'user' query or an
    'assistant' answer — linked to its parent ChatSession.
    """
    __tablename__ = "BODHNET_MESSAGES"

    # Surrogate integer PK — Oracle GENERATED ALWAYS AS IDENTITY
    # Identity() emits the correct DDL for Oracle 12c+ (including 23ai).
    # Without it, SQLAlchemy emits a plain NUMBER column and Oracle never
    # assigns a value, causing ORA-01400: cannot insert NULL into ID.
    id: Mapped[int] = mapped_column(
        Integer,
        Identity(always=True),    # → GENERATED ALWAYS AS IDENTITY in Oracle DDL
        primary_key=True,
        comment="Surrogate primary key (Oracle IDENTITY column)",
    )

    session_id: Mapped[str] = mapped_column(
        String(36),
        ForeignKey("BODHNET_SESSIONS.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
        comment="FK → BODHNET_SESSIONS.id",
    )

    # 'user' or 'assistant'
    role: Mapped[str] = mapped_column(
        String(16),
        nullable=False,
        comment="Message author: 'user' or 'assistant'",
    )

    # Oracle CLOB maps to SQLAlchemy Text — supports long model responses
    content: Mapped[str] = mapped_column(
        Text,
        nullable=False,
        comment="Full text content of the message",
    )

    timestamp: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.current_timestamp(),
        nullable=False,
        index=True,
        comment="UTC timestamp of this message",
    )

    # Relationship back to parent session
    session: Mapped["ChatSession"] = relationship(
        "ChatSession",
        back_populates="messages",
    )

    def __repr__(self) -> str:
        preview = (self.content[:40] + "…") if len(self.content) > 40 else self.content
        return f"<ChatMessage id={self.id} role={self.role!r} content={preview!r}>"

