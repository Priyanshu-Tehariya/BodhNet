/**
 * BodhNet: Automated Knowledge Management and Query System
 * Frontend Chat Controller — Phase 3C: SSE Streaming
 *
 * Flow:
 *  1. User submits message → POST to /chat servlet
 *  2. Servlet returns text/event-stream (SSE)
 *  3. fetch() ReadableStream reader consumes chunks
 *  4. Each "data: <token>" event is decoded and appended to a live bubble
 *  5. On "data: [DONE]" the bubble is finalised with markdown rendering
 *  6. On "data: [META] <id>" the conversation_id is synced from server
 *  7. On "data: [ERROR] <msg>" an error bubble is rendered
 */
document.addEventListener('DOMContentLoaded', () => {
  // Elements
  const chatMessages    = document.getElementById('chatMessages');
  const chatInput       = document.getElementById('chatInput');
  const sendBtn         = document.getElementById('sendBtn');
  const welcomeScreen   = document.getElementById('welcomeScreen');
  const newChatBtn      = document.getElementById('newChatBtn');
  const clearChatBtn    = document.getElementById('clearChatBtn');
  const sidebar         = document.getElementById('sidebar');
  const sidebarToggle   = document.getElementById('sidebarToggle');
  const sidebarClose    = document.getElementById('sidebarClose');
  const promptChips     = document.querySelectorAll('.prompt-chip');

  // Application State
  let currentConversationId = 'conv_' + Date.now();
  let conversationHistory   = [];
  let isGenerating          = false;
  let activeAbortController = null;   // allows stream cancellation on new chat

  // Endpoint
  const contextPath      = typeof CONTEXT_PATH !== 'undefined' ? CONTEXT_PATH : '';
  const CHAT_API_ENDPOINT = `${contextPath}/chat`;

  // -------------------------------------------------------------------------
  // Textarea auto-resize
  // -------------------------------------------------------------------------
  function adjustTextareaHeight() {
    chatInput.style.height = 'auto';
    chatInput.style.height = Math.min(chatInput.scrollHeight, 180) + 'px';
    sendBtn.disabled = chatInput.value.trim().length === 0 || isGenerating;
  }

  chatInput.addEventListener('input', adjustTextareaHeight);

  chatInput.addEventListener('keydown', (e) => {
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      if (!sendBtn.disabled && !isGenerating) handleSendMessage();
    }
  });

  sendBtn.addEventListener('click', () => {
    if (!isGenerating && chatInput.value.trim().length > 0) handleSendMessage();
  });

  // -------------------------------------------------------------------------
  // Prompt chips
  // -------------------------------------------------------------------------
  promptChips.forEach(chip => {
    chip.addEventListener('click', () => {
      const promptText = chip.getAttribute('data-prompt') || chip.querySelector('.chip-title').textContent;
      chatInput.value = promptText;
      adjustTextareaHeight();
      handleSendMessage();
    });
  });

  // -------------------------------------------------------------------------
  // New / Clear chat
  // -------------------------------------------------------------------------
  newChatBtn.addEventListener('click', startNewChat);
  if (clearChatBtn) clearChatBtn.addEventListener('click', startNewChat);

  function startNewChat() {
    if (activeAbortController) {
      activeAbortController.abort();
      activeAbortController = null;
    }
    isGenerating = false;
    currentConversationId = 'conv_' + Date.now();
    conversationHistory   = [];
    chatMessages.innerHTML = '';
    if (welcomeScreen) {
      welcomeScreen.style.display = 'flex';
      chatMessages.appendChild(welcomeScreen);
    }
    chatInput.value = '';
    adjustTextareaHeight();
    chatInput.focus();
    if (window.innerWidth <= 768 && sidebar.classList.contains('open')) {
      sidebar.classList.remove('open');
    }
  }

  // -------------------------------------------------------------------------
  // Mobile sidebar
  // -------------------------------------------------------------------------
  if (sidebarToggle) sidebarToggle.addEventListener('click', () => sidebar.classList.toggle('open'));
  if (sidebarClose)  sidebarClose.addEventListener('click',  () => sidebar.classList.remove('open'));

  // =========================================================================
  // Core: Send Message with SSE streaming
  // =========================================================================
  async function handleSendMessage() {
    const rawMessage = chatInput.value.trim();
    if (!rawMessage || isGenerating) return;

    // Remove welcome screen
    if (welcomeScreen && welcomeScreen.parentNode === chatMessages) {
      chatMessages.removeChild(welcomeScreen);
    }

    // User bubble
    appendMessage('user', rawMessage);
    conversationHistory.push({ role: 'user', content: rawMessage });

    chatInput.value = '';
    adjustTextareaHeight();

    // Typing indicator (replaced by live bubble on first token)
    isGenerating = true;
    sendBtn.disabled = true;
    const loadingBubble = appendLoadingBubble();
    scrollToBottom();

    // Create a live assistant bubble that will receive streaming tokens
    let liveBubbleRow   = null;
    let liveBubbleEl    = null;
    let rawAccumulated  = '';        // raw text as tokens arrive (pre-markdown)
    let streamErrored   = false;

    const abortCtrl = new AbortController();
    activeAbortController = abortCtrl;

    try {
      const payload = {
        conversation_id: currentConversationId,
        message: rawMessage,
        history: conversationHistory.slice(-6),
      };

      const response = await fetch(CHAT_API_ENDPOINT, {
        method:  'POST',
        headers: { 'Content-Type': 'application/json', 'Accept': 'text/event-stream' },
        body:    JSON.stringify(payload),
        signal:  abortCtrl.signal,
      });

      if (!response.ok) {
        // Non-streaming error response from servlet (before stream started)
        removeLoadingBubble(loadingBubble);
        const data = await response.json().catch(() => null);
        appendErrorMessage(
          data?.error || `HTTP ${response.status}: Server error`,
          data?.details || ''
        );
        return;
      }

      // --- Begin reading the SSE stream via fetch ReadableStream -----------
      const reader  = response.body.getReader();
      const decoder = new TextDecoder('utf-8');
      let   buffer  = '';   // accumulates partial lines between read() calls

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;

        buffer += decoder.decode(value, { stream: true });

        // Split on newline — SSE events are newline-terminated
        const lines = buffer.split('\n');
        // Keep the last (possibly incomplete) line in the buffer
        buffer = lines.pop();

        for (const line of lines) {
          // Remove optional trailing carriage return (\r) without stripping spaces
          const rawLine = line.endsWith('\r') ? line.slice(0, -1) : line;
          if (!rawLine.startsWith('data:')) continue;

          // Extract token using substring(6) to strictly preserve all leading and trailing spaces
          // Do NOT run .trim() on the token payload
          const payload_raw = rawLine.startsWith('data: ')
            ? rawLine.substring(6)
            : rawLine.substring(5);

          // -- End of stream sentinel --
          if (payload_raw === '[DONE]') {
            // Finalise the live bubble with proper markdown rendering
            if (liveBubbleEl && rawAccumulated.trim()) {
              finaliseBubble(liveBubbleEl, rawAccumulated.trim());
              // Add to local history for multi-turn context
              conversationHistory.push({ role: 'assistant', content: rawAccumulated.trim() });
            }
            break;
          }

          // -- Server metadata event (conversation_id sync) --
          if (payload_raw.startsWith('[META]')) {
            const serverId = payload_raw.slice(6).trim();
            if (serverId) currentConversationId = serverId;
            continue;
          }

          // -- Error event from backend --
          if (payload_raw.startsWith('[ERROR]')) {
            streamErrored = true;
            removeLoadingBubble(loadingBubble);
            if (liveBubbleRow) liveBubbleRow.remove();
            appendErrorMessage(
              payload_raw.slice(7).trim() || 'Stream error from AI service',
              'Check that FastAPI and Ollama are running.'
            );
            break;
          }

          // -- Normal token chunk --
          // SSE newlines were escaped as \\n by Python; restore them
          const token = payload_raw.replace(/\\n/g, '\n');

          if (!liveBubbleRow) {
            // First token: swap loading indicator for a live text bubble
            removeLoadingBubble(loadingBubble);
            const created = createLiveAssistantBubble();
            liveBubbleRow = created.row;
            liveBubbleEl  = created.bubble;
          }

          rawAccumulated += token;
          // Stream raw text into the bubble (escaped, no markdown yet — too noisy mid-stream)
          appendTokenToLiveBubble(liveBubbleEl, token);
          scrollToBottom();
        }
      }

    } catch (err) {
      if (err.name === 'AbortError') {
        // User started a new chat — silently discard
      } else {
        console.error('BodhNet stream error:', err);
        removeLoadingBubble(loadingBubble);
        if (liveBubbleRow) liveBubbleRow.remove();
        if (!streamErrored) {
          appendErrorMessage(
            'Unable to reach BodhNet Java Web Server (/chat)',
            'Check network status and verify Tomcat 11 is running.'
          );
        }
      }
    } finally {
      isGenerating          = false;
      activeAbortController = null;
      sendBtn.disabled      = chatInput.value.trim().length === 0;
      scrollToBottom();
      chatInput.focus();
    }
  }

  // =========================================================================
  // Live bubble helpers
  // =========================================================================

  /**
   * Creates an empty assistant bubble row ready to receive streaming tokens.
   * Returns references to the row and the bubble content element.
   */
  function createLiveAssistantBubble() {
    const row = document.createElement('div');
    row.className = 'message-row assistant';

    const avatar = document.createElement('div');
    avatar.className = 'avatar bot-avatar';
    avatar.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2a10 10 0 0 1 10 10c0 5.523-4.477 10-10 10S2 17.523 2 12A10 10 0 0 1 12 2z"/><path d="M8 12h8"/><path d="M12 8v8"/></svg>`;

    const bubble = document.createElement('div');
    bubble.className = 'bubble streaming-bubble';
    bubble.setAttribute('aria-live', 'polite');

    // Cursor blink element — removed when stream ends
    const cursor = document.createElement('span');
    cursor.className = 'stream-cursor';
    cursor.textContent = '▌';
    bubble.appendChild(cursor);

    row.appendChild(avatar);
    row.appendChild(bubble);
    chatMessages.appendChild(row);

    return { row, bubble };
  }

  /**
   * Appends a raw token to the streaming bubble as escaped plain text.
   * Markdown rendering is deferred until the stream completes (finaliseBubble).
   */
  function appendTokenToLiveBubble(bubble, token) {
    if (!bubble) return;
    const cursor = bubble.querySelector('.stream-cursor');
    // Insert text node exactly as received to preserve whitespace
    const textNode = document.createTextNode(token);
    if (cursor) {
      bubble.insertBefore(textNode, cursor);
    } else {
      bubble.appendChild(textNode);
    }
  }

  /**
   * Called when [DONE] arrives — replaces the raw streamed text with the
   * fully formatted markdown HTML, removes the blinking cursor, and adds meta.
   */
  function finaliseBubble(bubble, fullText) {
    bubble.classList.remove('streaming-bubble');
    bubble.innerHTML = formatAssistantMarkdown(fullText);
    attachCodeCopyHandlers(bubble);

    const meta = document.createElement('div');
    meta.className = 'message-meta';
    const timeStr = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
    meta.innerHTML = `<span>BodhNet</span> • <span>qwen3:4b</span> • <span>${timeStr}</span>`;
    bubble.appendChild(meta);
  }

  // =========================================================================
  // Static / full-block message rendering (user messages, errors)
  // =========================================================================

  function appendMessage(role, content, modelName = null) {
    const row = document.createElement('div');
    row.className = `message-row ${role}`;

    const avatar = document.createElement('div');
    avatar.className = `avatar ${role === 'user' ? 'user-avatar' : 'bot-avatar'}`;

    if (role === 'user') {
      avatar.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M19 21v-2a4 4 0 0 0-4-4H9a4 4 0 0 0-4 4v2"/><circle cx="12" cy="7" r="4"/></svg>`;
    } else {
      avatar.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2a10 10 0 0 1 10 10c0 5.523-4.477 10-10 10S2 17.523 2 12A10 10 0 0 1 12 2z"/><path d="M8 12h8"/><path d="M12 8v8"/></svg>`;
    }

    const bubble = document.createElement('div');
    bubble.className = 'bubble';

    if (role === 'user') {
      bubble.textContent = content;
    } else {
      bubble.innerHTML = formatAssistantMarkdown(content);
      attachCodeCopyHandlers(bubble);
      const meta = document.createElement('div');
      meta.className = 'message-meta';
      const timeStr = new Date().toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' });
      meta.innerHTML = `<span>BodhNet</span> • <span>${escapeHtml(modelName || 'qwen3:4b')}</span> • <span>${timeStr}</span>`;
      bubble.appendChild(meta);
    }

    if (role === 'user') { row.appendChild(bubble); row.appendChild(avatar); }
    else                 { row.appendChild(avatar);  row.appendChild(bubble); }

    chatMessages.appendChild(row);
    scrollToBottom();
  }

  function appendLoadingBubble() {
    const row = document.createElement('div');
    row.className = 'message-row assistant typing-row';

    const avatar = document.createElement('div');
    avatar.className = 'avatar bot-avatar';
    avatar.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><path d="M12 2a10 10 0 0 1 10 10c0 5.523-4.477 10-10 10S2 17.523 2 12A10 10 0 0 1 12 2z"/><path d="M8 12h8"/><path d="M12 8v8"/></svg>`;

    const bubble = document.createElement('div');
    bubble.className = 'bubble typing-bubble';
    bubble.innerHTML = `
      <div class="typing-dots">
        <span class="typing-dot"></span>
        <span class="typing-dot"></span>
        <span class="typing-dot"></span>
      </div>
      <span class="typing-text">BodhNet is reasoning...</span>
    `;

    row.appendChild(avatar);
    row.appendChild(bubble);
    chatMessages.appendChild(row);
    return row;
  }

  function removeLoadingBubble(el) {
    if (el && el.parentNode) el.parentNode.removeChild(el);
  }

  function appendErrorMessage(errorTitle, details) {
    const row = document.createElement('div');
    row.className = 'message-row assistant';

    const avatar = document.createElement('div');
    avatar.className = 'avatar bot-avatar';
    avatar.innerHTML = `<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><circle cx="12" cy="12" r="10"/><line x1="12" y1="8" x2="12" y2="12"/><line x1="12" y1="16" x2="12.01" y2="16"/></svg>`;

    const bubble = document.createElement('div');
    bubble.className = 'bubble error-bubble';
    bubble.innerHTML = `
      <strong>Error:</strong> ${escapeHtml(errorTitle)}
      ${details ? `<div style="margin-top:6px;font-size:0.8rem;opacity:0.85;">${escapeHtml(details)}</div>` : ''}
    `;

    row.appendChild(avatar);
    row.appendChild(bubble);
    chatMessages.appendChild(row);
    scrollToBottom();
  }

  // =========================================================================
  // Markdown formatter (post-stream, full text only)
  // =========================================================================
  function formatAssistantMarkdown(text) {
    if (!text) return '';

    const codeBlocks = [];
    let processed = text.replace(/```([a-zA-Z0-9_\-\+]*)\n([\s\S]*?)```/g, (match, lang, code) => {
      const id = `__CODE_BLOCK_${codeBlocks.length}__`;
      codeBlocks.push({ lang: lang || 'text', code: code.trim() });
      return id;
    });

    processed = escapeHtml(processed);
    processed = processed.replace(/\*\*(.*?)\*\*/g, '<strong>$1</strong>');
    processed = processed.replace(/`([^`]+)`/g, '<code>$1</code>');
    processed = processed.replace(/(?:^|\n)\s*[-*]\s+(.+)/g, '\n<li>$1</li>');
    processed = processed.replace(/(<li>.*<\/li>)/gs, '<ul>$1</ul>');

    const paragraphs = processed.split(/\n\n+/);
    let html = paragraphs.map(p => {
      p = p.trim();
      if (!p) return '';
      if (p.startsWith('<ul>') || p.startsWith('__CODE_BLOCK_')) return p;
      return `<p>${p.replace(/\n/g, '<br>')}</p>`;
    }).join('');

    codeBlocks.forEach((block, idx) => {
      const codeHtml = `
        <div class="code-wrapper">
          <div class="code-header">
            <span>${escapeHtml(block.lang)}</span>
            <button class="btn-copy-code" type="button">
              <svg width="12" height="12" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>
              Copy
            </button>
          </div>
          <pre><code class="language-${escapeHtml(block.lang)}">${escapeHtml(block.code)}</code></pre>
        </div>
      `;
      html = html.replace(`__CODE_BLOCK_${idx}__`, codeHtml);
    });

    return html;
  }

  function attachCodeCopyHandlers(container) {
    container.querySelectorAll('.btn-copy-code').forEach(btn => {
      btn.addEventListener('click', () => {
        const pre = btn.closest('.code-wrapper').querySelector('pre code');
        if (pre) {
          navigator.clipboard.writeText(pre.textContent).then(() => {
            const orig = btn.innerHTML;
            btn.innerHTML = '✓ Copied!';
            setTimeout(() => { btn.innerHTML = orig; }, 2000);
          }).catch(err => console.error('Copy failed:', err));
        }
      });
    });
  }

  function escapeHtml(str) {
    if (!str) return '';
    return str
      .replace(/&/g, '&amp;')
      .replace(/</g, '&lt;')
      .replace(/>/g, '&gt;')
      .replace(/"/g, '&quot;')
      .replace(/'/g, '&#039;');
  }

  function scrollToBottom() {
    const el = document.getElementById('chatScrollArea');
    if (el) el.scrollTop = el.scrollHeight;
  }
});
