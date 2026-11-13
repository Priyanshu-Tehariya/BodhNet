<%@ page language="java" contentType="text/html; charset=UTF-8" pageEncoding="UTF-8"%>
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>BodhNet - Automated Knowledge Management and Query System</title>
    <!-- BodhNet Chat Stylesheet -->
    <link rel="stylesheet" href="${pageContext.request.contextPath}/assets/css/chat.css">
</head>
<body>
    <div class="app-layout">
        <!-- Sidebar Navigation -->
        <aside class="sidebar" id="sidebar">
            <div class="sidebar-header">
                <div class="brand-badge">
                    <div class="brand-logo">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/>
                        </svg>
                    </div>
                    <div class="brand-text">
                        <h1>BodhNet</h1>
                        <p>Knowledge Assistant</p>
                    </div>
                </div>
                <button class="btn-sidebar-close" id="sidebarClose" title="Close Sidebar" aria-label="Close sidebar">
                    <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <line x1="18" y1="6" x2="6" y2="18"></line>
                        <line x1="6" y1="6" x2="18" y2="18"></line>
                    </svg>
                </button>
            </div>

            <!-- New Chat Action -->
            <div class="sidebar-actions">
                <button class="btn-new-chat" id="newChatBtn" type="button">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                        <line x1="12" y1="5" x2="12" y2="19"></line>
                        <line x1="5" y1="12" x2="19" y2="12"></line>
                    </svg>
                    <span>New Chat</span>
                </button>
            </div>

            <!-- Chat History — populated dynamically by chat.js -->
            <div class="sidebar-history" id="sidebarHistory">
                <!-- Session items injected here by JavaScript -->
            </div>

        </aside>

        <!-- Main Chat Area -->
        <main class="chat-main">
            <!-- Top Chat Header -->
            <header class="chat-header">
                <div class="header-left">
                    <button class="btn-sidebar-toggle" id="sidebarToggle" title="Open Sidebar" aria-label="Open sidebar">
                        <svg width="22" height="22" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <line x1="3" y1="12" x2="21" y2="12"></line>
                            <line x1="3" y1="6" x2="21" y2="6"></line>
                            <line x1="3" y1="18" x2="21" y2="18"></line>
                        </svg>
                    </button>
                    <div class="header-title-container">
                        <div class="chat-title">BodhNet Assistant</div>
                        <div class="chat-subtitle">Automated Knowledge Management and Query System</div>
                    </div>
                </div>

                <div class="header-right">
                    <button class="btn-action-ghost" id="clearChatBtn" type="button" title="Clear current chat">
                        <svg width="15" height="15" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                            <polyline points="3 6 5 6 21 6"></polyline>
                            <path d="M19 6v14a2 2 0 0 1-2 2H7a2 2 0 0 1-2-2V6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2"></path>
                        </svg>
                        <span>Clear</span>
                    </button>
                </div>
            </header>

            <!-- Chat Message Scroll Container -->
            <div class="chat-scroll-area" id="chatScrollArea">
                <div class="chat-messages-container" id="chatMessages">
                    <!-- Initial Welcome Screen -->
                    <div class="welcome-screen" id="welcomeScreen">
                        <div class="welcome-logo">
                            <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                                <path d="M12 2L2 7l10 5 10-5-10-5zM2 17l10 5 10-5M2 12l10 5 10-5"/>
                            </svg>
                        </div>
                        <h2 class="welcome-title">Welcome to BodhNet</h2>
                        <p class="welcome-subtitle">
                            Your local AI knowledge assistant powered by Apple Silicon local inference.<br>
                            Ask broad questions, explore concepts, or solve complex problems.
                        </p>

                        <div class="prompt-chips-grid">
                            <div class="prompt-chip" data-prompt="Explain how Apple Silicon unified memory accelerates local LLM inference.">
                                <span class="chip-title">Apple Silicon & LLMs</span>
                                <span class="chip-desc">How unified memory architecture accelerates local models</span>
                            </div>
                            <div class="prompt-chip" data-prompt="Explain recursion in Python with a clear code example and time complexity.">
                                <span class="chip-title">Python Recursion</span>
                                <span class="chip-desc">Learn recursive logic with clear algorithmic examples</span>
                            </div>
                            <div class="prompt-chip" data-prompt="How do Jakarta Servlets and Tomcat 11 manage asynchronous HTTP requests?">
                                <span class="chip-title">Jakarta Servlets 6.0</span>
                                <span class="chip-desc">Explore Tomcat 11 architecture and request handling</span>
                            </div>
                            <div class="prompt-chip" data-prompt="What are the best practices for structuring Oracle PL/SQL packages?">
                                <span class="chip-title">Oracle PL/SQL Design</span>
                                <span class="chip-desc">Best practices for procedures, packages, and transactions</span>
                            </div>
                        </div>
                    </div>
                </div>
            </div>

            <!-- Bottom Composer Area -->
            <div class="composer-wrapper">
                <div class="composer-box">
                    <textarea 
                        id="chatInput" 
                        class="chat-input" 
                        placeholder="Ask BodhNet anything... (Press Enter to send, Shift+Enter for newline)"
                        rows="1"
                        autocomplete="off"></textarea>
                    <button id="sendBtn" class="btn-send" type="button" disabled title="Send message" aria-label="Send message">
                        <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5">
                            <line x1="22" y1="2" x2="11" y2="13"></line>
                            <polygon points="22 2 15 22 11 13 2 9 22 2"></polygon>
                        </svg>
                    </button>
                </div>
                <div class="composer-disclaimer">
                    BodhNet runs on local hardware via Ollama. Responses are generated locally and should be verified for accuracy.
                </div>
            </div>
        </main>
    </div>

    <!-- Pass Servlet Context Path to client script -->
    <script>
        const CONTEXT_PATH = "${pageContext.request.contextPath}";
    </script>
    <!-- Chat Client Script -->
    <script src="${pageContext.request.contextPath}/assets/js/chat.js?v=6"></script>
</body>
</html>

