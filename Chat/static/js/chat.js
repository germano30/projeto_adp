// Chat Application JavaScript

class ChatApp {
    constructor() {
        this.messagesContainer = document.getElementById('messagesContainer');
        this.messageInput = document.getElementById('messageInput');
        this.sendButton = document.getElementById('sendButton');
        this.typingIndicator = null;

        this.init();
    }

    init() {
        // Event listeners
        this.sendButton.addEventListener('click', () => this.sendMessage());
        this.messageInput.addEventListener('keypress', (e) => {
            if (e.key === 'Enter' && !e.shiftKey) {
                e.preventDefault();
                this.sendMessage();
            }
        });

        // Focus on input
        this.messageInput.focus();
    }

    async sendMessage() {
        const message = this.messageInput.value.trim();

        if (!message) {
            return;
        }

        // Disable input while processing
        this.setInputState(false);

        // Add user message to chat
        this.addMessage(message, 'user');

        // Clear input
        this.messageInput.value = '';

        // Show loading indicator
        this.showLoading(true);

        try {
            // Send message to server
            const response = await fetch('/api/chat', {
                method: 'POST',
                headers: {
                    'Content-Type': 'application/json',
                },
                body: JSON.stringify({ message: message })
            });

            const data = await response.json();

            // Hide loading indicator
            this.showLoading(false);

            if (data.success) {
                // Add bot response
                this.addMessage(data.response, 'bot', data.route);
            } else {
                // Add error message
                this.addMessage(
                    'Sorry, an error occurred while processing your message. Please try again.',
                    'bot'
                );
                console.error('Error:', data.error);
            }
        } catch (error) {
            this.showLoading(false);
            this.addMessage(
                'Sorry, unable to connect to the server. Please check your connection and try again.',
                'bot'
            );
            console.error('Network error:', error);
        } finally {
            // Re-enable input
            this.setInputState(true);
            this.messageInput.focus();
        }
    }

    addMessage(text, sender, route = null) {
        const messageDiv = document.createElement('div');
        messageDiv.className = `message ${sender}-message`;

        const avatarSrc = sender === 'bot'
            ? '/static/atendente.png'
            : '/static/usuario.png';

        const time = this.getCurrentTime();

        // Add route badge if available
        let routeBadge = '';
        if (route) {
            const routeLabels = {
                'sql': 'SQL Query',
                'lightrag': 'Knowledge Base',
                'hybrid': 'Hybrid Query'
            };
            const routeLabel = routeLabels[route] || route;
            routeBadge = `<span class="route-badge ${route}">${routeLabel}</span>`;
        }

        messageDiv.innerHTML = `
            <img src="${avatarSrc}" alt="${sender}" class="message-avatar">
            <div class="message-content">
                ${routeBadge}
                <div class="message-bubble">
                    <p>${this.formatMessage(text)}</p>
                    <span class="message-time">${time}</span>
                </div>
            </div>
        `;

        this.messagesContainer.appendChild(messageDiv);
        this.scrollToBottom();
    }

    formatMessage(text) {
        // Remove Markdown formatting first
        let cleanText = text;

        // Remove bold (**text** or __text__)
        cleanText = cleanText.replace(/\*\*(.+?)\*\*/g, '$1');
        cleanText = cleanText.replace(/__(.+?)__/g, '$1');

        // Remove italic (*text* or _text_)
        cleanText = cleanText.replace(/\*(.+?)\*/g, '$1');
        cleanText = cleanText.replace(/_(.+?)_/g, '$1');

        // Remove headers (# ## ###)
        cleanText = cleanText.replace(/^#{1,6}\s+/gm, '');

        // Remove list markers (* - +)
        cleanText = cleanText.replace(/^[\*\-\+]\s+/gm, '• ');

        // Remove code blocks (```)
        cleanText = cleanText.replace(/```[\s\S]*?```/g, '');
        cleanText = cleanText.replace(/`(.+?)`/g, '$1');

        // Escape HTML
        const div = document.createElement('div');
        div.textContent = cleanText;
        let html = div.innerHTML;

        // Convert newlines to <br>
        html = html.replace(/\n/g, '<br>');

        // Make URLs clickable
        html = html.replace(
            /(https?:\/\/[^\s]+)/g,
            '<a href="$1" target="_blank" style="color: #5a67d8; text-decoration: underline;">$1</a>'
        );

        return html;
    }

    getCurrentTime() {
        const now = new Date();
        const hours = now.getHours().toString().padStart(2, '0');
        const minutes = now.getMinutes().toString().padStart(2, '0');
        return `${hours}:${minutes}`;
    }

    showLoading(show) {
        if (show) {
            // Create typing indicator element
            this.typingIndicator = document.createElement('div');
            this.typingIndicator.className = 'loading-indicator visible';
            this.typingIndicator.innerHTML = `
                <img src="/static/atendente.png" alt="Bot" class="message-avatar">
                <div class="typing-bubble">
                    <span></span>
                    <span></span>
                    <span></span>
                </div>
            `;
            this.messagesContainer.appendChild(this.typingIndicator);
            this.scrollToBottom();
        } else {
            // Remove typing indicator
            if (this.typingIndicator) {
                this.typingIndicator.remove();
                this.typingIndicator = null;
            }
        }
    }

    setInputState(enabled) {
        this.messageInput.disabled = !enabled;
        this.sendButton.disabled = !enabled;
    }

    scrollToBottom() {
        setTimeout(() => {
            this.messagesContainer.scrollTop = this.messagesContainer.scrollHeight;
        }, 100);
    }
}

// Initialize the chat app when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    new ChatApp();
});
