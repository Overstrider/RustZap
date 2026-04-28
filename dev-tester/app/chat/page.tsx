"use client";

import { FormEvent, useState } from "react";
import { useRustZap } from "@/components/rustzap-provider";

function messageTime(value: string) {
  return new Intl.DateTimeFormat("pt-BR", {
    hour: "2-digit",
    minute: "2-digit"
  }).format(new Date(value));
}

export default function ChatPage() {
  const { state, actions, activeConversation, activeMessages } = useRustZap();
  const [outboundText, setOutboundText] = useState("");
  const [inboundText, setInboundText] = useState("Oi, quero falar com um corretor.");

  async function submitOutbound(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!activeConversation) {
      return;
    }
    await actions.sendMessage(activeConversation.id, outboundText);
    setOutboundText("");
  }

  async function submitInbound(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!activeConversation) {
      return;
    }
    await actions.simulateInboundText(activeConversation.id, inboundText);
    setInboundText("");
  }

  return (
    <section className="page flush">
      <div className="workspace-grid">
        <aside className="surface list-surface">
          <h2>Conversations</h2>
          <div className="compact-list">
            {state.conversations.map((conversation) => (
              <button
                className={conversation.id === state.selectedConversationId ? "active" : undefined}
                key={conversation.id}
                type="button"
                onClick={() => actions.selectConversation(conversation.id)}
              >
                <span>
                  <b>{conversation.title}</b>
                  <small>{conversation.type} · seq {conversation.lastSeq}</small>
                </span>
                {conversation.unreadCount > 0 ? <strong>{conversation.unreadCount}</strong> : null}
              </button>
            ))}
          </div>
        </aside>

        <section className="surface chat-surface">
          <div className="section-header">
            <div>
              <h1>{activeConversation?.title ?? "No conversation"}</h1>
              <p>{activeConversation?.id ?? "Select a conversation to start testing."}</p>
            </div>
            {activeConversation ? (
              <button type="button" onClick={() => actions.markConversationRead(activeConversation.id)}>
                Mark Read
              </button>
            ) : null}
          </div>

          <div className="message-list">
            {activeMessages.map((message) => (
              <article className={`tester-message ${message.direction}`} key={message.id}>
                <div>
                  <strong>{message.senderName}</strong>
                  <small>
                    #{message.conversationSeq} · {message.status} · {messageTime(message.createdAt)}
                  </small>
                </div>
                <p>{message.text}</p>
                {message.transcriptText ? <blockquote>{message.transcriptText}</blockquote> : null}
                <div className="message-actions">
                  <button type="button" onClick={() => actions.reactToMessage(message.id, "👍")}>
                    {message.reaction ? `Reacted ${message.reaction}` : "React"}
                  </button>
                  <button type="button" onClick={() => actions.toggleStar(message.id)}>
                    {message.isStarred ? "Unstar" : "Star"}
                  </button>
                  <button type="button" onClick={() => actions.togglePin(message.id)}>
                    {message.isPinned ? "Unpin" : "Pin"}
                  </button>
                </div>
              </article>
            ))}
          </div>

          {activeConversation ? (
            <div className="chat-controls">
              <form onSubmit={submitOutbound}>
                <input
                  aria-label="Outbound message"
                  value={outboundText}
                  onChange={(event) => setOutboundText(event.target.value)}
                  placeholder="Send outbound message"
                />
                <button type="submit">Send</button>
              </form>
              <form onSubmit={submitInbound}>
                <input
                  aria-label="Inbound simulation"
                  value={inboundText}
                  onChange={(event) => setInboundText(event.target.value)}
                  placeholder="Simulate inbound message"
                />
                <button type="submit">Receive</button>
              </form>
              <div className="toolbar wrap">
                <button type="button" onClick={() => actions.simulateInboundAudio(activeConversation.id)}>
                  Audio
                </button>
                <button type="button" onClick={() => actions.simulateInboundMedia(activeConversation.id, "image")}>
                  Image
                </button>
                <button type="button" onClick={() => actions.simulateInboundMedia(activeConversation.id, "document")}>
                  Document
                </button>
              </div>
            </div>
          ) : null}
        </section>
      </div>
    </section>
  );
}
