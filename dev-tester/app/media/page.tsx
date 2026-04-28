"use client";

import { useRustZap } from "@/components/rustzap-provider";

function formatBytes(value: number) {
  if (value < 1024) {
    return `${value} B`;
  }
  if (value < 1024 * 1024) {
    return `${Math.round(value / 1024)} KB`;
  }
  return `${(value / 1024 / 1024).toFixed(1)} MB`;
}

export default function MediaPage() {
  const { state, actions } = useRustZap();
  const activeConversation = state.conversations.find((item) => item.id === state.selectedConversationId);

  return (
    <section className="page">
      <div className="page-header">
        <div>
          <h1>Media</h1>
          <p>Inspect temporary media, saved objects, and the send/upload simulation path.</p>
        </div>
        {activeConversation ? (
          <div className="toolbar">
            <button type="button" onClick={() => actions.simulateInboundMedia(activeConversation.id, "image")}>
              Sim Image
            </button>
            <button type="button" onClick={() => actions.simulateInboundMedia(activeConversation.id, "document")}>
              Sim Document
            </button>
            <button type="button" onClick={() => actions.simulateInboundAudio(activeConversation.id)}>
              Sim Audio
            </button>
          </div>
        ) : null}
      </div>

      <section className="surface">
        <h2>Objects</h2>
        {state.media.length === 0 ? (
          <p className="empty-state">No media objects in the tester state.</p>
        ) : (
          <div className="table-list media-table">
            {state.media.map((media) => (
              <div key={media.id}>
                <span>
                  <b>{media.originalFilename}</b>
                  <small>{media.id} · {media.mimeType}</small>
                </span>
                <span>{media.mediaType}</span>
                <span>{formatBytes(media.sizeBytes)}</span>
                <span>{media.storageStatus}</span>
                <button
                  type="button"
                  disabled={media.storageStatus === "permanent"}
                  onClick={() => actions.saveMedia(media.id)}
                >
                  Save
                </button>
              </div>
            ))}
          </div>
        )}
      </section>

      <section className="split-grid">
        <article className="surface">
          <h2>Active Conversation</h2>
          {activeConversation ? (
            <dl className="details-list">
              <div>
                <dt>Title</dt>
                <dd>{activeConversation.title}</dd>
              </div>
              <div>
                <dt>ID</dt>
                <dd>{activeConversation.id}</dd>
              </div>
              <div>
                <dt>Seq</dt>
                <dd>{activeConversation.lastSeq}</dd>
              </div>
            </dl>
          ) : (
            <p className="empty-state">Select a conversation in Chat to target media simulations.</p>
          )}
        </article>
        <article className="surface">
          <h2>Transcripts</h2>
          {state.transcripts.length === 0 ? (
            <p className="empty-state">Audio transcripts appear here after simulation.</p>
          ) : (
            <div className="compact-list readonly">
              {state.transcripts.map((transcript) => (
                <div key={transcript.id}>
                  <span>
                    <b>{transcript.text}</b>
                    <small>{transcript.provider} · {transcript.model} · {transcript.status}</small>
                  </span>
                </div>
              ))}
            </div>
          )}
        </article>
      </section>
    </section>
  );
}
