"use client";

import {
  createContext,
  type ReactNode,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useRef,
  useState
} from "react";
import {
  CHANNEL_ID,
  COMPANY_ID,
  PROJECT_ID,
  createInitialState,
  isoFromNow,
  makeEvent,
  makeQrCodeText
} from "@/lib/mock-data";
import {
  DEFAULT_CHANNEL_ID,
  devPath,
  idempotencyKey,
  initialMode,
  requestRustZap,
  rustZapWsUrl,
  tenantPath
} from "@/lib/rustzap-api";
import type {
  ApiMode,
  CapabilityKey,
  GroupMember,
  MediaType,
  Message,
  MessageStatus,
  RustZapEvent,
  RustZapHttpResponse,
  RustZapState
} from "@/lib/types";

type Actions = {
  setMode: (mode: ApiMode) => void;
  ensureDevTenant: () => Promise<void>;
  refreshReal: () => Promise<void>;
  createChannel: () => Promise<void>;
  connectChannel: () => Promise<void>;
  fetchQr: () => Promise<void>;
  rotateQr: () => Promise<void>;
  cycleChannelStatus: () => void;
  selectConversation: (conversationId: string) => void;
  sendMessage: (conversationId: string, text: string) => Promise<void>;
  simulateInboundText: (conversationId: string, text: string) => Promise<void>;
  simulateInboundAudio: (conversationId: string) => Promise<void>;
  simulateInboundMedia: (conversationId: string, mediaType: Exclude<MediaType, "audio">) => Promise<void>;
  reactToMessage: (messageId: string, emoji: string) => Promise<void>;
  togglePin: (messageId: string) => Promise<void>;
  toggleStar: (messageId: string) => Promise<void>;
  markConversationRead: (conversationId: string) => Promise<void>;
  saveMedia: (mediaId: string) => Promise<void>;
  selectGroup: (groupId: string) => void;
  simulateGroupEvent: (groupId: string, kind: "join" | "leave") => Promise<void>;
  exitGroup: (groupId: string) => Promise<void>;
  connectWebSocket: () => void;
  disconnectWebSocket: () => void;
};

type RustZapContextValue = {
  state: RustZapState;
  actions: Actions;
  activeConversation: RustZapState["conversations"][number] | undefined;
  activeMessages: Message[];
  averageLatencyMs: number;
};

const RustZapContext = createContext<RustZapContextValue | null>(null);

function nextEventSeq(events: RustZapEvent[]) {
  return (events[0]?.eventSeq ?? 1000) + 1;
}

function updateConversationForMessage(state: RustZapState, conversationId: string, seq: number, unreadDelta = 1) {
  return state.conversations.map((conversation) =>
    conversation.id === conversationId
      ? {
          ...conversation,
          lastSeq: Math.max(conversation.lastSeq, seq),
          lastMessageAt: new Date().toISOString(),
          unreadCount: Math.max(0, conversation.unreadCount + unreadDelta)
        }
      : conversation
  );
}

function coerceObject(body: unknown) {
  return body && typeof body === "object" && !Array.isArray(body) ? (body as Record<string, unknown>) : {};
}

function coerceCapabilities(body: unknown, current: Record<CapabilityKey, boolean>) {
  const root = coerceObject(body);
  const features = coerceObject(root.features);
  return (Object.keys(current) as CapabilityKey[]).reduce(
    (next, key) => {
      const feature = coerceObject(features[key]);
      next[key] = typeof feature.supported === "boolean" ? feature.supported : current[key];
      return next;
    },
    { ...current }
  );
}

export function RustZapProvider({ children }: { children: ReactNode }) {
  const [state, setState] = useState<RustZapState>(() => ({
    ...createInitialState(),
    mode: initialMode()
  }));
  const wsRef = useRef<WebSocket | null>(null);
  const reconnectTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const mockWsTimerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const wsWantedRef = useRef(false);
  const connectWebSocketRef = useRef<() => void>(() => undefined);

  const addEvent = useCallback((type: string, payload: Record<string, unknown>) => {
    setState((current) => {
      const event = makeEvent(type, nextEventSeq(current.events), {
        project_id: PROJECT_ID,
        company_id: COMPANY_ID,
        ...payload
      });
      return {
        ...current,
        events: [event, ...current.events].slice(0, 200)
      };
    });
  }, []);

  const runReal = useCallback(
    async (
      label: string,
      method: string,
      path: string,
      body?: unknown,
      key?: string
    ): Promise<RustZapHttpResponse | undefined> => {
      setState((current) => ({ ...current, error: undefined }));
      try {
        const response = await requestRustZap(method, path, body, key);
        setState((current) => ({
          ...current,
          lastRealResponse: {
            label,
            status: response.status,
            body: response.body
          },
          error: response.status >= 400 ? `${label} returned HTTP ${response.status}` : undefined
        }));
        addEvent(`http.${label}`, {
          method,
          path,
          status: response.status,
          body: response.body as Record<string, unknown>
        });
        return response;
      } catch (error) {
        const message = error instanceof Error ? error.message : "Real mode request failed";
        setState((current) => ({ ...current, error: message }));
        addEvent("http.error", {
          label,
          method,
          path,
          message
        });
        return undefined;
      }
    },
    [addEvent]
  );

  useEffect(() => {
    if (state.mode !== "mock") {
      return;
    }
    const timer = setInterval(() => {
      setState((current) => {
        if (!["waiting_qr", "connecting"].includes(current.channel.status)) {
          return current;
        }
        if (Date.parse(current.channel.qrExpiresAt) > Date.now()) {
          return current;
        }
        const qrGeneration = current.channel.qrGeneration + 1;
        return {
          ...current,
          channel: {
            ...current.channel,
            qrGeneration,
            qrCodeText: makeQrCodeText(qrGeneration),
            qrExpiresAt: isoFromNow(current.channel.qrRefreshSeconds * 1000)
          }
        };
      });
    }, 1000);

    return () => clearInterval(timer);
  }, [state.mode]);

  const setMode = useCallback(
    (mode: ApiMode) => {
      setState((current) => ({
        ...current,
        mode,
        error: undefined,
        ws: {
          status: "idle",
          attempts: 0
        }
      }));
      addEvent("tester.mode.changed", { mode });
    },
    [addEvent]
  );

  const ensureDevTenant = useCallback(async () => {
    if (state.mode === "real") {
      await runReal("project.create", "POST", "/v1/projects", {
        id: PROJECT_ID,
        name: "TETOZ Dev"
      });
      await runReal("company.create", "POST", `/v1/projects/${PROJECT_ID}/companies`, {
        id: COMPANY_ID,
        external_company_id: COMPANY_ID,
        name: "Company Dev"
      });
    }
    setState((current) => ({ ...current, devTenantReady: true }));
    addEvent("dev.tenant.ready", {
      project_id: PROJECT_ID,
      company_id: COMPANY_ID
    });
  }, [addEvent, runReal, state.mode]);

  const refreshReal = useCallback(async () => {
    if (state.mode !== "real") {
      addEvent("tester.refresh.mock", { status: "mock_state_current" });
      return;
    }

    await runReal("health", "GET", "/health");
    await runReal("ready", "GET", "/ready");
    const channel = await runReal(
      "channel.get",
      "GET",
      tenantPath(`/channels/whatsapp/accounts/${DEFAULT_CHANNEL_ID}`)
    );
    const body = coerceObject(channel?.body);
    if (typeof body.status === "string") {
      setState((current) => ({
        ...current,
        channel: {
          ...current.channel,
          status: body.status as RustZapState["channel"]["status"]
        }
      }));
    }
    const capabilities = await runReal(
      "channel.capabilities",
      "GET",
      tenantPath(`/channels/whatsapp/accounts/${DEFAULT_CHANNEL_ID}/capabilities`)
    );
    setState((current) => ({
      ...current,
      channel: {
        ...current.channel,
        capabilities: coerceCapabilities(capabilities?.body, current.channel.capabilities)
      }
    }));
    await runReal("conversations.list", "GET", tenantPath("/conversations"));
    await runReal("groups.list", "GET", tenantPath("/groups"));
  }, [addEvent, runReal, state.mode]);

  const createChannel = useCallback(async () => {
    if (state.mode === "real") {
      await runReal("channel.create", "POST", tenantPath("/channels/whatsapp/accounts"), {
        id: DEFAULT_CHANNEL_ID,
        label: "WhatsApp Dev",
        phone_e164: "+5511999000000"
      });
    }
    setState((current) => ({
      ...current,
      channel: {
        ...current.channel,
        id: CHANNEL_ID,
        status: "disconnected"
      }
    }));
    addEvent("channel.created", { channel_id: CHANNEL_ID });
  }, [addEvent, runReal, state.mode]);

  const fetchQr = useCallback(async () => {
    if (state.mode === "real") {
      const response = await runReal(
        "channel.qr",
        "GET",
        tenantPath(`/channels/whatsapp/accounts/${DEFAULT_CHANNEL_ID}/qr`)
      );
      const body = coerceObject(response?.body);
      setState((current) => ({
        ...current,
        channel: {
          ...current.channel,
          qrCodeText:
            typeof body.qr_code_text === "string" ? body.qr_code_text : current.channel.qrCodeText,
          qrExpiresAt:
            typeof body.expires_at === "string" ? body.expires_at : current.channel.qrExpiresAt
        }
      }));
      return;
    }
    addEvent("channel.qr", {
      channel_id: CHANNEL_ID,
      expires_at: state.channel.qrExpiresAt
    });
  }, [addEvent, runReal, state.channel.qrExpiresAt, state.mode]);

  const connectChannel = useCallback(async () => {
    if (state.mode === "real") {
      await runReal(
        "channel.connect",
        "POST",
        tenantPath(`/channels/whatsapp/accounts/${DEFAULT_CHANNEL_ID}/connect`)
      );
      await fetchQr();
    }
    setState((current) => ({
      ...current,
      channel: {
        ...current.channel,
        status: "waiting_qr",
        qrExpiresAt: isoFromNow(current.channel.qrRefreshSeconds * 1000)
      }
    }));
    addEvent("channel.status", {
      channel_id: CHANNEL_ID,
      status: "waiting_qr"
    });
  }, [addEvent, fetchQr, runReal, state.mode]);

  const rotateQr = useCallback(async () => {
    if (state.mode === "real") {
      await runReal("dev.qr_rotate", "POST", devPath("/simulate/qr-rotate"));
      await fetchQr();
      return;
    }
    setState((current) => {
      const qrGeneration = current.channel.qrGeneration + 1;
      return {
        ...current,
        channel: {
          ...current.channel,
          status: current.channel.status === "disconnected" ? "waiting_qr" : current.channel.status,
          qrGeneration,
          qrCodeText: makeQrCodeText(qrGeneration),
          qrExpiresAt: isoFromNow(current.channel.qrRefreshSeconds * 1000)
        }
      };
    });
    addEvent("channel.qr", {
      channel_id: CHANNEL_ID,
      rotated: true
    });
  }, [addEvent, fetchQr, runReal, state.mode]);

  const cycleChannelStatus = useCallback(() => {
    const order: RustZapState["channel"]["status"][] = [
      "disconnected",
      "waiting_qr",
      "connecting",
      "connected"
    ];
    setState((current) => {
      const next = order[(order.indexOf(current.channel.status) + 1) % order.length];
      return {
        ...current,
        channel: {
          ...current.channel,
          status: next,
          qrExpiresAt:
            next === "waiting_qr" || next === "connecting"
              ? isoFromNow(current.channel.qrRefreshSeconds * 1000)
              : current.channel.qrExpiresAt
        }
      };
    });
    addEvent("channel.status", { channel_id: CHANNEL_ID, status: "cycled" });
  }, [addEvent]);

  const selectConversation = useCallback((conversationId: string) => {
    setState((current) => ({ ...current, selectedConversationId: conversationId }));
  }, []);

  const sendMessage = useCallback(
    async (conversationId: string, text: string) => {
      const trimmed = text.trim();
      if (!trimmed) {
        return;
      }
      if (state.mode === "real") {
        await runReal(
          "message.send",
          "POST",
          tenantPath(`/conversations/${conversationId}/messages`),
          {
            type: "text",
            text: trimmed,
            quoted_message_id: null,
            metadata: {
              source: "dev_tester",
              mode: "manual",
              external_user_id: "dev_tester"
            }
          },
          idempotencyKey("send-message")
        );
        return;
      }

      const messageId = `msg_out_${Date.now()}`;
      setState((current) => {
        const conversation = current.conversations.find((item) => item.id === conversationId);
        const seq = (conversation?.lastSeq ?? 0) + 1;
        const message: Message = {
          id: messageId,
          conversationId,
          conversationSeq: seq,
          direction: "outbound",
          senderName: "RustZap",
          type: "text",
          text: trimmed,
          status: "queued",
          isStarred: false,
          isPinned: false,
          createdAt: new Date().toISOString()
        };
        return {
          ...current,
          messages: [...current.messages, message],
          conversations: updateConversationForMessage(current, conversationId, seq, 0)
        };
      });
      addEvent("message.sent", { conversation_id: conversationId, message_id: messageId });

      const statusFlow: MessageStatus[] = ["sent_to_whatsapp", "delivered", "read"];
      statusFlow.forEach((status, index) => {
        window.setTimeout(() => {
          setState((current) => ({
            ...current,
            messages: current.messages.map((message) =>
              message.id === messageId ? { ...message, status } : message
            )
          }));
          addEvent("message.receipt", {
            conversation_id: conversationId,
            message_id: messageId,
            receipt_type: status
          });
        }, (index + 1) * 900);
      });
    },
    [addEvent, runReal, state.mode]
  );

  const simulateInboundText = useCallback(
    async (conversationId: string, text: string) => {
      const trimmed = text.trim() || "Mensagem simulada recebida pelo WhatsApp.";
      if (state.mode === "real") {
        await runReal("dev.inbound_text", "POST", devPath("/simulate/inbound-text"), {
          conversation_id: conversationId,
          text: trimmed
        });
        return;
      }

      const messageId = `msg_in_${Date.now()}`;
      setState((current) => {
        const conversation = current.conversations.find((item) => item.id === conversationId);
        const seq = (conversation?.lastSeq ?? 0) + 1;
        const sender = conversation?.title ?? "Contato";
        const message: Message = {
          id: messageId,
          conversationId,
          conversationSeq: seq,
          direction: "inbound",
          senderName: sender,
          type: "text",
          text: trimmed,
          status: "received",
          isStarred: false,
          isPinned: false,
          createdAt: new Date().toISOString()
        };
        return {
          ...current,
          messages: [...current.messages, message],
          conversations: updateConversationForMessage(current, conversationId, seq)
        };
      });
      addEvent("message.received", { conversation_id: conversationId, message_id: messageId });
      addEvent("conversation.dirty", {
        conversation_id: conversationId,
        reason: "new_message",
        priority: 100
      });
    },
    [addEvent, runReal, state.mode]
  );

  const simulateInboundAudio = useCallback(
    async (conversationId: string) => {
      if (state.mode === "real") {
        await runReal("dev.inbound_audio", "POST", devPath("/simulate/inbound-audio"), {
          conversation_id: conversationId
        });
        return;
      }

      const now = Date.now();
      const messageId = `msg_audio_${now}`;
      const mediaId = `media_audio_${now}`;
      const transcriptText = "Audio dev transcrito: cliente quer retorno hoje a tarde.";
      setState((current) => {
        const conversation = current.conversations.find((item) => item.id === conversationId);
        const seq = (conversation?.lastSeq ?? 0) + 1;
        const message: Message = {
          id: messageId,
          conversationId,
          conversationSeq: seq,
          direction: "inbound",
          senderName: conversation?.title ?? "Contato",
          type: "voice_note",
          text: "Audio simulado",
          mediaId,
          status: "played",
          isStarred: false,
          isPinned: false,
          transcriptText,
          createdAt: new Date().toISOString()
        };
        return {
          ...current,
          messages: [...current.messages, message],
          media: [
            ...current.media,
            {
              id: mediaId,
              conversationId,
              messageId,
              mediaType: "audio",
              mimeType: "audio/ogg",
              originalFilename: "dev-audio.ogg",
              sizeBytes: 256400,
              storageStatus: "temp",
              expiresAt: isoFromNow(1000 * 60 * 60 * 6)
            }
          ],
          transcripts: [
            ...current.transcripts,
            {
              id: `transcript_${now}`,
              messageId,
              mediaId,
              provider: "groq",
              model: "whisper-large-v3-turbo",
              language: "pt",
              text: transcriptText,
              status: "completed",
              createdAt: new Date().toISOString()
            }
          ],
          conversations: updateConversationForMessage(current, conversationId, seq)
        };
      });
      addEvent("message.received", { conversation_id: conversationId, message_id: messageId });
      addEvent("media.stored", { media_id: mediaId, message_id: messageId });
      addEvent("transcript.completed", { media_id: mediaId, message_id: messageId });
    },
    [addEvent, runReal, state.mode]
  );

  const simulateInboundMedia = useCallback(
    async (conversationId: string, mediaType: Exclude<MediaType, "audio">) => {
      if (state.mode === "real") {
        await runReal("dev.inbound_image", "POST", devPath("/simulate/inbound-image"), {
          conversation_id: conversationId,
          media_type: mediaType
        });
        return;
      }

      const now = Date.now();
      const messageId = `msg_${mediaType}_${now}`;
      const mediaId = `media_${mediaType}_${now}`;
      const filename = mediaType === "image" ? "dev-image.jpg" : "dev-document.pdf";
      setState((current) => {
        const conversation = current.conversations.find((item) => item.id === conversationId);
        const seq = (conversation?.lastSeq ?? 0) + 1;
        const message: Message = {
          id: messageId,
          conversationId,
          conversationSeq: seq,
          direction: "inbound",
          senderName: conversation?.title ?? "Contato",
          type: mediaType,
          text: mediaType === "image" ? "Imagem simulada" : "Documento simulado",
          mediaId,
          status: "received",
          isStarred: false,
          isPinned: false,
          createdAt: new Date().toISOString()
        };
        return {
          ...current,
          messages: [...current.messages, message],
          media: [
            ...current.media,
            {
              id: mediaId,
              conversationId,
              messageId,
              mediaType,
              mimeType: mediaType === "image" ? "image/jpeg" : "application/pdf",
              originalFilename: filename,
              sizeBytes: mediaType === "image" ? 916000 : 480000,
              storageStatus: "temp",
              expiresAt: isoFromNow(1000 * 60 * 60 * 4)
            }
          ],
          conversations: updateConversationForMessage(current, conversationId, seq)
        };
      });
      addEvent("message.received", { conversation_id: conversationId, message_id: messageId });
      addEvent("media.stored", { media_id: mediaId, message_id: messageId });
    },
    [addEvent, runReal, state.mode]
  );

  const reactToMessage = useCallback(
    async (messageId: string, emoji: string) => {
      if (state.mode === "real") {
        await runReal("message.react", "POST", tenantPath(`/messages/${messageId}/react`), {
          emoji
        });
        return;
      }
      setState((current) => ({
        ...current,
        messages: current.messages.map((message) =>
          message.id === messageId ? { ...message, reaction: message.reaction ? undefined : emoji } : message
        )
      }));
      addEvent("message.reaction", { message_id: messageId, emoji });
    },
    [addEvent, runReal, state.mode]
  );

  const togglePin = useCallback(
    async (messageId: string) => {
      if (!state.channel.capabilities.pin_message) {
        addEvent("tester.command.disabled", {
          command: "pin_message",
          message_id: messageId
        });
        return;
      }
      const message = state.messages.find((item) => item.id === messageId);
      if (state.mode === "real") {
        await runReal(
          message?.isPinned ? "message.unpin" : "message.pin",
          message?.isPinned ? "DELETE" : "POST",
          tenantPath(`/messages/${messageId}/pin`)
        );
        return;
      }
      setState((current) => ({
        ...current,
        messages: current.messages.map((item) =>
          item.id === messageId ? { ...item, isPinned: !item.isPinned } : item
        )
      }));
      addEvent("message.pinned", { message_id: messageId, pinned: !message?.isPinned });
    },
    [addEvent, runReal, state.channel.capabilities.pin_message, state.messages, state.mode]
  );

  const toggleStar = useCallback(
    async (messageId: string) => {
      if (!state.channel.capabilities.star_message) {
        addEvent("tester.command.disabled", {
          command: "star_message",
          message_id: messageId
        });
        return;
      }
      const message = state.messages.find((item) => item.id === messageId);
      if (state.mode === "real") {
        await runReal(
          message?.isStarred ? "message.unstar" : "message.star",
          message?.isStarred ? "DELETE" : "POST",
          tenantPath(`/messages/${messageId}/star`)
        );
        return;
      }
      setState((current) => ({
        ...current,
        messages: current.messages.map((item) =>
          item.id === messageId ? { ...item, isStarred: !item.isStarred } : item
        )
      }));
      addEvent("message.starred", { message_id: messageId, starred: !message?.isStarred });
    },
    [addEvent, runReal, state.channel.capabilities.star_message, state.messages, state.mode]
  );

  const markConversationRead = useCallback(
    async (conversationId: string) => {
      if (state.mode === "real") {
        await runReal("conversation.mark_read", "POST", tenantPath(`/conversations/${conversationId}/mark-read`));
        return;
      }
      setState((current) => ({
        ...current,
        conversations: current.conversations.map((conversation) =>
          conversation.id === conversationId ? { ...conversation, unreadCount: 0 } : conversation
        ),
        messages: current.messages.map((message) =>
          message.conversationId === conversationId && message.direction === "inbound"
            ? { ...message, status: "read" }
            : message
        )
      }));
      addEvent("message.receipt", { conversation_id: conversationId, receipt_type: "read" });
    },
    [addEvent, runReal, state.mode]
  );

  const saveMedia = useCallback(
    async (mediaId: string) => {
      if (state.mode === "real") {
        await runReal("media.save", "POST", tenantPath(`/media/${mediaId}/save`), {
          entity_type: "lead",
          entity_id: "lead_dev",
          folder: "documentos_do_lead",
          filename: "dev-media",
          metadata: {
            source: "dev_tester",
            saved_by_user_id: "dev_user"
          }
        });
        return;
      }
      setState((current) => ({
        ...current,
        media: current.media.map((media) =>
          media.id === mediaId
            ? {
                ...media,
                storageStatus: "permanent",
                savedAt: new Date().toISOString()
              }
            : media
        )
      }));
      addEvent("media.stored", { media_id: mediaId, storage_status: "permanent" });
    },
    [addEvent, runReal, state.mode]
  );

  const selectGroup = useCallback((groupId: string) => {
    setState((current) => ({ ...current, selectedGroupId: groupId }));
  }, []);

  const simulateGroupEvent = useCallback(
    async (groupId: string, kind: "join" | "leave") => {
      if (state.mode === "real") {
        await runReal("dev.group_event", "POST", devPath("/simulate/group-event"), {
          group_id: groupId,
          kind
        });
        return;
      }

      const addedMember: GroupMember = {
        contactId: `contact_guest_${Date.now()}`,
        name: "Convidado Dev",
        phoneE164: "+5511966663300",
        role: "member",
        isAdmin: false,
        joinedAt: new Date().toISOString()
      };
      let removedName = "";
      setState((current) => ({
        ...current,
        groups: current.groups.map((group) => {
          if (group.id !== groupId) {
            return group;
          }
          if (kind === "join") {
            return {
              ...group,
              members: [...group.members, addedMember],
              lastMessageAt: new Date().toISOString()
            };
          }
          const removable = [...group.members].reverse().find((member) => !member.isAdmin);
          removedName = removable?.name ?? "";
          return {
            ...group,
            members: group.members.filter((member) => member.contactId !== removable?.contactId),
            lastMessageAt: new Date().toISOString()
          };
        })
      }));
      addEvent(kind === "join" ? "group.member.added" : "group.member.removed", {
        group_id: groupId,
        member: kind === "join" ? addedMember.name : removedName
      });
    },
    [addEvent, runReal, state.mode]
  );

  const exitGroup = useCallback(
    async (groupId: string) => {
      if (state.mode === "real") {
        await runReal("group.exit", "POST", tenantPath(`/groups/${groupId}/exit`));
        return;
      }
      setState((current) => ({
        ...current,
        groups: current.groups.map((group) =>
          group.id === groupId
            ? {
                ...group,
                exitedAt: new Date().toISOString(),
                canExit: false,
                canManage: false,
                role: "unknown"
              }
            : group
        )
      }));
      addEvent("group.updated", { group_id: groupId, exited: true });
    },
    [addEvent, runReal, state.mode]
  );

  const disconnectWebSocket = useCallback(() => {
    wsWantedRef.current = false;
    if (reconnectTimerRef.current) {
      clearTimeout(reconnectTimerRef.current);
      reconnectTimerRef.current = null;
    }
    if (mockWsTimerRef.current) {
      clearInterval(mockWsTimerRef.current);
      mockWsTimerRef.current = null;
    }
    if (wsRef.current) {
      wsRef.current.close();
      wsRef.current = null;
    }
    setState((current) => ({
      ...current,
      ws: {
        ...current.ws,
        status: "closed"
      }
    }));
  }, []);

  const connectWebSocket = useCallback(() => {
    wsWantedRef.current = true;
    if (mockWsTimerRef.current) {
      clearInterval(mockWsTimerRef.current);
      mockWsTimerRef.current = null;
    }
    if (state.mode === "mock") {
      setState((current) => ({
        ...current,
        ws: {
          status: "connected",
          attempts: current.ws.attempts + 1
        }
      }));
      addEvent("websocket.connected", { mode: "mock" });
      mockWsTimerRef.current = setInterval(() => {
        addEvent("conversation.dirty", {
          conversation_id: "conv_maria",
          reason: "mock_heartbeat",
          priority: 20
        });
      }, 5000);
      return;
    }

    setState((current) => ({
      ...current,
      ws: {
        status: current.ws.attempts > 0 ? "reconnecting" : "connecting",
        attempts: current.ws.attempts + 1
      }
    }));

    const ws = new WebSocket(rustZapWsUrl());
    wsRef.current = ws;

    ws.onopen = () => {
      setState((current) => ({
        ...current,
        ws: {
          ...current.ws,
          status: "connected",
          lastError: undefined
        }
      }));
      ws.send(
        JSON.stringify({
          type: "subscribe",
          project_id: PROJECT_ID,
          company_id: COMPANY_ID,
          topics: ["channel.*", "conversation.dirty", "message.*", "media.*", "transcript.*", "group.*"]
        })
      );
      addEvent("websocket.connected", { mode: "real", url: rustZapWsUrl() });
    };

    ws.onmessage = (message) => {
      let payload: Record<string, unknown> = { raw: String(message.data) };
      try {
        payload = JSON.parse(String(message.data)) as Record<string, unknown>;
      } catch {
        payload = { raw: String(message.data) };
      }
      addEvent(typeof payload.type === "string" ? payload.type : "websocket.message", payload);
    };

    ws.onerror = () => {
      setState((current) => ({
        ...current,
        ws: {
          ...current.ws,
          status: "error",
          lastError: "WebSocket error"
        }
      }));
    };

    ws.onclose = () => {
      if (!wsWantedRef.current) {
        return;
      }
      setState((current) => ({
        ...current,
        ws: {
          ...current.ws,
          status: "reconnecting"
        }
      }));
      reconnectTimerRef.current = setTimeout(() => {
        if (wsWantedRef.current) {
          connectWebSocketRef.current();
        }
      }, 1800);
    };
  }, [addEvent, state.mode]);

  useEffect(() => {
    connectWebSocketRef.current = connectWebSocket;
  }, [connectWebSocket]);

  useEffect(() => () => disconnectWebSocket(), [disconnectWebSocket]);

  const activeConversation = useMemo(
    () => state.conversations.find((conversation) => conversation.id === state.selectedConversationId),
    [state.conversations, state.selectedConversationId]
  );
  const activeMessages = useMemo(
    () =>
      state.messages
        .filter((message) => message.conversationId === state.selectedConversationId)
        .sort((left, right) => left.conversationSeq - right.conversationSeq),
    [state.messages, state.selectedConversationId]
  );
  const averageLatencyMs = useMemo(() => {
    if (state.events.length === 0) {
      return 0;
    }
    const total = state.events.reduce((sum, event) => sum + event.latencyMs, 0);
    return Math.round(total / state.events.length);
  }, [state.events]);

  const value = useMemo<RustZapContextValue>(
    () => ({
      state,
      actions: {
        setMode,
        ensureDevTenant,
        refreshReal,
        createChannel,
        connectChannel,
        fetchQr,
        rotateQr,
        cycleChannelStatus,
        selectConversation,
        sendMessage,
        simulateInboundText,
        simulateInboundAudio,
        simulateInboundMedia,
        reactToMessage,
        togglePin,
        toggleStar,
        markConversationRead,
        saveMedia,
        selectGroup,
        simulateGroupEvent,
        exitGroup,
        connectWebSocket,
        disconnectWebSocket
      },
      activeConversation,
      activeMessages,
      averageLatencyMs
    }),
    [
      activeConversation,
      activeMessages,
      averageLatencyMs,
      connectChannel,
      connectWebSocket,
      createChannel,
      cycleChannelStatus,
      disconnectWebSocket,
      ensureDevTenant,
      exitGroup,
      fetchQr,
      markConversationRead,
      reactToMessage,
      refreshReal,
      rotateQr,
      saveMedia,
      selectConversation,
      selectGroup,
      sendMessage,
      setMode,
      simulateGroupEvent,
      simulateInboundAudio,
      simulateInboundMedia,
      simulateInboundText,
      state,
      togglePin,
      toggleStar
    ]
  );

  return <RustZapContext.Provider value={value}>{children}</RustZapContext.Provider>;
}

export function useRustZap() {
  const context = useContext(RustZapContext);
  if (!context) {
    throw new Error("useRustZap must be used inside RustZapProvider");
  }
  return context;
}
