import type {
  CapabilityKey,
  ChannelAccount,
  Contact,
  Conversation,
  Group,
  MediaObject,
  Message,
  Project,
  RustZapEvent,
  RustZapState,
  Transcript
} from "@/lib/types";

export const PROJECT_ID = "tetoz";
export const COMPANY_ID = "company_dev";
export const CHANNEL_ID = "ch_dev_whatsapp";
export const DEFAULT_QR_REFRESH_SECONDS = 20;

const capabilityKeys: CapabilityKey[] = [
  "send_text",
  "send_media",
  "send_reaction",
  "pin_message",
  "star_message",
  "mark_read",
  "groups_read",
  "groups_manage",
  "group_invite_accept",
  "group_member_promote",
  "group_member_demote"
];

export function isoFromNow(offsetMs: number) {
  return new Date(Date.now() + offsetMs).toISOString();
}

export function formatBytes(sizeBytes: number) {
  if (sizeBytes < 1024) {
    return `${sizeBytes} B`;
  }
  if (sizeBytes < 1024 * 1024) {
    return `${(sizeBytes / 1024).toFixed(1)} KB`;
  }
  return `${(sizeBytes / 1024 / 1024).toFixed(1)} MB`;
}

export function makeQrCodeText(generation: number) {
  return [
    "RUSTZAP-DEV-QR",
    `project=${PROJECT_ID}`,
    `company=${COMPANY_ID}`,
    `channel=${CHANNEL_ID}`,
    `generation=${generation}`,
    `nonce=${Math.random().toString(36).slice(2, 10)}`
  ].join(";");
}

export function makeEvent(
  type: string,
  eventSeq: number,
  payload: Record<string, unknown>,
  createdAt = new Date(Date.now() - Math.round(10 + Math.random() * 85)).toISOString()
): RustZapEvent {
  const receivedAt = new Date().toISOString();
  return {
    id: `evt_${eventSeq}_${Math.random().toString(36).slice(2, 7)}`,
    type,
    eventSeq,
    createdAt,
    receivedAt,
    latencyMs: Math.max(1, Date.parse(receivedAt) - Date.parse(createdAt)),
    payload
  };
}

function capabilities(): Record<CapabilityKey, boolean> {
  return capabilityKeys.reduce(
    (acc, key) => {
      acc[key] = true;
      return acc;
    },
    {} as Record<CapabilityKey, boolean>
  );
}

export function createInitialState(): RustZapState {
  const project: Project = {
    id: PROJECT_ID,
    name: "TETOZ Dev",
    status: "active"
  };
  const company = {
    id: COMPANY_ID,
    projectId: PROJECT_ID,
    name: "Company Dev",
    status: "active" as const
  };
  const channel: ChannelAccount = {
    id: CHANNEL_ID,
    label: "WhatsApp Dev",
    provider: "whatsapp",
    phoneE164: "+5511999000000",
    status: "disconnected",
    qrCodeText: makeQrCodeText(1),
    qrExpiresAt: isoFromNow(DEFAULT_QR_REFRESH_SECONDS * 1000),
    qrRefreshSeconds: DEFAULT_QR_REFRESH_SECONDS,
    qrGeneration: 1,
    capabilities: capabilities()
  };
  const contacts: Contact[] = [
    {
      id: "contact_maria",
      displayName: "Maria Lima",
      pushName: "Maria",
      phoneE164: "+5511999112233",
      businessDescription: "Lead looking for a two-bedroom apartment.",
      firstContactAt: isoFromNow(-1000 * 60 * 60 * 24 * 12),
      lastContactAt: isoFromNow(-1000 * 60 * 4)
    },
    {
      id: "contact_joao",
      displayName: "Joao Pereira",
      pushName: "Joao",
      phoneE164: "+5511988776655",
      firstContactAt: isoFromNow(-1000 * 60 * 60 * 24 * 4),
      lastContactAt: isoFromNow(-1000 * 60 * 33)
    },
    {
      id: "contact_bia",
      displayName: "Bia Ramos",
      pushName: "Bia",
      phoneE164: "+5511977774411",
      firstContactAt: isoFromNow(-1000 * 60 * 60 * 24 * 2),
      lastContactAt: isoFromNow(-1000 * 60 * 9)
    }
  ];
  const conversations: Conversation[] = [
    {
      id: "conv_maria",
      type: "direct",
      title: "Maria Lima",
      contactId: "contact_maria",
      lastSeq: 5,
      lastMessageAt: isoFromNow(-1000 * 60 * 4),
      unreadCount: 2,
      controlMode: "autopilot",
      isPinned: true,
      isMuted: false
    },
    {
      id: "conv_joao",
      type: "direct",
      title: "Joao Pereira",
      contactId: "contact_joao",
      lastSeq: 2,
      lastMessageAt: isoFromNow(-1000 * 60 * 33),
      unreadCount: 0,
      controlMode: "manual",
      isPinned: false,
      isMuted: false
    },
    {
      id: "conv_group_sales",
      type: "group",
      title: "Equipe comercial",
      groupId: "group_sales",
      lastSeq: 3,
      lastMessageAt: isoFromNow(-1000 * 60 * 11),
      unreadCount: 1,
      controlMode: "copilot",
      isPinned: false,
      isMuted: true
    }
  ];
  const messages: Message[] = [
    {
      id: "msg_maria_1",
      conversationId: "conv_maria",
      conversationSeq: 1,
      direction: "inbound",
      senderName: "Maria Lima",
      type: "text",
      text: "Oi, tenho interesse no apartamento da Vila Mariana.",
      status: "received",
      isStarred: true,
      isPinned: false,
      createdAt: isoFromNow(-1000 * 60 * 18)
    },
    {
      id: "msg_maria_2",
      conversationId: "conv_maria",
      conversationSeq: 2,
      direction: "outbound",
      senderName: "RustZap",
      type: "text",
      text: "Oi, Maria. Recebi seu contato e vou te mandar as opcoes.",
      status: "read",
      isStarred: false,
      isPinned: false,
      createdAt: isoFromNow(-1000 * 60 * 16)
    },
    {
      id: "msg_maria_3",
      conversationId: "conv_maria",
      conversationSeq: 3,
      direction: "inbound",
      senderName: "Maria Lima",
      type: "voice_note",
      text: "Audio recebido",
      mediaId: "media_audio_maria",
      status: "played",
      isStarred: false,
      isPinned: true,
      transcriptText: "Prefiro visitar no sabado de manha e preciso de uma vaga.",
      createdAt: isoFromNow(-1000 * 60 * 12)
    },
    {
      id: "msg_maria_4",
      conversationId: "conv_maria",
      conversationSeq: 4,
      direction: "inbound",
      senderName: "Maria Lima",
      type: "image",
      text: "Comprovante enviado",
      mediaId: "media_image_maria",
      status: "received",
      isStarred: false,
      isPinned: false,
      createdAt: isoFromNow(-1000 * 60 * 7)
    },
    {
      id: "msg_maria_5",
      conversationId: "conv_maria",
      conversationSeq: 5,
      direction: "outbound",
      senderName: "RustZap",
      type: "text",
      text: "Vou separar horarios disponiveis.",
      status: "delivered",
      isStarred: false,
      isPinned: false,
      createdAt: isoFromNow(-1000 * 60 * 4)
    },
    {
      id: "msg_joao_1",
      conversationId: "conv_joao",
      conversationSeq: 1,
      direction: "inbound",
      senderName: "Joao Pereira",
      type: "text",
      text: "Tem casas na Zona Norte?",
      status: "read",
      isStarred: false,
      isPinned: false,
      createdAt: isoFromNow(-1000 * 60 * 50)
    },
    {
      id: "msg_joao_2",
      conversationId: "conv_joao",
      conversationSeq: 2,
      direction: "outbound",
      senderName: "RustZap",
      type: "text",
      text: "Tenho algumas opcoes. Qual faixa de preco?",
      status: "read",
      isStarred: false,
      isPinned: false,
      createdAt: isoFromNow(-1000 * 60 * 33)
    },
    {
      id: "msg_group_1",
      conversationId: "conv_group_sales",
      conversationSeq: 1,
      direction: "inbound",
      senderName: "Bia Ramos",
      type: "text",
      text: "Novo lead pediu atendimento no grupo.",
      status: "received",
      isStarred: false,
      isPinned: false,
      createdAt: isoFromNow(-1000 * 60 * 25)
    },
    {
      id: "msg_group_2",
      conversationId: "conv_group_sales",
      conversationSeq: 2,
      direction: "inbound",
      senderName: "Joao Pereira",
      type: "document",
      text: "Lista de unidades",
      mediaId: "media_doc_group",
      status: "received",
      isStarred: true,
      isPinned: false,
      createdAt: isoFromNow(-1000 * 60 * 16)
    },
    {
      id: "msg_group_3",
      conversationId: "conv_group_sales",
      conversationSeq: 3,
      direction: "outbound",
      senderName: "RustZap",
      type: "text",
      text: "Recebido. Vou registrar no CRM consumidor.",
      status: "server_ack",
      isStarred: false,
      isPinned: false,
      createdAt: isoFromNow(-1000 * 60 * 11)
    }
  ];
  const media: MediaObject[] = [
    {
      id: "media_audio_maria",
      conversationId: "conv_maria",
      messageId: "msg_maria_3",
      contactId: "contact_maria",
      mediaType: "audio",
      mimeType: "audio/ogg",
      originalFilename: "voice-note-maria.ogg",
      sizeBytes: 348012,
      storageStatus: "temp",
      expiresAt: isoFromNow(1000 * 60 * 60 * 6)
    },
    {
      id: "media_image_maria",
      conversationId: "conv_maria",
      messageId: "msg_maria_4",
      contactId: "contact_maria",
      mediaType: "image",
      mimeType: "image/jpeg",
      originalFilename: "comprovante.jpg",
      sizeBytes: 1284011,
      storageStatus: "temp",
      expiresAt: isoFromNow(1000 * 60 * 60 * 5)
    },
    {
      id: "media_doc_group",
      conversationId: "conv_group_sales",
      messageId: "msg_group_2",
      mediaType: "document",
      mimeType: "application/pdf",
      originalFilename: "unidades.pdf",
      sizeBytes: 640120,
      storageStatus: "permanent",
      expiresAt: isoFromNow(1000 * 60 * 60 * 24),
      savedAt: isoFromNow(-1000 * 60 * 14)
    }
  ];
  const transcripts: Transcript[] = [
    {
      id: "transcript_maria_audio",
      messageId: "msg_maria_3",
      mediaId: "media_audio_maria",
      provider: "groq",
      model: "whisper-large-v3-turbo",
      language: "pt",
      text: "Prefiro visitar no sabado de manha e preciso de uma vaga.",
      status: "completed",
      createdAt: isoFromNow(-1000 * 60 * 11)
    }
  ];
  const groups: Group[] = [
    {
      id: "group_sales",
      subject: "Equipe comercial",
      description: "Grupo de atendimento comercial usado no fluxo dev.",
      ownerJid: "5511999000000@s.whatsapp.net",
      role: "admin",
      canManage: true,
      canExit: true,
      lastMessageAt: isoFromNow(-1000 * 60 * 11),
      members: [
        {
          contactId: "contact_maria",
          name: "Maria Lima",
          phoneE164: "+5511999112233",
          role: "member",
          isAdmin: false,
          joinedAt: isoFromNow(-1000 * 60 * 60 * 24 * 10)
        },
        {
          contactId: "contact_joao",
          name: "Joao Pereira",
          phoneE164: "+5511988776655",
          role: "admin",
          isAdmin: true,
          joinedAt: isoFromNow(-1000 * 60 * 60 * 24 * 18)
        },
        {
          contactId: "contact_bia",
          name: "Bia Ramos",
          phoneE164: "+5511977774411",
          role: "owner",
          isAdmin: true,
          joinedAt: isoFromNow(-1000 * 60 * 60 * 24 * 42)
        }
      ]
    }
  ];
  const events = [
    makeEvent("channel.status", 1003, {
      channel_id: CHANNEL_ID,
      status: "disconnected"
    }),
    makeEvent("conversation.dirty", 1002, {
      conversation_id: "conv_maria",
      to_seq: 5,
      reason: "new_message",
      priority: 100
    }),
    makeEvent("transcript.completed", 1001, {
      message_id: "msg_maria_3",
      media_id: "media_audio_maria"
    })
  ];

  return {
    mode: "mock",
    devTenantReady: true,
    projects: [project],
    companies: [company],
    channel,
    contacts,
    conversations,
    messages,
    media,
    transcripts,
    groups,
    events,
    selectedConversationId: "conv_maria",
    selectedGroupId: "group_sales",
    ws: {
      status: "idle",
      attempts: 0
    }
  };
}
