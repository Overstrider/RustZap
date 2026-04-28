export type ApiMode = "mock" | "real";

export type ChannelStatus = "disconnected" | "waiting_qr" | "connecting" | "connected";

export type ConversationType = "direct" | "group";

export type ControlMode = "manual" | "copilot" | "background" | "autopilot" | "human_takeover";

export type MessageDirection = "inbound" | "outbound";

export type MessageType = "text" | "image" | "audio" | "voice_note" | "document" | "system";

export type MessageStatus =
  | "received"
  | "queued"
  | "sent_to_whatsapp"
  | "server_ack"
  | "delivered"
  | "read"
  | "played"
  | "failed";

export type MediaType = "image" | "audio" | "document";

export type StorageStatus = "temp" | "quarantine" | "permanent" | "deleted" | "rejected";

export type GroupRole = "owner" | "admin" | "member" | "unknown";

export type WsStatus = "idle" | "connecting" | "connected" | "reconnecting" | "closed" | "error";

export type CapabilityKey =
  | "send_text"
  | "send_media"
  | "send_reaction"
  | "pin_message"
  | "star_message"
  | "mark_read"
  | "groups_read"
  | "groups_manage"
  | "group_invite_accept"
  | "group_member_promote"
  | "group_member_demote";

export type Project = {
  id: string;
  name: string;
  status: "active" | "inactive";
};

export type Company = {
  id: string;
  projectId: string;
  name: string;
  status: "active" | "inactive";
};

export type ChannelAccount = {
  id: string;
  label: string;
  provider: "whatsapp";
  phoneE164: string;
  status: ChannelStatus;
  qrCodeText: string;
  qrExpiresAt: string;
  qrRefreshSeconds: number;
  qrGeneration: number;
  capabilities: Record<CapabilityKey, boolean>;
};

export type Contact = {
  id: string;
  displayName: string;
  pushName: string;
  phoneE164: string;
  businessDescription?: string;
  firstContactAt: string;
  lastContactAt: string;
};

export type Conversation = {
  id: string;
  type: ConversationType;
  title: string;
  contactId?: string;
  groupId?: string;
  lastSeq: number;
  lastMessageAt: string;
  unreadCount: number;
  controlMode: ControlMode;
  isPinned: boolean;
  isMuted: boolean;
};

export type Message = {
  id: string;
  conversationId: string;
  conversationSeq: number;
  direction: MessageDirection;
  senderName: string;
  type: MessageType;
  text: string;
  mediaId?: string;
  status: MessageStatus;
  isStarred: boolean;
  isPinned: boolean;
  reaction?: string;
  transcriptText?: string;
  createdAt: string;
};

export type MediaObject = {
  id: string;
  conversationId: string;
  messageId: string;
  contactId?: string;
  mediaType: MediaType;
  mimeType: string;
  originalFilename: string;
  sizeBytes: number;
  storageStatus: StorageStatus;
  expiresAt: string;
  savedAt?: string;
};

export type Transcript = {
  id: string;
  messageId: string;
  mediaId: string;
  provider: "groq";
  model: string;
  language: string;
  text: string;
  status: "pending" | "processing" | "completed" | "failed";
  createdAt: string;
};

export type GroupMember = {
  contactId: string;
  name: string;
  phoneE164: string;
  role: GroupRole;
  isAdmin: boolean;
  joinedAt: string;
};

export type Group = {
  id: string;
  subject: string;
  description: string;
  ownerJid: string;
  role: GroupRole;
  canManage: boolean;
  canExit: boolean;
  exitedAt?: string;
  members: GroupMember[];
  lastMessageAt: string;
};

export type RustZapEvent = {
  id: string;
  type: string;
  eventSeq: number;
  createdAt: string;
  receivedAt: string;
  latencyMs: number;
  payload: Record<string, unknown>;
};

export type WsState = {
  status: WsStatus;
  attempts: number;
  lastError?: string;
};

export type RustZapState = {
  mode: ApiMode;
  devTenantReady: boolean;
  projects: Project[];
  companies: Company[];
  channel: ChannelAccount;
  contacts: Contact[];
  conversations: Conversation[];
  messages: Message[];
  media: MediaObject[];
  transcripts: Transcript[];
  groups: Group[];
  events: RustZapEvent[];
  selectedConversationId: string;
  selectedGroupId: string;
  ws: WsState;
  lastRealResponse?: {
    label: string;
    status: number;
    body: unknown;
  };
  error?: string;
};

export type RustZapHttpResponse = {
  status: number;
  body: unknown;
};
