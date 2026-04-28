# 12 - Dev Tester Screens

## Status
Pendente.

## Evidencia Atual
- `dev-tester/` existe e compila.
- `dev-tester/app/page.tsx` implementa uma UI unica de WhatsApp/chat.
- `dev-tester/app/api/rustzap/[...path]/route.ts` implementa proxy interno.
- `AppShell` referencia `/dashboard`, `/channel`, `/chat`, `/groups`, `/media`, `/events`, mas essas paginas nao existem.

## O Que Falta
- Criar telas obrigatorias separadas.
- Unificar a UI atual com provider/mock/real mode ou remover codigo duplicado.
- Tela dedicada de eventos com stream WebSocket e latencia media.
- Tela dedicada de grupos.
- Tela dedicada de media.
- Mock mode funcional na primeira tela e real localhost mode funcional.

## Plano De Implementacao
1. Manter `RustZapProvider` como fonte unica de estado do tester.
2. Criar layout app com `AppShell`.
3. Criar paginas:
   - `/dashboard`
   - `/channel`
   - `/chat`
   - `/groups`
   - `/media`
   - `/events`
4. Mover a UI de chat atual para `/chat`.
5. Criar `/channel` para tenant, canal, connect, QR, contador e status.
6. Criar `/groups` para lista, membros, admins e simulacao de evento.
7. Criar `/media` para lista, download-url e save permanente.
8. Criar `/events` para WebSocket, reconnect, stream e latencia media.
9. Criar redirect de `/` para `/dashboard` ou dashboard real.
10. Garantir que mock mode nao dependa do backend.
11. Garantir que real mode use proxy `/api/rustzap`.

## Criterios De Aceite
- Todas as rotas obrigatorias abrem no Next build.
- Mock mode funciona sem RustZap rodando.
- Real mode chama `localhost:8167` via proxy.
- Tela events mostra reconexao e latencia media.
- Tela channel mostra QR, `expires_at`, contador regressivo e status.

## Testes
- `npm run typecheck`
- `npm run lint`
- `npm run build`
- Teste manual com `NEXT_PUBLIC_RUSTZAP_MOCK=true`.
- Teste manual com backend RustZap em real mode.
- Playwright opcional para navegar pelas seis paginas.
