# AGENTS.md

## Product direction
- Build a Telegram chatbot that acts as a proxy over isolated OpenCode sessions.
- Telegram chat history and Telegram Bot API semantics are the source of truth; do not invent a separate primary session model that conflicts with the chat.
- Users may not know what a session is; expose “new clean context” only as an explicit user command.
- Support user messages with images/files and return AI processing results back in the same Telegram chat.

## Execution model
- AI work should run in isolated OpenCode workers with access only to chat-provided data and explicitly allowed MCP servers.
- The serverless bot backend should route work to a pool of OpenCode workers rather than running all AI processing inline.
- Assume hosting targets Yandex Cloud serverless primitives unless requirements change.
- Persist required data in cloud storage such as S3 buckets; partition storage so one Telegram chat/user cannot accidentally see another’s data.
- Prefer a serverless database compatible with Yandex Cloud, e.g. YDB, for operational state.

## Implementation preferences
- Backend language priority: Go first, then TypeScript, then Python.
- Keep infrastructure choices friendly to Yandex Cloud serverless and YDB support.
- Include a minimal admin surface for worker health and consumed-token monitoring.

## Current repo state
- This repository is currently empty: no manifests, scripts, CI, tests, or existing instruction files were present when this file was created.
- Do not claim build/test/lint commands until they exist in executable config.
