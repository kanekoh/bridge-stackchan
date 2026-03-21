# CLAUDE.md

## Project Overview
This project connects OpenClaw, Stack-chan, MQTT, Slack, and voice services to build a cute family assistant robot system.

Main goals:
- Let Stack-chan act as a super cute assistant robot
- Allow Slack messages to trigger Stack-chan speech
- Use OpenClaw as the conversation brain
- Use MQTT as the event/output channel to M5Stack
- Use VOICEVOX to generate speech audio
- Later support audio input from Stack-chan through a web bridge and STT

## System Architecture
Current high-level architecture:

Slack -> OpenClaw -> Bridge API -> VOICEVOX -> MQTT -> Stack-chan

Future conversational architecture:

Stack-chan -> Cloudflare Tunnel -> Front Web Server / Bridge API -> STT -> OpenClaw -> VOICEVOX -> MQTT -> Stack-chan

## Key Design Principles
- Keep the architecture simple and modular
- Let OpenClaw decide what to say, not how to deliver audio
- Let the Bridge API handle VOICEVOX, MP3 generation, file hosting, and MQTT publish
- Keep Stack-chan lightweight
- Prefer short, cute, spoken-friendly responses
- Avoid sending excessive logs or large raw text to the LLM
- Optimize for practical latency and maintainability

## Stack-chan Personality Requirements
Stack-chan is a super cute assistant robot.

Behavior and tone:
- Default language is Japanese
- If spoken to in English, reply in simple Japanese with light katakana-English
- Replies should be short, simple, cute, and easy to speak aloud
- Tone should be warm, cheerful, gentle, and supportive
- Avoid formal business tone
- Avoid long and overly detailed replies unless explicitly requested
- Technical explanations should still be practical and correct

## Shared Family Usage
This system is used by the family.

Rules:
- Do not assume only one fixed user
- Do not overfit behavior to a single individual unless the current conversation clearly identifies them
- Keep responses understandable for different family members
- Use friendly and neutral phrasing

## Current Implementation Priorities
### Phase 1
Implement Slack-triggered speech output:
- Slack message enters OpenClaw
- OpenClaw generates a spoken response
- Bridge API generates MP3 with VOICEVOX
- Bridge API publishes audio URL to MQTT
- Stack-chan detects MQTT event and plays audio

### Phase 2
Implement Stack-chan voice input:
- M5Stack records audio
- Audio is uploaded through Cloudflare Tunnel to the Bridge API
- STT converts audio to text
- Text is sent to OpenClaw
- OpenClaw response is converted to speech
- MQTT delivers playback instruction to Stack-chan

### Phase 3
Integrate external services:
- Google Calendar
- Trello
- Scheduled notifications
- Context-aware reminders

## Bridge API Responsibilities
The Bridge API should be a lightweight service running on Raspberry Pi.

Responsibilities:
- Receive text to speak
- Generate speech audio using VOICEVOX
- Save generated audio files
- Serve audio files over HTTP
- Publish MQTT messages with audio URLs
- Receive audio uploaded from Stack-chan
- Run STT
- Forward text to OpenClaw
- Return or publish OpenClaw responses

Recommended endpoints:
- `POST /speak`
- `POST /ingest-audio`
- `GET /audio/<id>.mp3`
- `GET /healthz`

## MQTT Design
Use MQTT as the device event channel.

Suggested topics:
- `stackchan/<deviceId>/speak`
- `stackchan/<deviceId>/status`
- `stackchan/<deviceId>/event`

Suggested `speak` payload:
```json
{
  "type": "speak",
  "audioUrl": "https://example.com/audio/abc123.mp3",
  "text": "おはよう。きょうの予定をおしらせするよ。",
  "source": "openclaw",
  "priority": "normal",
  "requestId": "req-123"
}
