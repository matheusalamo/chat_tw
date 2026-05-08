import os
import asyncio
import re
import json
import aiohttp
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse
from pathlib import Path
from datetime import datetime
import uvicorn

TWITCH_CHANNEL = "ielziinho"
DISCORD_WEBHOOK_URL = "https://ptb.discord.com/api/webhooks/1501345501994287359/VDAEHn-bUi2tPjfbIFvu-FhBjz8n1YwZ1gcRF_oO7XfTsNk3KiwE4cseWqNT8QD3q6wn"
TWITCH_OAUTH_TOKEN = "fyxjxh7nckwaz2v8hhjpn6pu89kpke"
NICK = "lordsmo00ke"


class ConnectionManager:
    def __init__(self):
        self.active: list[WebSocket] = []

    async def connect(self, ws: WebSocket):
        await ws.accept()
        self.active.append(ws)

    def disconnect(self, ws: WebSocket):
        self.active.remove(ws)

    async def broadcast(self, data: str):
        for ws in self.active.copy():
            try:
                await ws.send_text(data)
            except:
                self.active.remove(ws)


manager = ConnectionManager()


class TwitchClient:
    def __init__(self):
        self.host = "irc.chat.twitch.tv"
        self.port = 6667
        self.reader = None
        self.writer = None
        self.running = True

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self.writer.write(f"PASS oauth:{TWITCH_OAUTH_TOKEN}\n".encode())
        self.writer.write(f"NICK {NICK}\n".encode())
        self.writer.write(f"JOIN #{TWITCH_CHANNEL}\n".encode())
        await self.writer.drain()

    async def send_discord(self, author: str, content: str):
        ts = datetime.now().strftime("%H:%M:%S")
        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(
                    DISCORD_WEBHOOK_URL,
                    json={"content": f"[{ts}] {author}: {content}"},
                ) as resp:
                    if resp.status not in (200, 204):
                        print(f"Discord error: {resp.status}")
            except Exception as e:
                print(f"Discord send error: {e}")

    async def run(self):
        while self.running:
            try:
                await self.connect()
                print(f"Conectado a #{TWITCH_CHANNEL}")

                while self.running:
                    data = await self.reader.read(4096)
                    if not data:
                        break

                    for msg in data.decode("utf-8", errors="ignore").split("\r\n"):
                        if not msg:
                            continue
                        if msg.startswith("PING"):
                            self.writer.write(b"PONG :tmi.twitch.tv\r\n")
                            await self.writer.drain()
                            continue

                        m = re.search(r":(\w+)!.*PRIVMSG #(\w+) :(.+)", msg)
                        if m:
                            author, channel, content = m.groups()
                            payload = json.dumps({
                                "author": author,
                                "content": content,
                                "timestamp": datetime.now().strftime("%H:%M:%S"),
                            })
                            await manager.broadcast(payload)
                            await self.send_discord(author, content)

            except Exception as e:
                print(f"Erro: {e}. Reconectando em 10s...")
                await asyncio.sleep(10)

    def stop(self):
        self.running = False
        if self.writer:
            self.writer.close()


twitch = TwitchClient()


app = FastAPI()

HTML = (Path(__file__).parent / "templates" / "dashboard.html").read_text(encoding="utf-8")


@app.get("/")
async def get():
    return HTMLResponse(HTML)


@app.websocket("/ws")
async def websocket(ws: WebSocket):
    await manager.connect(ws)
    try:
        while True:
            await ws.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(ws)


@app.on_event("startup")
async def startup():
    asyncio.create_task(twitch.run())


@app.on_event("shutdown")
async def shutdown():
    twitch.stop()


if __name__ == "__main__":
    port = int(os.getenv("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
