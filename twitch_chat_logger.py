import asyncio
import aiohttp
import re
import os

TWITCH_CHANNEL = os.environ.get("TWITCH_CHANNEL", "ielziinho")
DISCORD_WEBHOOK_URL = os.environ["DISCORD_WEBHOOK_URL"]
TWITCH_OAUTH_TOKEN = os.environ["TWITCH_OAUTH_TOKEN"]
NICK = os.environ.get("NICK", "lordsmo00ke")

class TwitchChatLogger:
    def __init__(self):
        self.host = "irc.chat.twitch.tv"
        self.port = 6667
        self.reader = None
        self.writer = None

    async def connect(self):
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        self.writer.write(f"PASS oauth:{TWITCH_OAUTH_TOKEN}\n".encode())
        self.writer.write(f"NICK {NICK}\n".encode())
        self.writer.write(f"JOIN #{TWITCH_CHANNEL}\n".encode())
        await self.writer.drain()

    async def send_to_discord(self, author, content):
        from datetime import datetime
        timestamp = datetime.now().strftime("%H:%M:%S")
        message = f"[{timestamp}] {author}: {content}"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.post(DISCORD_WEBHOOK_URL, json={"content": message}) as resp:
                    if resp.status not in [200, 204]:
                        print(f"Erro Discord: {resp.status}")
            except Exception as e:
                print(f"Erro ao enviar para Discord: {e}")

    async def run(self):
        await self.connect()
        print(f"Conectado ao canal #{TWITCH_CHANNEL}")

        while True:
            data = await self.reader.read(4096)
            if not data:
                print("Conexão fechada")
                break

            messages = data.decode("utf-8", errors="ignore").split("\r\n")
            for msg in messages:
                if not msg:
                    continue

                print(f"RAW: {msg}")

                if msg.startswith("PING"):
                    self.writer.write(b"PONG :tmi.twitch.tv\r\n")
                    await self.writer.drain()
                    print("PONG enviado")
                    continue

                match = re.search(r":(\w+)!.*PRIVMSG #(\w+) :(.+)", msg)
                if match:
                    author = match.group(1)
                    channel = match.group(2)
                    content = match.group(3)

                    print(f"Mensagem detectada - {author}: {content}")
                    await self.send_to_discord(author, content)

    async def close(self):
        if self.writer:
            self.writer.close()
            await self.writer.wait_closed()

async def main():
    while True:
        try:
            logger = TwitchChatLogger()
            await logger.run()
        except Exception as e:
            print(f"Erro: {e}. Reconectando em 10 segundos...")
            await asyncio.sleep(10)
        except KeyboardInterrupt:
            break

if __name__ == "__main__":
    asyncio.run(main())
