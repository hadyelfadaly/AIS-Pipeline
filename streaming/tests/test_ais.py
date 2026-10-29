import asyncio, json
from pathlib import Path
import websockets
 
KEY = "ad6069147eb4e7f5265838262cc4ab418b90c5f9"
SUB = {
    "APIKey": KEY,
    "BoundingBoxes": [[[-90, -180], [90, 180]]],
    "\\FilterMessageTypes": ["PositionReport", "ShipStaticData"],
}
 
OUT = Path(__file__).parent / "ais_dump.jsonl"
OUT.parent.mkdir(parents=True, exist_ok=True)
 
 
async def run():
    delay = 1
    with open(OUT, "a", encoding="utf-8") as f:
        while True:
            try:
                async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
                    await ws.send(json.dumps(SUB))
                    delay = 1
                    async for raw in ws:
                        if isinstance(raw, bytes):
                            raw = raw.decode("utf-8")
                        msg = json.loads(raw)
                        f.write(raw + "\n")
                        f.flush()
                        md = msg.get("MetaData", {})
                        print(
                            msg.get("MessageType"),
                            md.get("MMSI"),
                            str(md.get("ShipName", "")).strip(),
                            md.get("time_utc"),
                        )
            except websockets.ConnectionClosed as e:
                print("connection closed:", e)
            except OSError as e:
                print("network error:", e)
            await asyncio.sleep(delay)
            delay = min(delay * 2, 60)
 
 
try:
    asyncio.run(run())
except KeyboardInterrupt:
    print("stopped")