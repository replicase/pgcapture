import asyncio
from pgcapture import PGCaptureClient

def dump(msg):
    print(str(msg['change']))

if __name__ == '__main__':
    client = PGCaptureClient('127.0.0.1:10000', 'database|subscription', TableRegex='.*')
    asyncio.run(client.capture(dump))
