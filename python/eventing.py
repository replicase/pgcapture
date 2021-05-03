import sys
import grpc
import asyncio

from pgcapture_pb2 import CaptureInit, CaptureAck, CaptureRequest
from pgcapture_pb2_grpc import DBLogGatewayStub
from google.protobuf.struct_pb2 import Struct

class PGCapture:

    def __init__(self, grpc_addr, uri, **kwargs):
        params = Struct()
        params.update(kwargs)

        self._grpc_addr = grpc_addr
        self._init = CaptureRequest(init=CaptureInit(uri=uri, parameters=params))

    async def capture(self, handler):
        async with grpc.aio.insecure_channel(self._grpc_addr) as channel:
            stub = DBLogGatewayStub(channel)
            stream = stub.Capture()

            await stream.write(self._init)
            async for msg in stream:
                ack = CaptureAck(checkpoint=msg.checkpoint)
                try:
                    handler(msg)
                except Exception as e:
                    ack.requeue_reason=str(e)
                    print(ack.requeue_reason, file=sys.stderr)

                await stream.write(CaptureRequest(ack=ack))
