import sys
import grpc
import asyncio

from . decoders import OIDRegistery
from pb.pgcapture_pb2 import CaptureInit, CaptureAck, CaptureRequest
from pb.pgcapture_pb2_grpc import DBLogGatewayStub
from google.protobuf.struct_pb2 import Struct

class PGCaptureClient:

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
                    handler({
                        'checkpoint': msg.checkpoint,
                        'change': {
                            'op': msg.change.op,
                            'schema': msg.change.schema,
                            'table': msg.change.table,
                            'new': decode(msg.change.new),
                            'old': decode(msg.change.old),
                        }
                    })
                except Exception as e:
                    ack.requeue_reason=str(e)
                    print("requeue failed {}.{} record with error={}".format(msg.change.schema, msg.change.table, str(e)), file=sys.stderr)

                await stream.write(CaptureRequest(ack=ack))


def decode(fields):
    decoded = []
    for field in fields:
        if field.HasField('binary'):
            decode = OIDRegistery[field.oid]
            if not decode:
                print("unsupported oid '{}' on field '{}'".format(field.oid, field.name), file=sys.stderr)
                continue
            decoded.append({'name': field.name, 'oid': field.oid, 'value': decode(field.binary)})
        elif field.HasField("text"):
            decoded.append({'name': field.name, 'oid': field.oid, 'value': field.text})

    return decoded
