import json
import struct
from datetime import date, datetime, time, timedelta
from decimal import Decimal
from typing import Any, Callable, Dict, Tuple, cast
from uuid import UUID

_UnpackInt = Callable[[bytes], Tuple[int]]
_UnpackFloat = Callable[[bytes], Tuple[float]]

_unpack_int2 = cast(_UnpackInt, struct.Struct("!h").unpack)
_unpack_int4 = cast(_UnpackInt, struct.Struct("!i").unpack)
_unpack_uint4 = cast(_UnpackInt, struct.Struct("!I").unpack)
_unpack_int8 = cast(_UnpackInt, struct.Struct("!q").unpack)
_unpack_float4 = cast(_UnpackFloat, struct.Struct("!f").unpack)
_unpack_float8 = cast(_UnpackFloat, struct.Struct("!d").unpack)
microsecFromUnixEpochToY2K = 946684800 * 1000000

_struct_head = struct.Struct("!III")
_struct_dim = struct.Struct("!II")
_struct_len = struct.Struct("!i")

def BoolDecoder(data):
    return data != b"\x00"

def ByteaDecoder(data):
    return data

def Int8Decoder(data):
    return _unpack_int8(data)[0]

def Int2Decoder(data):
    return _unpack_int2(data)[0]

def Int4Decoder(data):
    return _unpack_int4(data)[0]

def TextDecoder(data):
    return str(data, 'utf8')

def JSONDecoder(data):
    return json.loads(str(data, 'utf8'))

def Float4Decoder(data):
    return _unpack_float4(data)[0]

def Float8Decoder(data):
    return _unpack_float8(data)[0]

def UnknownDecoder(data):
    return str(data, 'utf8')

def BoolArrayDecoder(data):
    return decodeArray(data, BoolDecoder)

def Int2ArrayDecoder(data):
    return decodeArray(data, Int2Decoder)

def Int4ArrayDecoder(data):
    return decodeArray(data, Int4Decoder)

def TextArrayDecoder(data):
    return decodeArray(data, TextDecoder)

def ByteaArrayDecoder(data):
    return decodeArray(data, ByteaDecoder)

def BPCharArrayDecoder(data):
    return decodeArray(data, BPCharDecoder)

def VarcharArrayDecoder(data):
    return decodeArray(data, VarcharDecoder)

def Int8ArrayDecoder(data):
    return decodeArray(data, Int8Decoder)

def Float4ArrayDecoder(data):
    return decodeArray(data, Float4Decoder)

def Float8ArrayDecoder(data):
    return decodeArray(data, Float8Decoder)

def BPCharDecoder(data):
    return str(data, 'utf8')

def VarcharDecoder(data):
    return str(data, 'utf8')

def DateDecoder(data):
    offset = _unpack_int4(data)[0]
    return date(2000, 1, 1) + timedelta(offset)

def TimeDecoder(data):
    micros = _unpack_int8(data)[0]
    return time.min + timedelta(0, 0, micros)

def TimestampDecoder(data):
    microsecSinceY2K = _unpack_int8(data)[0]
    microsecSinceUnixEpoch = microsecFromUnixEpochToY2K + microsecSinceY2K
    return datetime.utcfromtimestamp(microsecSinceUnixEpoch/1000000) + timedelta(microseconds=microsecSinceUnixEpoch%1000000)

def TimestampArrayDecoder(data):
    return decodeArray(data, TimestampDecoder)

def DateArrayDecoder(data):
    return decodeArray(data, DateDecoder)

def TimestamptzDecoder(data):
    return TimestampDecoder(data)

def TimestamptzArrayDecoder(data):
    return decodeArray(data, TimestamptzDecoder)

def UUIDDecoder(data):
    return UUID(bytes=data)

def UUIDArrayDecoder(data):
    return decodeArray(data, UUIDDecoder)

def JSONBDecoder(data):
    return json.loads(str(data[1:], 'utf8'))

def JSONBArrayDecoder(data):
    return decodeArray(data, JSONBDecoder)

def decodeArray(data, callback):
    ndims, hasnull, oid = _struct_head.unpack_from(data[:12])
    if not ndims:
        return []

    p = 12 + 8 * ndims
    dims = [
        _struct_dim.unpack_from(data, i)[0] for i in list(range(12, p, 8))
    ]

    def consume(p):
        while 1:
            size = _struct_len.unpack_from(data, p)[0]
            p += 4
            if size != -1:
                yield callback(data[p : p + size])
                p += size
            else:
                yield None

    items = consume(p)

    def agg(dims):
        if not dims:
            return next(items)
        else:
            dim, dims = dims[0], dims[1:]
            return [agg(dims) for _ in range(dim)]

    return agg(dims)

OIDRegistery = {
    16: BoolDecoder,
	17: ByteaDecoder,
	# 18: QCharDecoder,
	# 19: NameDecoder,
	20: Int8Decoder,
	21: Int2Decoder,
	23: Int4Decoder,
	25: TextDecoder,
	# 26: OIDDecoder,
	# 27: TIDDecoder,
	# 28: XIDDecoder,
	# 29: CIDDecoder,
	114: JSONDecoder,
	# 600: PointDecoder,
	# 601: LsegDecoder,
	# 602: PathDecoder,
	# 603: BoxDecoder,
	# 604: PolygonDecoder,
	# 628: LineDecoder,
	# 650: CIDRDecoder,
	# 651: CIDRArrayDecoder,
	700: Float4Decoder,
	701: Float8Decoder,
	# 718: CircleDecoder,
	705: UnknownDecoder,
	# 829: MacaddrDecoder,
	# 869: InetDecoder,
	1000: BoolArrayDecoder,
	1005: Int2ArrayDecoder,
	1007: Int4ArrayDecoder,
	1009: TextArrayDecoder,
	1001: ByteaArrayDecoder,
	1014: BPCharArrayDecoder,
	1015: VarcharArrayDecoder,
	1016: Int8ArrayDecoder,
	1021: Float4ArrayDecoder,
	1022: Float8ArrayDecoder,
	# 1033: ACLItemDecoder,
	# 1034: ACLItemArrayDecoder,
	# 1041: InetArrayDecoder,
	1042: BPCharDecoder,
	1043: VarcharDecoder,
	1082: DateDecoder,
	1083: TimeDecoder,
	1114: TimestampDecoder,
	1115: TimestampArrayDecoder,
	1182: DateArrayDecoder,
	1184: TimestamptzDecoder,
	1185: TimestamptzArrayDecoder,
	# 1186: IntervalDecoder,
	# 1231: NumericArrayDecoder,
	# 1560: BitDecoder,
	# 1562: VarbitDecoder,
	# 1700: NumericDecoder,
	# 2249: RecordDecoder,
	2950: UUIDDecoder,
	2951: UUIDArrayDecoder,
	3802: JSONBDecoder,
	3807: JSONBArrayDecoder,
	# 3912: DaterangeDecoder,
	# 3904: Int4rangeDecoder,
	# 3906: NumrangeDecoder,
	# 3908: TsrangeDecoder,
	# 3909: TsrangeArrayDecoder,
	# 3910: TstzrangeDecoder,
	# 3911: TstzrangeArrayDecoder,
	# 3926: Int8rangeDecoder,
}
