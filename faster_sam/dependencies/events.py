import base64
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Protocol, Type, runtime_checkable
from uuid import uuid4
import uuid

from fastapi import Request
from pydantic import BaseModel

from faster_sam.dependencies.schemas import SQSInfo

KILO_SECONDS = 1000.0


async def apigateway_proxy(request: Request) -> Dict[str, Any]:
    now = datetime.now(timezone.utc)
    body = await request.body()
    event = {
        "body": body.decode(),
        "path": request.url.path,
        "httpMethod": request.method,
        "isBase64Encoded": False,
        "queryStringParameters": dict(request.query_params),
        "pathParameters": dict(request.path_params),
        "headers": dict(request.headers),
        "requestContext": {
            "stage": request.app.version,
            "requestId": str(uuid4()),
            "requestTime": now.strftime(r"%d/%b/%Y:%H:%M:%S %z"),
            "requestTimeEpoch": int(now.timestamp()),
            "identity": {
                "sourceIp": getattr(request.client, "host", None),
                "userAgent": request.headers.get("user-agent"),
            },
            "path": request.url.path,
            "httpMethod": request.method,
            "protocol": f"HTTP/{request.scope['http_version']}",
        },
    }
    return event


@runtime_checkable
class IntoSQSInfo(Protocol):
    def into(self) -> SQSInfo: ...


def sqs(schema: Type[BaseModel]) -> Callable[..., dict[str, Any]]:
    def dep(message: schema) -> dict[str, Any]:

        assert isinstance(message, IntoSQSInfo)

        info = message.into()

        event = {
            "Records": [
                {
                    "messageId": info.id,
                    "receiptHandle": str(uuid.uuid4()),
                    "body": info.body,
                    "attributes": {
                        "ApproximateReceiveCount": info.receive_count,
                        "SentTimestamp": info.sent_timestamp,
                        "SenderId": str(uuid.uuid4()),
                        "ApproximateFirstReceiveTimestamp": info.sent_timestamp,
                    },
                    "messageAttributes": info.message_attributes,
                    "md5OfBody": hashlib.md5(info.body.encode()).hexdigest(),
                    "eventSource": "aws:sqs",
                    "eventSourceARN": info.source_arn,
                    "awsRegion": None,
                },
            ]
        }

        return event

    return dep
