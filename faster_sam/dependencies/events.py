import base64
import hashlib
import json
from datetime import datetime, timezone
from typing import Any, Dict
from uuid import uuid4

from fastapi import Request

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


async def sqs(request: Request) -> Dict[str, Any]:
    data = await request.body()
    body = json.loads(data)

    publish_time = datetime.strptime(body["message"]["publishTime"], "%Y-%m-%dT%H:%M:%S.%fZ")
    publish_time *= KILO_SECONDS

    attributes = {
        "ApproximateReceiveCount": body["deliveryAttempt"],
        "SentTimestamp": datetime.timestamp(publish_time),
        "SenderId": "",
        "ApproximateFirstReceiveTimestamp": "",
    }

    event = {
        "Records": [
            {
                "messageId": body["message"]["messageId"],
                "receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
                "body": base64.b64decode(body["message"]["data"]).decode("UTF-8"),
                "attributes": attributes,
                "messageAttributes": body["message"].get("attributes", {}),
                "md5OfBody": hashlib.md5(data).hexdigest(),
                "eventSource": "aws:sqs",
                "eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
                "awsRegion": "us-east-2",
            },
        ]
    }

    return event
