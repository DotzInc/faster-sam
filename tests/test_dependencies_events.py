from datetime import datetime
import hashlib
import json
import unittest

from fastapi import FastAPI, Request

from faster_sam.dependencies import events


def build_apigateway_request():
    async def receive():
        return {"type": "http.request", "body": b'{"message": "pong"}'}

    scope = {
        "type": "http",
        "http_version": "1.1",
        "root_path": "",
        "path": "/ping/pong",
        "method": "GET",
        "query_string": b"q=all&skip=100",
        "path_params": {"message": "pong"},
        "client": ("127.0.0.1", 80),
        "app": FastAPI(),
        "headers": [
            (b"content-type", b"application/json"),
            (b"user-agent", b"python/unittest"),
        ],
    }

    return Request(scope, receive)


def build_sqs_request():
    async def receive():
        body = {
            "deliveryAttempt": 1,
            "message": {
                "attributes": {"foo": "bar"},
                "data": "aGVsbG8=",
                "messageId": "10519041647717348",
                "message_id": "10519041647717348",
                "publishTime": "2024-02-22T15:45:31.346Z",
                "publish_time": "2024-02-22T15:45:31.346Z",
            },
            "subscription": "projects/foo/subscriptions/bar",
        }
        return {"type": "http.request", "body": json.dumps(body).encode()}

    scope = {
        "type": "http",
        "http_version": "1.1",
        "root_path": "",
        "path": "/ping/pong",
        "method": "GET",
        "query_string": [],
        "path_params": {},
        "client": ("127.0.0.1", 80),
        "app": FastAPI(),
    }

    return Request(scope, receive)


class TestApiGatewayProxy(unittest.IsolatedAsyncioTestCase):
    async def test_event(self):
        request = build_apigateway_request()
        event = await events.apigateway_proxy(request)

        self.assertIsInstance(event, dict)
        self.assertEqual(event["body"], '{"message": "pong"}')
        self.assertEqual(event["path"], "/ping/pong")
        self.assertEqual(event["httpMethod"], "GET")
        self.assertEqual(event["isBase64Encoded"], False)
        self.assertEqual(event["queryStringParameters"], {"q": "all", "skip": "100"})
        self.assertEqual(event["pathParameters"], {"message": "pong"})
        self.assertEqual(
            event["headers"],
            {"content-type": "application/json", "user-agent": "python/unittest"},
        )
        self.assertEqual(event["requestContext"]["stage"], "0.1.0")
        self.assertEqual(event["requestContext"]["identity"]["sourceIp"], "127.0.0.1")
        self.assertEqual(event["requestContext"]["identity"]["userAgent"], "python/unittest")
        self.assertEqual(event["requestContext"]["path"], "/ping/pong")
        self.assertEqual(event["requestContext"]["httpMethod"], "GET")
        self.assertEqual(event["requestContext"]["protocol"], "HTTP/1.1")

    async def test_sqs_event(self):
        request = build_sqs_request()
        event = await events.sqs_event(request)

        self.assertIsInstance(event, dict)
        record = event["Records"][0]
        self.assertEqual(record["messageId"], "10519041647717348")
        self.assertEqual(record["body"], "hello")
        self.assertEqual(record["attributes"]["ApproximateReceiveCount"], 1)
        self.assertEqual(
            record["attributes"]["SentTimestamp"],
            int(
                datetime.strptime("2024-02-22T15:45:31.346Z", "%Y-%m-%dT%H:%M:%S.%fZ").timestamp()
                * events.KILO_SECONDS
            ),
        )
        self.assertEqual(record["attributes"]["SenderId"], "")
        self.assertEqual(record["attributes"]["ApproximateFirstReceiveTimestamp"], "")
        self.assertEqual(record["messageAttributes"], {"foo": "bar"})
        self.assertEqual(
            record["md5OfBody"],
            hashlib.md5(
                json.dumps(
                    {
                        "deliveryAttempt": 1,
                        "message": {
                            "attributes": {"foo": "bar"},
                            "data": "aGVsbG8=",
                            "messageId": "10519041647717348",
                            "message_id": "10519041647717348",
                            "publishTime": "2024-02-22T15:45:31.346Z",
                            "publish_time": "2024-02-22T15:45:31.346Z",
                        },
                        "subscription": "projects/foo/subscriptions/bar",
                    }
                ).encode("utf-8")
            ).hexdigest(),
        )
        self.assertEqual(record["eventSource"], "aws:sqs")
        self.assertEqual(record["eventSourceARN"], "arn:aws:sqs:us-east-2:123456789012:my-queue")
        self.assertEqual(record["awsRegion"], "us-east-2")
