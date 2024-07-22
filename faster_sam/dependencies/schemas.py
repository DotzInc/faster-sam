from datetime import datetime
from pydantic import BaseModel, Base64UrlStr, Field


class SQSInfo(BaseModel):
    id: str
    body: str
    receive_count: int
    sent_timestamp: int
    message_attributes: dict[str, str] | None
    source_arn: str


class PubSubMessage(BaseModel):
    data: Base64UrlStr
    attributes: dict[str, str] | None = Field(default=None)
    messageId: str
    publishTime: datetime


class PubSubEnvelope(BaseModel):
    message: PubSubMessage
    subscription: str
    deliveryAttempt: int

    def into(self) -> SQSInfo:
        milliseconds = 1000

        publish_time = int(self.message.publishTime.timestamp() * milliseconds)

        topic_name = self.subscription.rsplit("/", maxsplit=1)[-1]
        source_arn = f"arn:aws:sqs:::{topic_name}"
        return SQSInfo(
            id=self.message.messageId,
            body=self.message.data,
            receive_count=self.deliveryAttempt,
            sent_timestamp=publish_time,
            message_attributes=self.message.attributes,
            source_arn=source_arn,
        )
