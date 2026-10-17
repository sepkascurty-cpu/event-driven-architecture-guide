#!/usr/bin/env python3
"""
order_processor.py -- Event-driven order pipeline sample.

Producer : publishes OrderCreated events to an EventBridge bus.
Consumer : Lambda-style handler for SQS messages. Idempotent by construction
           (a DynamoDB conditional write claims each eventId exactly once) and
           tolerant of poison messages (SQS redrive policy -> DLQ).

Run the local demo (no AWS credentials needed):
    python order_processor.py
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass, field

EVENT_TYPE = "OrderCreated"
EVENT_VERSION = "v1"
BUS_NAME = "orders-bus"
SOURCE = "shop.orders"
MAX_RECEIVES = 5  # SQS redrive policy: after 5 failed receives -> DLQ
IDEMPOTENCY_TABLE = os.environ.get("IDEMPOTENCY_TABLE", "order-events")


# ------------------------------------------------------------------ producers

def build_order_event(order_id: str, customer_id: str, total_cents: int) -> dict:
    """An event is a fact about the past: past-tense name, no 'please' inside."""
    event_id = hashlib.sha256(f"{order_id}:{EVENT_VERSION}".encode()).hexdigest()[:32]
    return {
        "Source": SOURCE,
        "DetailType": f"{EVENT_TYPE}.{EVENT_VERSION}",
        "EventBusName": BUS_NAME,
        "Detail": json.dumps({
            "event_id": event_id,  # <-- the idempotency key. Never reuse it.
            "event_type": EVENT_TYPE,
            "event_version": EVENT_VERSION,
            "occurred_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "order": {
                "order_id": order_id,
                "customer_id": customer_id,
                "total_cents": total_cents,
            },
        }),
    }


def publish_order_created(order_id: str, customer_id: str, total_cents: int) -> dict:
    """Emit the fact. We don't know -- or care -- who reacts to it."""
    import boto3  # lazy import: the local demo runs without boto3

    entry = build_order_event(order_id, customer_id, total_cents)
    resp = boto3.client("events").put_events(Entries=[entry])
    if resp.get("FailedEntryCount", 0):
        raise RuntimeError(f"put_events failed entries: {resp}")
    return resp


# ------------------------------------------- idempotency (the important part)

class InMemoryIdempotencyStore:
    """Local stand-in with the same atomic claim() semantics as DynamoDB."""

    def __init__(self) -> None:
        self._claimed: set[str] = set()

    def claim(self, event_id: str) -> bool:
        """Return True only for the FIRST claim of event_id (atomic)."""
        if event_id in self._claimed:
            return False
        self._claimed.add(event_id)
        return True


class DynamoIdempotencyStore:
    """Production store: the conditional write IS the idempotency check.

    put_item(ConditionExpression="attribute_not_exists(event_id)") makes the
    check-and-set atomic -- two concurrent Lambdas racing on the same event
    can't both win. No lock, no transaction, no race window.
    """

    def __init__(self, table_name: str = IDEMPOTENCY_TABLE) -> None:
        import boto3

        self._table = boto3.resource("dynamodb").Table(table_name)
        self._conflict = self._table.meta.client.exceptions.ConditionalCheckFailedException

    def claim(self, event_id: str) -> bool:
        try:
            self._table.put_item(
                Item={
                    "event_id": event_id,
                    "claimed_at": int(time.time()),
                    # auto-expire: keeps the table small, bounds replay windows
                    "ttl": int(time.time()) + 7 * 24 * 3600,
                },
                ConditionExpression="attribute_not_exists(event_id)",
            )
            return True
        except self._conflict:
            return False


# ------------------------------------------------------------------ consumers

@dataclass
class DemoQueues:
    """Tiny in-memory model of SQS + DLQ for the local demo."""
    main: list = field(default_factory=list)
    dlq: list = field(default_factory=list)
    attempts: dict = field(default_factory=dict)

    def receive(self, message: dict) -> None:
        key = message["event_id"]
        self.attempts[key] = self.attempts.get(key, 0) + 1
        if self.attempts[key] > MAX_RECEIVES:
            self.dlq.append(message)  # the redrive policy, minus the AWS
            print(f"  [sqs] {key[:8]}... failed {MAX_RECEIVES}x -> DLQ")
        else:
            self.main.append(message)


def fulfill_order(order: dict) -> None:
    """The real side effect. Runs at most once per event_id -- guaranteed by claim()."""
    print(f"  [consumer] fulfilling order {order['order_id']} "
          f"for {order['customer_id']} (${order['total_cents'] / 100:.2f})")


def handle_once(detail: dict, store) -> str:
    """Process one event exactly once, effect-wise. Returns 'processed' | 'duplicate'."""
    event_id = detail["event_id"]
    if not store.claim(event_id):
        return "duplicate"  # at-least-once delivery struck: skip silently
    fulfill_order(detail["order"])
    return "processed"


def extract_detail(record: dict) -> dict:
    """Unwrap the EventBridge -> SQS envelope (the SQS body holds the bus event)."""
    body = json.loads(record["body"])
    return json.loads(body["Detail"]) if "Detail" in body else body


def lambda_handler(event: dict, context, _store=None) -> dict:
    """AWS Lambda entrypoint (SQS event source mapping).

    Partial batch failures are omitted for clarity -- in production, return
    batchItemFailures so only failed messages are retried, not the whole batch.
    """
    store = _store or DynamoIdempotencyStore()
    for record in event.get("Records", []):
        handle_once(extract_detail(record), store)
    return {"statusCode": 200}


# ----------------------------------------------------------------- local demo

def demo() -> None:
    print("== Event-driven order pipeline -- local demo ==\n")

    store = InMemoryIdempotencyStore()
    queues = DemoQueues()

    # 1. Producer emits a fact.
    entry = build_order_event("ord-123", "cust-42", 49900)
    detail = json.loads(entry["Detail"])
    print(f"1. producer published {EVENT_TYPE}.{EVENT_VERSION} "
          f"(event_id={detail['event_id'][:8]}...)")

    # 2. At-least-once delivery: the SAME message arrives twice.
    print("2. SQS delivers the message... twice (at-least-once is the contract):")
    for i in (1, 2):
        print(f"   delivery {i}: {handle_once(detail, store)}")

    # 3. Poison message: fails every attempt -> redrive policy parks it in the DLQ.
    print(f"3. poison message (fails every attempt, MAX_RECEIVES={MAX_RECEIVES}):")
    poison = {"event_id": "poison-1",
              "order": {"order_id": "bad", "customer_id": "x", "total_cents": 0}}
    for _ in range(MAX_RECEIVES + 1):
        queues.receive(poison)
    print(f"   DLQ depth: {len(queues.dlq)} (alarm on this in CloudWatch!)\n")

    print("Demo done: duplicates skipped, poison parked. "
          "That's the whole reliability story.")


if __name__ == "__main__":
    demo()
