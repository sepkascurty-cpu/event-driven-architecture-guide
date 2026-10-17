# Event-Driven Architecture Quiz

5 questions. Answers are collapsed — guess first, then click.

---

**Q1.** Your Lambda consumer is triggered by a standard SQS queue. CloudWatch shows the same `OrderCreated` event processed twice — and the customer was charged twice. What broke, and what is the minimal fix?

<details>
<summary>Answer</summary>

Nothing "broke": standard SQS promises at-least-once delivery, so duplicates are the contract, not a bug. The minimal fix is an idempotent consumer — key the side effect on `eventId` and claim it with a DynamoDB conditional write (`ConditionExpression="attribute_not_exists(event_id)"`) *before* charging. The second delivery loses the race on the conditional write and becomes a silent no-op — exactly what `claim()` in `order_processor.py` does.
</details>

---

**Q2.** Payments must be processed in order per customer, but welcome emails don't care about order. Which queue type do you choose for each — and why not use the strict one for both?

<details>
<summary>Answer</summary>

Payments → SQS FIFO with `MessageGroupId = customerId`: strict per-group ordering. Emails → standard queue: higher throughput, cheaper. Don't use FIFO for everything: FIFO caps throughput per message group (~300 msg/s without batching) and costs more. Ordering is a constraint you pay for — spend it only where correctness demands it.
</details>

---

**Q3.** One malformed message keeps failing inside your consumer. Left alone, it retries forever and burns Lambda invocations. Which two SQS mechanisms contain it — and what do you monitor?

<details>
<summary>Answer</summary>

(1) A **redrive policy** with `maxReceiveCount` (e.g. 5) so the message is moved to a dead-letter queue instead of retrying forever. (2) The **DLQ itself** as the parking lot — fix the bug or the schema, then redrive the DLQ back to the source queue. Monitor `ApproximateNumberOfMessagesVisible` on the DLQ with a CloudWatch alarm: any depth above zero means something needs attention.
</details>

---

**Q4.** Your checkout flow is: reserve inventory → charge card → send confirmation. If charging fails, the reservation must be released. Do you choreograph this with events or orchestrate it — and why?

<details>
<summary>Answer</summary>

**Orchestrate it** — with Step Functions driving a saga. This flow needs explicit rollback / compensation (release the reservation when the charge fails) and a single place to see where a checkout got stuck. Pure choreography ("each service reacts to events") makes compensating actions and debugging painful: a missed event silently stalls the flow with no central execution history. Rule of thumb from the guide: choreograph the happy path, orchestrate the money path.
</details>

---

**Q5.** A downstream service can only handle 10 requests/second, but your event bus can deliver thousands. Where do you put the brake — and which two settings act as the brake pads?

<details>
<summary>Answer</summary>

Put the brake at the **Lambda event source mapping**, not in your code: set **reserved concurrency** on the consumer Lambda (caps parallel executions, which caps downstream RPS) and tune **batch size / batching window** to smooth bursts into fewer, larger invocations. Reserved concurrency doubles as a circuit breaker — when the downstream degrades, throttled invocations retry via the queue instead of hammering it.
</details>

---

Scored 5/5? Now go lower a DLQ alarm threshold and see who notices. (Kidding. Mostly.) ⚡
