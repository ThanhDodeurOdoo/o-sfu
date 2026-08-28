# S4. Separate intent from realization

Keep requested intent separate from the resource that realizes it. Resource
loss, replacement or renegotiation must not erase intent. Only its owning
domain operation may remove it.

Give each pending realization an identity. Accept completion only when that
identity matches the pending realization for the current request.

> [!NOTE]
> More read: **[the Kubernetes controller pattern](https://kubernetes.io/docs/concepts/architecture/controller/)** and **[optimistic concurrency control in Google Cloud](https://docs.cloud.google.com/java/docs/occ)**.

**Example:** `RouteGraph` keeps `Subscription::intent` when its current
publication detaches. `ConsumerRealization::Pending` carries a
`RouteReservationId` that rejects stale setup completion.

**Avoid**

```rust
struct Subscription {
    // Detaching the current publication would erase receiver intent.
    current: Option<CurrentPublication>,
}
```

**Prefer**

```rust
struct Subscription {
    // Intent survives publication detach or receiver replacement.
    intent: SourceSubscriptionIntent,
    current: Option<CurrentPublication>,
}

struct CurrentPublication {
    source_id: PublishedSourceId,
    selection: ConsumerSourceSelection,
    realization: ConsumerRealization,
}

#[derive(Debug, Default)]
enum ConsumerRealization {
    #[default]
    Absent,
    // The reservation ID rejects stale completions from older setup attempts.
    Pending(RouteReservationId, Option<RouteRelay>),
    Committed(
        TransportConsumerRoute,
        String,
        RoutedConsumerId,
        Option<RouteRelay>,
    ),
}
```

**Rationale:** A temporary resource can disappear while the request remains.
Stale completions must not modify or erase its replacement.
