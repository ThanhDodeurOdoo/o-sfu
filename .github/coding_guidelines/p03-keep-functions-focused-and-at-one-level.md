# P3. Keep each function focused and at one level of detail

A function coordinates domain operations or implements one lower-level step.
Keep parsing, wire formatting, validation mechanics and collection details out
of coordination code. Use `?`, `let ... else` and early returns to avoid deep
nesting.

> [!NOTE]
> More read: **[Google's guidance on short functions](https://google.github.io/styleguide/cppguide.html#Write_Short_Functions)**.
>
> partially enforced with: [cognitive_complexity](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#cognitive_complexity),
> [complexity::excessive_nesting](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#excessive_nesting),
> [pedantic::manual_let_else](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#manual_let_else),
> [pedantic::too_many_lines](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#too_many_lines)
> and [style::question_mark](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#question_mark).

**Example:** `verify_auth_payload` coordinates room resolution and
authentication while `resolve_handshake_room` and
`authenticate_room_scoped_claims` own those lower-level steps.

**Avoid**

```rust
async fn verify_auth_payload(
    state: &WebSocketServices,
    auth_payload: &AuthPayload,
    remote_address: &str,
) -> Result<AuthenticatedJoin, WebSocketCloseCode> {
    // Unverified room lookup mechanics obscure the authenticated-join sequence.
    let room = if let Some(room_id) = auth_payload.channel.as_deref() {
        resolve_room_by_id(state, room_id).await?
    } else {
        let unverified = auth::decode_unverified_claims::<WebSocketConnectClaims>(
            &auth_payload.jwt,
        )
        .map_err(|_error| WebSocketCloseCode::AuthFailed)?;
        resolve_room_by_id(state, &unverified.room_id).await?
    };
    let (claims, proof) =
        authenticate_room_scoped_claims(&auth_payload.jwt, &room, remote_address)?;
    Ok(AuthenticatedJoin {
        room,
        claims,
        proof: WebSocketAuth(proof),
    })
}
```

**Prefer**

```rust
async fn verify_auth_payload(
    state: &WebSocketServices,
    auth_payload: &AuthPayload,
    remote_address: &str,
) -> Result<AuthenticatedJoin, WebSocketCloseCode> {
    // The coordinator stays at one level while helpers own lookup and verification.
    let room = resolve_handshake_room(state, auth_payload).await?;
    let (claims, proof) =
        authenticate_room_scoped_claims(&auth_payload.jwt, &room, remote_address)?;
    Ok(AuthenticatedJoin {
        room,
        claims,
        proof: WebSocketAuth(proof),
    })
}
```

**Rationale:** Operating at a single level of abstraction makes control flow and
business logic immediately clear.
