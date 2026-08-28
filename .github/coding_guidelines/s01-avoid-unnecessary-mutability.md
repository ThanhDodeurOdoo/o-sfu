# S1. Avoid unnecessary mutability

Prefer immutable values and finish construction before exposure. Limit
necessary mutation to the smallest scope. In **Rust** use `mut` only for
reassignment or mutable borrowing and compute final values directly. In
**TypeScript** prefer `const` and `readonly` outside mutable API contracts.
Borrow when use stays local. Clone only when an independent value or shared
ownership is required. Use `mem::take` or `mem::replace` to move without
cloning.

> [!NOTE]
> More read: **[aliasing in the Rustonomicon](https://doc.rust-lang.org/nomicon/aliasing.html)** and **[ownership and borrowing in The Rust Book](https://doc.rust-lang.org/book/ch04-01-what-is-ownership.html)**.
>
> partially enforced with: [needless_pass_by_ref_mut](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#needless_pass_by_ref_mut),
> [redundant_clone](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#redundant_clone),
> [complexity::clone_on_copy](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#clone_on_copy)
> and [style::unnecessary_mut_passed](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unnecessary_mut_passed).

**Example:** Return computed values from helper functions instead of passing a
mutable struct through multiple modification steps.

**Avoid**

```rust
// Passing `&mut` across helpers obscures which functions modify which fields.
fn setup_session(session: &mut Session, auth: &AuthPayload) {
    authenticate_user(session, auth);
    assign_permissions(session, auth);
}

fn authenticate_user(session: &mut Session, auth: &AuthPayload) {
    session.user_id = Some(auth.user_id);
    session.authenticated = true;
}
```

**Prefer**

```rust
// Functions take immutable inputs and return values for explicit construction.
fn create_session(auth: &AuthPayload) -> Session {
    let user_id = authenticate_user(auth);
    let permissions = evaluate_permissions(auth);
    Session::new(user_id, permissions)
}
```

**Rationale:** Immutable values prevent unintended writes. Keeping mutation
local makes state transitions and ownership easier to audit.
