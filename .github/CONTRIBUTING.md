# Contributing

If you want to make a PR that does substantial changes to the codebase, before wasting time writing too much code:

**You work at Odoo**: use our internal means of communication to reach me first.

**You are an external contributor**: open an [issue](https://github.com/ThanhDodeurOdoo/o-sfu/issues) to talk about it and to defend your idea first.

> [!WARNING]
> AI policy
>
> Trivial changes are allowed (rewording docstring, basic autocompletion,...)
>
> Non-Trivial changes written by AI must have the `AI` tag on the PR.
>
> The author must always understand all the added code and can justify the changes (replying with copy-pasted AI responses does not count).

## Learning resources

- [The Rust Book](https://doc.rust-lang.org/book/)
- [The Rustonomicon (unsafe/advanced)](https://doc.rust-lang.org/nomicon/)
- [Rust by Example](https://doc.rust-lang.org/rust-by-example/)
- [Rust Cookbook](https://github.com/rust-lang-nursery/rust-cookbook/)
- [Idiomatic Rust snippets](https://idiomatic-rust-snippets.org/)
- ["The Rust Programming Language" by Aaron Turon (video)](https://youtu.be/O5vzLKg7y-k)
- ["Living with Rust Long-Term" by Jon Gjengset (video)](https://youtu.be/r35cBkPRNMI)
- ["Rust makes cents" by No Boilerplate (video)](https://www.youtube.com/watch?v=4dvf6kM70qM)

## Style guidelines

### General Rules
(some of the rules are enforced by lint like clippy)

- **No Low-Value Comments**: Avoid trivial comments that describe obvious code or that is just a rephrase of a function or variable name. Only write comments for necessary complex logic or obscure implementation / standard docstring / header comment (for files that need a global explanation).
- **Justify Overrides**: Any override of a linter rule MUST be justified with a descriptive comment.
- **Avoid literals**: use constants or enums with a meaningful name instead. Magic numbers and strings, for example from RFCs have their dedicated rfc crate.
- **Document unhandled errors**: Errors that are thrown, or `Result` types in Rust, must have their errors documented.
- **Tests**: Every new feature must include corresponding tests (meaningful tests, not noisy trivial checks) / proof / fuzzing / ... (depending on the changes).
- Failing the performance CI isn't necessarily breaking, but the commit message should include a `performance` section that justifies why.

### Rust

- **Formatting**: `cargo +nightly fmt`, Always run it before committing (we use nightly for the import ordering). Our rules can be found at [rustfmt.toml](../rustfmt.toml), more information on the defaults can be found at the [rustfmt documentation](https://rust-lang.github.io/rustfmt/).
- **Linting**: `cargo clippy --workspace --all-targets --all-features -- -D warnings`, We use Clippy with strict rules. The enforced rules can be found in [Cargo.toml](../Cargo.toml), see the [Clippy documentation](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html) for explanations.
- **Justify overrides**: Any override of a rule MUST be justified with a "reason".

### TypeScript & JavaScript (Bundle)

- **No lazy typing**: The use of the `any` type is strictly forbidden. Use proper interfaces or types.
- **No double Assertions**: Avoid `as unknown as`. If you must use it, provide a justifying comment (it id jusifiable when the type is really unknown (eg: external API)).
- **Defined Assertions**: Use the `!` operator only when you are absolutely certain the value is neither `null` nor `undefined`. It may require a comment.
- **Enforce immutability**: When possible, enforce immutability (`as const` / `readonly`).

## Tooling

if running rust-analyzer and on an OS that is not linux, I recommend adding `"rust-analyzer.cargo.target": "x86_64-unknown-linux-gnu"` to your settngs. 

## Verification

Verification commands and the `tests/` layout are at [tests/README.md](https://github.com/ThanhDodeurOdoo/o-sfu/blob/master/tests/README.md).

## Running the server

TODO: will write a dedicated md doc later

Same general idea than odoo/sfu

The `otel-tracing` cargo feature is enabled by default. Disable it with
`--no-default-features` when you want a logging-only build that does not compile
the OpenTelemetry exporter stack.


To generate a key, a 32 bytes long crypto-safe base64 string is recomended, eg:
(it must be the same for odoo's `ODOO_SFU_KEY`)

```bash
openssl rand -base64 32
```

the bind address is the address that listens for HTTP and WebSocket, when testing
it should be the same as `ODOO_SFU_URL` in odoo.

> [!WARNING]  
> `PUBLIC_IP` shouldn't be a localhost loopback, it should an actual eternally visible IP,
> at least your local IP, and on production, your server's public IP.
>

```bash
AUTH_KEY="" \
PUBLIC_IP=192.168.1.99 \
BIND_ADDRESS=127.0.0.1:8070 \
RTC_MIN_PORT=40000 \
RTC_MAX_PORT=40031 \
cargo run --release -p o-sfu
```

I also recomend using this worker config if you're not testing a specific spillover mode,
it distributes users between all workers which is useful for testing the cross worker relay.
```
RTC_MEDIA_WORKER_COUNT=4
ROOM_MAX_LOCAL_ROUTERS=4
ROOM_SPILLOVER_MODE=bounded
```


the command above do: the HTTP and WebSocket listener on `BIND_ADDRESS` and uses the
configured UDP range for RTC traffic. 

use `PROXY=false` for direct-exposed development. 
uet `PROXY=true` only when `o-sfu` sits behind a trusted reverse
proxy that overwrites `x-forwarded-*` headers before forwarding requests.

For reverse-proxy deployments, keep this in mind:
(full deployment guide at [deployment.md](https://github.com/ThanhDodeurOdoo/o-sfu/blob/master/DEPLOYMENT.md))

- expose the TCP listener at `BIND_ADDRESS` for HTTP and WebSocket traffic
- expose the full UDP range from `RTC_MIN_PORT` through `RTC_MAX_PORT`
- do not put media UDP traffic through NGINX;
- `PUBLIC_IP` is the externally visible IP, it will be used by RTC to connect

example with doker container:

```bash
docker build --tag o-sfu:local .

docker run --rm \
  -e AUTH_KEY="$(openssl rand -base64 32)" \
  -e PUBLIC_IP=203.0.113.10 \
  -e PROXY=true \
  -e RTC_MIN_PORT=40000 \
  -e RTC_MAX_PORT=40031 \
  -p 8070:8070 \
  -p 40000-40031:40000-40031/udp \
  o-sfu:local
```

## testing with container image

Build the server image from the repository root with:

```bash
docker build --tag o-sfu:local .
```

Run a local container by providing the auth key, the advertised RTC IP (not a loopback, your local IP), and the UDP range:

```bash
docker run --rm \
  -p 8080:8080 \
  -p 40000-49999:40000-49999/udp \
  -e AUTH_KEY=dev-secret \
  -e PROXY=true \
  -e PUBLIC_IP=203.0.113.10 \
  o-sfu:local
```
