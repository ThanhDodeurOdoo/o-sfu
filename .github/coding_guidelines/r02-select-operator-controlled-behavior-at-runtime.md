# R2. Configure operator choices at runtime

Operator-controlled codecs, media policy and limits use runtime configuration,
not Cargo features. Define variables under
[`src/config`](../../src/config/), load them through `Env::var` in
`Config::from_env` and document them in [`DEPLOYMENT.md`](../../DEPLOYMENT.md).
Validate defaults and supplied values. Reject unsupported, conflicting or
retired settings. Renames need precedence or mutual rejection.

Each Cargo feature must add optional infrastructure or verification capability.
Document and test each feature combination enabled by CI or deployment
manifests because Cargo can unify features across dependencies. See the
[Cargo feature-unification guidance](https://doc.rust-lang.org/cargo/reference/features.html#feature-unification).

**Example:** `load_media_codec_flags` loads each codec choice through `Env`.

**Avoid**

```rust
// A Cargo feature bakes operator policy into the binary.
#[cfg(feature = "h264")]
const H264_ENABLED: bool = true;
```

**Prefer**

```rust
// Env validates the deployment-specific choice at startup.
.with_h264(env.var("CODEC_H264").default(defaults.h264_enabled())?)
```

**Rationale:** Runtime configuration keeps operator choices validated and lets
one build serve different deployments.
