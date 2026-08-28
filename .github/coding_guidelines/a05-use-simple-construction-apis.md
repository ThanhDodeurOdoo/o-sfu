# A5. Use the simplest construction API

Constructors return valid values. `new` accepts only valid input, `try_new`
handles fallible validation and `Default` requires one unsurprising canonical
value. Builders suit many or optional arguments, accumulated compound input,
shared terminal-operation configuration or construction side effects, never a
small fixed input set.
See the Rust API Guidelines on [builders for complex
construction](https://rust-lang.github.io/api-guidelines/type-safety.html#builders-enable-construction-of-complex-values-c-builder).

Keep invariant-bearing fields private. Plain records may expose independent,
unconstrained fields.

> [!NOTE]
> partially enforced with: [pedantic::unnecessary_wraps](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#unnecessary_wraps),
> [style::new_ret_no_self](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#new_ret_no_self),
> [style::new_without_default](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#new_without_default)
> and [style::self_named_constructors](https://rust-lang.github.io/rust-clippy/rust-1.95.0/index.html#self_named_constructors).

**Example:** `RoomMediaLimits::try_new` validates required limits at construction
time instead of exposing unvalidated public fields.

**Avoid**

```rust
// Public fields allow callers to construct invalid states such as zero limits.
pub struct RoomMediaLimits {
    pub max_active_audio_speakers: usize,
    pub max_video_downloads_per_receiver: usize,
}
```

**Prefer**

```rust
pub struct RoomMediaLimits {
    max_active_audio_speakers: usize,
    max_video_downloads_per_receiver: usize,
}

impl RoomMediaLimits {
    // Successful construction proves both room media limits are non-zero.
    pub const fn try_new(
        max_active_audio_speakers: usize,
        max_video_downloads_per_receiver: usize,
    ) -> Result<Self, RoomMediaLimitsError> {
        if max_active_audio_speakers == 0 {
            return Err(RoomMediaLimitsError::MaxActiveAudioSpeakersZero);
        }
        if max_video_downloads_per_receiver == 0 {
            return Err(RoomMediaLimitsError::MaxVideoDownloadsPerReceiverZero);
        }
        Ok(Self {
            max_active_audio_speakers,
            max_video_downloads_per_receiver,
        })
    }
}
```

**Rationale:** A simple validated constructor prevents invalid states while
avoiding builder boilerplate for small structs. Private fields also allow the
internal layout to change without affecting callers.
