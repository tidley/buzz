# Buzz Mobile

Flutter mobile client for Buzz.

## Setup

```bash
cd mobile
flutter pub get
```

## Run

```bash
# From repo root (recommended — starts Docker, relay, and simulator):
just mobile-dev

# Direct (requires services and relay already running):
cd mobile && flutter run
```

### Worktree-aware debug identity

Debug builds produced from a git worktree get a unique app identifier keyed
to the **worktree directory name** (`com.buzz.buzzMobile.<slug>` on iOS,
`xyz.block.buzz.mobile.<slug>` on Android) plus a display-only branch label
in the app name (`Buzz (my-branch)`, or a short SHA when the worktree is
detached). Because the identifier follows the directory rather than the
branch, one worktree keeps exactly one installed app — and its login state —
across branch switches, and builds from multiple worktrees install side by
side, mirroring the desktop dev experience. Release and profile builds
always keep the production identity and name.

`just mobile-dev` and `just mobile-build-android` apply this automatically by
running `scripts/mobile-worktree-overrides.sh`, which writes two gitignored
files:

- `mobile/ios/Flutter/WorktreeOverrides.xcconfig` (included by Debug builds
  only; a developer's `AppOverrides.xcconfig` is included after it, so
  app-specific overrides like a personal `BUNDLE_IDENTIFIER` for device
  signing always win)
- `mobile/android/worktree.properties` (read by the debug build type only)

For direct Xcode / Android Studio / `flutter run` development, run
`./scripts/mobile-worktree-overrides.sh` from the repo root once per branch
switch to refresh the display label (the install identity never changes);
the persisted files are then picked up by any subsequent build. In the main
checkout the script is a no-op that removes stale override files, restoring
the plain `Buzz` identity.

To remove leftover worktree-suffixed installs from booted iOS simulators and
connected Android emulators, run `just mobile-clean` (add `--dry-run` via
`./scripts/mobile-worktree-clean.sh --dry-run` to preview). Production
installs are never touched.

## Checks

```bash
dart format --output=none --set-exit-if-changed .
flutter analyze
flutter test
```

Or from the repo root: `just mobile-check` and `just mobile-test`.

## Android release signing

Android release builds fail unless all upload-key inputs are supplied through the
environment:

- `BUZZ_ANDROID_UPLOAD_KEYSTORE_PATH`: path to a CI-vended keystore file
- `BUZZ_ANDROID_UPLOAD_KEYSTORE_PASSWORD`
- `BUZZ_ANDROID_UPLOAD_KEY_ALIAS`
- `BUZZ_ANDROID_UPLOAD_KEY_PASSWORD`

The keystore path must be absolute, and the keystore must remain outside the
repository. Development and debug builds do not require these variables.

Release pipelines that sign through the central APK Signer service instead of
a local upload keystore must set `BUZZ_ANDROID_RELEASE_SIGNING=external`. That
mode produces an unsigned release bundle and refuses to run if any
`BUZZ_ANDROID_UPLOAD_*` value is also set.

## Optional FIPS bridge

`crates/buzz-fips-mobile` is the Buzz-owned Android `cdylib` boundary for a
Flutter FFI integration. It owns one `FipsMobileQuicSession`, and exposes
start, connect, send, receive, stop, and status. It does not change the relay
transport unless a community both enables its FIPS preference and uses a FIPS
peer URL. It is not included in normal Android builds.

The dependency is pinned to `https://github.com/tidley/fips.git` revision
`3f58f1c`. The bridge creates an in-memory identity at `start`; it does not
write identity material. `connect` accepts a UTF-8 Nostr `npub` and establishes
the persistent QUIC stream through FIPS Nostr/STUN discovery. `send` and
`receive` exchange length-delimited application frames on that stream.

On Android builds that include the library, a preferred community with a relay
URL that includes `fipsPeer=<peer-npub>` uses `FipsRelayTransport`. The Dart bridge loads
`libbuzz_fips_mobile.so`, maps bridge status codes to connection and stream
errors, and receives frames on a worker isolate so it does not block the UI.
The transport is selected only on Android. If the preference is disabled, the
URL has no FIPS peer, or the optional library is absent, Buzz uses
the normal WebSocket transport instead.

All operations return a `BridgeStatus` code. `receive(frame, capacity, out_len)`
writes the required frame length to `out_len`; if it returns `BufferTooSmall`,
the frame remains pending and the caller can retry with a larger buffer.
`submit_frame` remains an ABI alias for `send`.

Set the Gradle project property `buzzFipsMobile` for an Android build. The task
writes ABI-specific `.so` files to `app/src/main/jniLibs/`. It needs Android
NDK and `cargo-ndk`, and remains opt-in so normal Flutter and Rust checks do
not need either tool.

```bash
cd mobile/android
./gradlew -PbuzzFipsMobile assembleDebug
```

## Architecture

```
lib/
├── main.dart              # Entry point, Riverpod bootstrap
├── app.dart               # MaterialApp with theme
├── shared/
│   └── theme/             # Catppuccin light/dark, spacing tokens, extensions
└── features/
    └── home/              # Placeholder home surface
```

- **State management:** Riverpod + Hooks (`HookConsumerWidget`)
- **Theme:** Catppuccin Latte (light) / Macchiato (dark) — matches desktop
- **Spacing:** `Grid` tokens for consistent spacing
- **Linting:** `flutter_lints` + `riverpod_lint` via `custom_lint`
- **Feature isolation:** No cross-feature imports except `shared/`
