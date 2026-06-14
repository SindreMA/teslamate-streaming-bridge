# Patched TeslaMate

A custom build of [TeslaMate](https://github.com/teslamate-org/teslamate) with a fix
for a streaming-API sleep bug that otherwise burns the Tesla Fleet API billing budget.

## Why

We feed TeslaMate via Fleet Telemetry → Kafka → the streaming bridge (this repo), with
the per-car **Streaming API** enabled. The car streams directly to our own endpoint, so
streaming itself costs ~nothing; the only Fleet-API cost is TeslaMate's `vehicle_data`
polling, which streaming is supposed to make unnecessary.

On **2026-06-14** the billing limit was exhausted. Root cause (traced in TeslaMate's
`lib/teslamate/vehicles/vehicle.ex`): once a streaming car is `:online`, it always calls
the billable `vehicle_data`. A sleeping car answers HTTP `408` ("vehicle unavailable"),
which TeslaMate retries every ~20s **forever** because:

1. There is **no `{:stream, :inactive}` handler for the `:online` state**, so a car that
   parks and sleeps (stream goes quiet) has no way out of `:online`.
2. When the stream connects but sends nothing, TeslaMate promotes to `:confirmed_real`
   and starts polling `vehicle_data` — backwards for a bridge setup, where **a silent
   stream means the car is asleep**.

One car looped for ~44h (thousands of `vehicle_data` calls) before the limit tripped.

## The patch

`patches/0001-streaming-no-vehicle-data-when-asleep.patch` (against the pinned
`TESLAMATE_VERSION`) makes two changes, both inside `use_streaming_api: true` paths only
(zero effect on normal polling users):

1. **Add a `{:stream, :inactive}` handler for `:online`** → drop the stream and
   re-evaluate from `:start` when the car stops streaming (i.e. went to sleep).
2. **Silent stream → `:confirmed_fake`** instead of `:confirmed_real` → stay on the free
   vehicle-list endpoint and only fetch `vehicle_data` once real stream data (numeric
   power) arrives, i.e. the car is genuinely awake/streaming.

Net effect: `vehicle_data` is called **only while the car is actually streaming**; an
asleep car polls only the free listing endpoint. No 20s loop, no budget burn.

## Build / deploy

CI: `.github/workflows/teslamate-patched.yml`. On changes under `teslamate-patch/**`
(or manual dispatch) it clones TeslaMate at `TESLAMATE_VERSION`, applies the patch,
builds with TeslaMate's own Dockerfile, pushes to
`registry.k8s.sindrema.com/images/teslamate-patched`, and bumps the image in
`sindre-k8s` (`manifests/useful-services/teslamate-deployment.yaml`).

## Upgrading TeslaMate

1. Bump `TESLAMATE_VERSION` in the workflow.
2. Locally verify the patch still applies:
   `git clone --branch <ver> … && cd teslamate && git apply --check ../teslamate-patch/patches/*.patch`
   — if it fails, regenerate the patch against the new source.
3. Push; CI rebuilds and redeploys.

## Reverting

Point `manifests/useful-services/teslamate-deployment.yaml` back to
`image: teslamate/teslamate:latest` (or `:vX.Y.Z`) and let Flux reconcile.
