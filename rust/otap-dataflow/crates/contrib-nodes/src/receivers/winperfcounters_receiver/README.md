# Windows Performance-Counter Receiver

## Metadata

| Field | Value |
| --- | --- |
| Type | `receiver:winperfcounters` |
| URN | `urn:otel:receiver:winperfcounters` |
| Feature | `winperfcounters-receiver` |
| Platform | Windows only |
| Stability | Experimental development POC |

## Overview

Reads configured exact Windows performance-counter paths through the live PDH
API. Each entry supplies its OTel metric name, unit, and description. The
receiver emits integer gauges with collection timestamps, data-point attribute
`windows.perf_counter.path`, and resource attribute `os.type=windows`. It builds
OTAP Arrow records directly, without intermediate OTLP protobuf. The included
memory counter is a development sample, not a hard-coded ALDO-W profile.

## Configuration

```yaml
type: receiver:winperfcounters
config:
  counters:
    - path: '\Memory\Available Bytes'
      name: windows.memory.available
      unit: By
      description: Physical memory immediately available for allocation.
  collection_interval: 30s
```

`counters` requires 1 through 256 entries. Every entry requires a non-empty
exact English PDH path, metric name, unit, and description. Paths are
case-insensitively unique, metric names are exactly unique, and `*` or `?`
wildcards are rejected. YAML single quotes preserve single backslashes.
`collection_interval` defaults to `30s` only when omitted; its supported range
is `1s` through `24h`. Invalid durations and unknown fields are errors.

Milestone 1 supports only native `PERF_COUNTER_RAWCOUNT` and
`PERF_COUNTER_LARGE_RAWCOUNT` counters. Values are requested with
`PDH_FMT_LARGE | PDH_FMT_NOSCALE`, preserving the exact integer rather than
silently applying the native display scale. The configured unit must describe
that unscaled value. For example, `\Memory\Available Bytes` advertises display
scale `-6`, but this receiver emits its unscaled byte count with unit `By`.
Rates, percentages, fractions, timers, averages, deltas, text, and
hexadecimal-display raw counters are rejected at startup with the path and
native type in the error. Applying any scale or conversion is unsupported.

The source pipeline must allocate **one core**. Multiple instances within one
process are rejected to avoid duplicate host-wide collection. Separate engine
processes are not coordinated; run only one collector for a given host.

## Examples

From `C:\Repos\otel-arrow\rust\otap-dataflow` in PowerShell:

```powershell
cargo run --features winperfcounters-receiver --bin df_engine -- -c configs\winperfcounters-console.yaml
```

The complete example is
[`configs/winperfcounters-console.yaml`](../../../../../configs/winperfcounters-console.yaml).
It uses one core and the existing console exporter's `pretty` format.
The first collection is immediate; press Ctrl+C to stop.

## Walkthrough

Read these files in order:

1. `../winperfcounters/config.rs`: strict reusable counter configuration.
2. `../winperfcounters/mod.rs`: the source-neutral, ordered integer `Sample`.
3. `pdh.rs`: the persistent query owner, native-type inspection, collection,
   status checks, integer extraction, and automatic query/counter cleanup.
4. `../winperfcounters/otap_builder.rs`: configured gauges and attributes.
5. `mod.rs`: factory registration, one-core guard, periodic collection on a
   dedicated blocking worker, downstream backpressure, and shutdown control.

One dedicated OS thread opens the query and all counters once, services a
capacity-one command channel, and closes the query on exit. Windows handles
never leave that thread and no unsafe `Send` is needed.

## Telemetry

Engine receiver telemetry remains available. This milestone adds no metric set.
PDH failures include the operation, source path, and hexadecimal status. A
failed scrape emits `winperfcounters.scrape_failed`, sends no partial batch or
zero value, and retries on the next configured interval. Query-close failures
emit `winperfcounters.close_failed`.

## Limits

- No wildcards, rates, scaled values, BLG, TCA, StatsD, or ETW input.
- No production exporter/authentication or Windows service packaging.
- No host identity discovery beyond `os.type`; console-only local inspection.
- One scrape is in flight at a time; missed ticks are skipped, not queued.
  Backpressure delays subsequent scrapes instead of buffering unbounded data.
- PDH collection/status failures are reported and retried at the next interval;
  no partial batches or zero substitutes are emitted. Worker and projection
  failures stop the receiver.
- Shutdown remains responsive during collection and downstream backpressure.
  An in-flight sample may be discarded on drain. Synchronous PDH calls cannot
  be cancelled. If one outlives the engine deadline, its worker retains query
  ownership and the singleton lease, then closes the query when the call
  returns. Hard cancellation or process isolation is outside this milestone.
- Configured counters must be installed and accessible. English PDH
  registration avoids dependence on localized display names.

## Validation

```powershell
cargo test -p otel-arrow-dfe-contrib-nodes --features winperfcounters-receiver --lib winperfcounters
```

Configuration/projection tests are portable. The worker test opens one query,
reads two installed Memory counters three times, and verifies one cleanup on
shutdown. Initialization-error coverage verifies partial query cleanup. A
simulated blocked worker verifies deadline return, later cleanup, and singleton
lease retention until the worker finishes.

The milestone-1 validation used these exact files:

```text
Binary: C:\Repos\otel-arrow\rust\otap-dataflow\target\debug\df_engine.exe
Config: C:\Repos\otel-arrow\rust\otap-dataflow\configs\winperfcounters-console.yaml
```

The binary was built from `C:\Repos\otel-arrow\rust\otap-dataflow` with:

```powershell
cargo build --features winperfcounters-receiver --bin df_engine
```

The bounded console smoke used loopback admin control:

```powershell
.\target\debug\df_engine.exe --config .\configs\winperfcounters-console.yaml --http-admin-bind 127.0.0.1:18085
Invoke-WebRequest -UseBasicParsing -Method Post -Uri 'http://127.0.0.1:18085/api/v1/groups/shutdown?wait=true&timeout_secs=10'
```

On 2026-09-08 it emitted three ASCII data points with the configured identity
(values vary with host memory pressure):

```text
METRIC name=windows.memory.available unit=By
GAUGE
DATA_POINT time_unix_nano=1788894834668402300 value_int=98923638784
  windows.perf_counter.path=\Memory\Available Bytes
```

The next timestamps were `1788894864661887000` and `1788894894665995200`,
giving intervals of 29.993 seconds and 30.004 seconds. `GET /api/v1/readyz`
returned HTTP 200. The waited admin shutdown returned HTTP 200 with
`{"status":"completed","durationMs":114}`; the process printed
`Pipeline run successfully`, exited with code 0, and was no longer running.
All 11 targeted receiver tests and the feature-enabled contrib-nodes crate
check passed.

The locally validated executable uses the unoptimized debug profile and targets
x64 Windows. Its PE imports include `VCRUNTIME140.dll`, Windows Universal CRT
API sets, and Windows system DLLs including `pdh.dll`. The target must provide
the compatible x64 Visual C++ runtime/UCRT and the Memory performance counter;
this is not a statically linked or production-ready deployment artifact.
Target-machine compatibility still requires validation on the IRVM.

Repository-wide validation is not complete: `cargo xtask check` was stopped
after a bounded three-minute attempt, and `tools/sanitycheck.py` reported
pre-existing CRLF line endings throughout the checkout.

## Related Docs

- [Contrib catalog](../../../README.md)
- [Runtime configuration](../../../../../docs/configuration.md)
