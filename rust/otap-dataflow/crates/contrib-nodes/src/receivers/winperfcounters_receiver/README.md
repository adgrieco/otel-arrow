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

Reads only `\Memory\Available Bytes` through the live Windows PDH API.
The output is an OpenTelemetry integer gauge named `windows.memory.available`
with unit `By`, collection timestamp, data-point attribute
`windows.perf_counter.path`, and resource attribute `os.type=windows`.
It is built directly as OTAP Arrow records, not via intermediate OTLP protobuf.
This input is a development sample, not the finalized host counter profile.

## Configuration

```yaml
type: receiver:winperfcounters
config:
  counter: '\Memory\Available Bytes'
  collection_interval: 30s
```

`counter` is required and accepts exactly the path above. YAML single quotes
preserve single backslashes. `collection_interval` defaults to `30s` only when
omitted; its supported range is `1s` through `24h`. Invalid durations, unknown
fields, null intervals, and all other counter paths are errors.

The source pipeline must allocate **one core**. Multiple instances within one
process are rejected to avoid duplicate host-wide collection. Separate engine
processes are not coordinated; run only one collector for a given host.

## Examples

From `rust\otap-dataflow` in PowerShell:

```powershell
cargo run --features winperfcounters-receiver --bin df_engine -- -c configs\winperfcounters-console.yaml
```

The complete example is
[`configs/winperfcounters-console.yaml`](../../../../../configs/winperfcounters-console.yaml).
It uses one core and the existing console exporter's `pretty` format.
The first collection is immediate; press Ctrl+C to stop.

## Walkthrough

Read these files in order:

1. `../winperfcounters/config.rs`: a Serde enum closes the supported input set.
2. `../winperfcounters/mod.rs`: the source-neutral, integer `Sample`.
3. `pdh.rs`: a stack-owned query, English counter registration, collection,
   status checks, integer extraction, and automatic query/counter cleanup.
4. `../winperfcounters/otap_builder.rs`: one metric, one data point, attributes.
5. `mod.rs`: factory registration, one-core guard, periodic collection on a
   blocking worker, downstream backpressure, and shutdown control.

Every scrape opens and closes its query. This intentionally simple lifecycle
works for this direct gauge; it must not be reused for two-sample rates.
No Windows handles move between threads and no unsafe `Send` is needed.

## Telemetry

Engine receiver telemetry remains available. This POC adds no metric set.
PDH failures include the operation, source path and hexadecimal status.
Query-close failures emit `winperfcounters.close_failed`.

## Limits

- No arbitrary paths, wildcard instances, rates, BLG, TCA, StatsD or ETW input.
- No production exporter/authentication or Windows service packaging.
- No host identity discovery beyond `os.type`; console-only local inspection.
- One scrape is in flight at a time; missed ticks are skipped, not queued.
  Backpressure delays subsequent scrapes instead of buffering unbounded data.
- PDH, worker and projection errors fail the receiver; no zero substitutes.
- Shutdown remains responsive during collection and downstream backpressure.
  An in-flight sample may be discarded on drain. Windows PDH calls cannot be
  cancelled; an already-running blocking task closes its query when the OS call
  returns and retains the singleton lease until then. A hung provider can delay
  runtime teardown; hard cancellation/isolation is outside this POC.
- Requires an installed, accessible Windows Memory performance counter.
  English PDH registration avoids dependence on localized display names.

## Validation

```powershell
cargo test -p otel-arrow-dfe-contrib-nodes --features winperfcounters-receiver --lib winperfcounters
```

Configuration/projection tests are portable. `pdh::tests::real_memory_gauge`
also reads the actual local Windows counter three times, without elevation or
rate warm-up. IRVM validation has not yet been performed.

A local two-sample console smoke on 2026-09-08 produced this first data point
(values vary with host memory pressure):

```text
METRIC name=windows.memory.available unit=By
GAUGE
DATA_POINT time_unix_nano=1788882733442799500 value_int=101356367872
  windows.perf_counter.path=\Memory\Available Bytes
```

The second sample arrived approximately 30 seconds later with value
`101388521472`. The loopback admin liveness endpoint returned HTTP 200.
Ctrl+C initiated graceful shutdown and the engine exited with code 0.
All seven targeted tests, the feature-enabled crate check and clippy, and
`--validate-and-exit` with the example YAML passed.

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
