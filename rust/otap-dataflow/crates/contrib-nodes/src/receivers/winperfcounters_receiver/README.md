# Windows Performance-Counter Receiver

## Metadata

| Field | Value |
| --- | --- |
| Type | `receiver:winperfcounters` |
| URN | `urn:otel:receiver:winperfcounters` |
| Feature | `winperfcounters-receiver` |
| Platform | Windows only |
| Stability | Experimental |

## Overview

Reads configured exact or instance-wildcard Windows performance-counter paths
through PDH and emits OTel gauges. Each data point includes the configured
metric identity and concrete `windows.perf_counter.path` attribute. Wildcard
points also include their configured path template and instance identity. The
resource includes `os.type=windows`.

The receiver accepts English paths. It periodically expands `*` in the
instance segment and keeps exact paths on the existing direct collection path.
Confirm that every configured object and counter is installed and enabled on
the target host.

## Configuration

```yaml
type: receiver:winperfcounters
config:
  counters:
    - path: '\Memory\Available Bytes'
      name: windows.memory.available
      unit: By
      description: Physical memory immediately available for allocation.
    - path: '\Processor(_Total)\% Processor Time'
      name: windows.processor.time
      unit: "%"
      description: Average processor utilization across all logical processors.
  collection_interval: 30s
  wildcard_refresh_interval: 2m
```

### Receiver options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `counters` | Yes | None | Between 1 and 256 counter entries |
| `collection_interval` | No | `30s` | Interval from `1s` through `24h` |
| `wildcard_refresh_interval` | No | See below | Wildcard discovery cadence |
| `max_instances_per_wildcard` | No | `256` | Per-path expansion limit |
| `max_expanded_counters` | No | `4096` | Receiver-wide expansion limit |

`wildcard_refresh_interval` defaults to `collection_interval` and must be
between `collection_interval` and `24h`.
Both expansion limits must be between `1` and `16384`, and the per-wildcard
limit cannot exceed the receiver-wide limit.

### Counter options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `path` | Yes | None | English PDH path |
| `name` | Yes | None | OTel metric name |
| `unit` | Yes | None | Unit of the emitted value after configured scaling |
| `description` | Yes | None | OTel metric description |
| `scale_power10` | No | `0` | Base-10 scale from `-18` through `18` |

Paths are case-insensitively unique, and metric names are exactly unique.
Unknown fields, empty metadata, duplicate entries, invalid intervals, and
unsupported scales are configuration errors.
Configured paths are limited to 2047 UTF-16 code units. Expanded paths that
reach PDH's 2048-unit native limit are omitted and diagnosed.

The `*` wildcard is allowed only in the instance segment. The `?` wildcard
and wildcards in machine, object, or counter names are rejected. Full and
partial instance patterns such as `\Process(*)\Private Bytes` and
`\Process(dotnet*)\Private Bytes` are supported. PDH documents `*` as a
wildcard but no literal escape contract, so literal `*` instance names cannot
be configured.

## Supported counter families

- Direct values use one sample and emit integer gauges:
  `PERF_COUNTER_RAWCOUNT`, `PERF_COUNTER_LARGE_RAWCOUNT`,
  `PERF_COUNTER_RAWCOUNT_HEX`, and `PERF_COUNTER_LARGE_RAWCOUNT_HEX`.
- Rates use two samples and emit double gauges:
  `PERF_COUNTER_COUNTER` and `PERF_COUNTER_BULK_COUNT`.
- Timer percentages use two samples and emit double gauges:
  `PERF_COUNTER_TIMER`, `PERF_COUNTER_TIMER_INV`, `PERF_100NSEC_TIMER`, and
  `PERF_100NSEC_TIMER_INV`.
- Raw fractions use one sample and emit double gauges:
  `PERF_RAW_FRACTION` and `PERF_LARGE_RAW_FRACTION`.
- Sample fractions use two samples and emit double gauges:
  `PERF_SAMPLE_FRACTION`.
- Averages use two samples and emit double gauges:
  `PERF_AVERAGE_TIMER` and `PERF_AVERAGE_BULK`.

PDH performs rate, timer, fraction, and average calculations and associates
visible fraction/average numerators with their provider-defined base counters.
Configure only the visible numerator path. Standalone base counters are not
metrics and are rejected.

All supported results are gauges. Formatted rates, interval percentages,
fractions, and averages are not emitted as cumulative OTel sums.

Unsupported exact counter types fail startup. Unsupported expanded instances
are omitted and diagnosed without suppressing healthy counters. Diagnostics
include the hexadecimal native type. See the [Windows Performance Counters
documentation][performance-counters] and the Windows SDK `winperf.h` header
for native type definitions.

## Scaling

The receiver requests unscaled native values and applies only
`scale_power10`. It never applies the provider's default display scale. The
configured unit must describe the value after this explicit scaling.

- Zero scale preserves direct values as exact integers.
- Positive integer scaling remains an integer when multiplication does not
  overflow.
- Negative integer scaling always emits a double. Exact decimal division is
  performed before conversion when possible; otherwise the source integer
  must be exactly representable as `f64`.
- Calculated values and scaled doubles must remain finite. Overflow,
  precision-unsafe integer conversion, and nonzero values underflowing to zero
  omit that point and report `winperfcounters.counter_failed`; other healthy
  points in the scrape still emit.

For example, `\Memory\Available Bytes` remains an exact byte count with unit
`By`, regardless of its provider display scale.

## Collection behavior

The receiver primes the PDH query during startup, then performs its first
scheduled scrape immediately.

- Wildcards are expanded at startup and then at
  `wildcard_refresh_interval`. Concrete paths are joined by configured counter
  and full case-insensitive path, including PDH's `#n` duplicate index.
- Newly discovered one-sample counters can emit on their first collection.
  Newly discovered two-sample counters warm independently.
- Removed instances stop emitting after the next discovery refresh.
- One-sample direct values and raw fractions can emit immediately.
- Two-sample rates, timers, sample fractions, and averages are omitted from the
  first scrape while warming. Other ready values still emit.
- A two-sample value becomes eligible at the next configured interval.
- When a sample-fraction or average base does not advance, no relevant
  operation occurred during that interval. Only that value is omitted; it is
  not replaced with zero.
- A decreasing base is invalid/reset data and omits that point while resetting
  its baseline for the next collection.
- A counter-local invalid PDH status, non-finite output, or scaling failure
  omits only that point and reports `winperfcounters.counter_failed`. Healthy
  exact counters and wildcard peers remain in the batch.
- Counter add/read failures remove only the affected handle and retry with
  exponential backoff capped by `wildcard_refresh_interval`.
- A query-level collection failure emits no batch and retries the existing
  query with bounded exponential backoff so transient failures preserve
  history. Three consecutive collection failures rebuild the worker-owned
  query and all counters.
- Wildcard expansion is sorted before applying the configured limits. Excess
  instances are omitted, reported explicitly, and reconsidered at the next
  discovery refresh; they are never presented as a complete expansion.

Each emitted point uses the collection timestamp and has no cumulative start
time.

## Examples

From `rust\otap-dataflow`:

```powershell
cargo run --features winperfcounters-receiver --bin df_engine -- -c configs\winperfcounters-console.yaml
```

The basic
[`winperfcounters-console.yaml`](../../../../../configs/winperfcounters-console.yaml)
example contains Available Bytes and total Processor utilization.

The
[`winperfcounters-calculations-console.yaml`](../../../../../configs/winperfcounters-calculations-console.yaml)
example also contains:

- `\Memory\% Committed Bytes In Use`
- `\Cache\Data Map Hits %`
- `\PhysicalDisk(_Total)\Avg. Disk sec/Read`
- `\PhysicalDisk(_Total)\Avg. Disk Bytes/Read`

The focused
[`winperfcounters-wildcard-console.yaml`](../../../../../configs/winperfcounters-wildcard-console.yaml)
example combines an exact Memory counter with
`\Process(*)\Private Bytes` and refreshes discovery every five seconds. It
uses only built-in Windows performance counters and demonstrates bounded
per-process expansion without requiring a separate test executable.

The calculations example requires the Memory, Processor, Cache, and
PhysicalDisk performance objects, the PhysicalDisk `_Total` instance, and
their provider-defined base counters. During an idle interval, Cache or disk
values may be absent when their bases do not advance.

To check the configuration structure without starting collection:

```powershell
.\df_engine.exe --validate-and-exit -c .\winperfcounters-calculations-console.yaml
```

Provider availability and native types are checked when the receiver starts,
not by `--validate-and-exit`.

## Limits

- The source pipeline must allocate one core.
- Only one receiver instance can collect in an engine process.
- Separate engine processes are not coordinated; avoid duplicate host
  collection.
- Use narrow wildcard patterns where possible. The configured per-path and
  receiver-wide limits bound active handles and emitted cardinality.
- One scrape can be in flight. Missed ticks are skipped rather than queued.
- Downstream backpressure delays later scrapes instead of creating an
  unbounded buffer.
- Synchronous PDH calls cannot be cancelled. Shutdown remains bounded, but a
  blocked provider call may retain its query resources until it returns.
- Production exporter configuration, authentication, Windows service
  packaging, BLG, TCA, StatsD, and ETW input are outside this receiver
  example.

## Telemetry

Counter-local failures emit `winperfcounters.counter_failed` with a configured
path template and low-cardinality reason. Expansion overflow emits
`winperfcounters.instance_limit_exceeded`; lifecycle and retry recovery emit
aggregate events. Query-level collection failures emit
`winperfcounters.scrape_failed`, and query-close failures emit
`winperfcounters.close_failed`.

The `receiver.winperfcounters` metric set records configured and active
counters, scrape success/failure and duration, discovery refreshes, instance
adds/removals/overflow, counter failures, retries/recoveries, query rebuilds,
and warm-up omissions.

## Related documentation

- [Using the PDH Functions to Consume Counter Data][using-pdh]
- [`PdhAddEnglishCounterW`][add-counter]
- [`PdhExpandWildCardPathW`][expand-wildcard]
- [`PdhGetCounterInfoW`][counter-info]
- [`PdhGetFormattedCounterValue`][formatted-value]
- [`PdhGetRawCounterValue`][raw-value]
- [Windows Performance Counters][performance-counters]

[add-counter]: https://learn.microsoft.com/windows/win32/api/pdh/nf-pdh-pdhaddenglishcounterw
[counter-info]: https://learn.microsoft.com/windows/win32/api/pdh/nf-pdh-pdhgetcounterinfow
[expand-wildcard]: https://learn.microsoft.com/windows/win32/api/pdh/nf-pdh-pdhexpandwildcardpathw
[formatted-value]: https://learn.microsoft.com/windows/win32/api/pdh/nf-pdh-pdhgetformattedcountervalue
[performance-counters]: https://learn.microsoft.com/windows/win32/perfctrs/performance-counters-portal
[raw-value]: https://learn.microsoft.com/windows/win32/api/pdh/nf-pdh-pdhgetrawcountervalue
[using-pdh]: https://learn.microsoft.com/windows/win32/perfctrs/using-the-pdh-functions-to-consume-counter-data
