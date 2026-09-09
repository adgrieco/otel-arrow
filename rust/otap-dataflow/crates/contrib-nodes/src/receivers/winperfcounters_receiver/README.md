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

Reads configured exact Windows performance-counter paths through PDH and emits
OTel gauges. Each data point includes the configured metric identity and
`windows.perf_counter.path` attribute. The resource includes
`os.type=windows`.

The receiver uses English counter paths and does not discover counters or
expand wildcards. Confirm that every configured object, counter, and instance
is installed and enabled on the target host.

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
```

### Receiver options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `counters` | Yes | None | Between 1 and 256 counter entries |
| `collection_interval` | No | `30s` | Interval from `1s` through `24h` |

### Counter options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `path` | Yes | None | Exact English PDH path without `*` or `?` |
| `name` | Yes | None | OTel metric name |
| `unit` | Yes | None | Unit of the emitted value after configured scaling |
| `description` | Yes | None | OTel metric description |
| `scale_power10` | No | `0` | Base-10 scale from `-18` through `18` |

Paths are case-insensitively unique, and metric names are exactly unique.
Unknown fields, empty metadata, duplicate entries, invalid intervals, and
unsupported scales are configuration errors.

## Supported counter families

| Family | Native types | Samples | Output |
| --- | --- | ---: | --- |
| Direct values | `PERF_COUNTER_RAWCOUNT`, `PERF_COUNTER_LARGE_RAWCOUNT`, `PERF_COUNTER_RAWCOUNT_HEX`, `PERF_COUNTER_LARGE_RAWCOUNT_HEX` | 1 | Integer gauge |
| Rates | `PERF_COUNTER_COUNTER`, `PERF_COUNTER_BULK_COUNT` | 2 | Double gauge |
| Timer percentages | `PERF_COUNTER_TIMER`, `PERF_COUNTER_TIMER_INV`, `PERF_100NSEC_TIMER`, `PERF_100NSEC_TIMER_INV` | 2 | Double gauge |
| Raw fractions | `PERF_RAW_FRACTION`, `PERF_LARGE_RAW_FRACTION` | 1 | Double gauge |
| Sample fractions | `PERF_SAMPLE_FRACTION` | 2 | Double gauge |
| Averages | `PERF_AVERAGE_TIMER`, `PERF_AVERAGE_BULK` | 2 | Double gauge |

PDH performs rate, timer, fraction, and average calculations and associates
visible fraction/average numerators with their provider-defined base counters.
Configure only the visible numerator path. Standalone base counters are not
metrics and are rejected.

All supported results are gauges. Formatted rates, interval percentages,
fractions, and averages are not emitted as cumulative OTel sums.

Other native types fail startup with the path and hexadecimal type. See the
[Windows Performance Counters documentation][performance-counters] and the
Windows SDK `winperf.h` header for native type definitions.

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
  fail the scrape.

For example, `\Memory\Available Bytes` remains an exact byte count with unit
`By`, regardless of its provider display scale.

## Collection behavior

The receiver primes the PDH query during startup, then performs its first
scheduled scrape immediately.

- One-sample direct values and raw fractions can emit immediately.
- Two-sample rates, timers, sample fractions, and averages are omitted from the
  first scrape while warming. Other ready values still emit.
- A two-sample value becomes eligible at the next configured interval.
- When a sample-fraction or average base does not advance, no relevant
  operation occurred during that interval. Only that value is omitted; it is
  not replaced with zero.
- A decreasing base is invalid/reset data and fails that scrape.
- Invalid PDH status, non-finite output, or scaling failure emits no partial
  batch. Collection retries at the next interval.

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

## Operational requirements and limitations

- The source pipeline must allocate one core.
- Only one receiver instance can collect in an engine process.
- Separate engine processes are not coordinated; avoid duplicate host
  collection.
- Counter paths must exist when the receiver starts. Dynamic instance
  discovery and wildcard recovery are not supported.
- One scrape can be in flight. Missed ticks are skipped rather than queued.
- Downstream backpressure delays later scrapes instead of creating an
  unbounded buffer.
- Synchronous PDH calls cannot be cancelled. Shutdown remains bounded, but a
  blocked provider call may retain its query resources until it returns.
- Partial-counter resilience, production exporter configuration,
  authentication, Windows service packaging, BLG, TCA, StatsD, and ETW input
  are outside this receiver example.

## Telemetry

Collection failures emit `winperfcounters.scrape_failed`. Query-close failures
emit `winperfcounters.close_failed`. Errors include the affected operation,
counter path where applicable, and PDH status or calculation error.

## References

- [Using the PDH Functions to Consume Counter Data][using-pdh]
- [`PdhAddEnglishCounterW`][add-counter]
- [`PdhGetCounterInfoW`][counter-info]
- [`PdhGetFormattedCounterValue`][formatted-value]
- [`PdhGetRawCounterValue`][raw-value]
- [Windows Performance Counters][performance-counters]

[add-counter]: https://learn.microsoft.com/windows/win32/api/pdh/nf-pdh-pdhaddenglishcounterw
[counter-info]: https://learn.microsoft.com/windows/win32/api/pdh/nf-pdh-pdhgetcounterinfow
[formatted-value]: https://learn.microsoft.com/windows/win32/api/pdh/nf-pdh-pdhgetformattedcountervalue
[performance-counters]: https://learn.microsoft.com/windows/win32/perfctrs/performance-counters-portal
[raw-value]: https://learn.microsoft.com/windows/win32/api/pdh/nf-pdh-pdhgetrawcountervalue
[using-pdh]: https://learn.microsoft.com/windows/win32/perfctrs/using-the-pdh-functions-to-consume-counter-data
