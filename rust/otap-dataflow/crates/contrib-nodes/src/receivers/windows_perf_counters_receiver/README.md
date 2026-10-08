# Windows Performance-Counter Receiver

## Metadata

| Field | Value |
| --- | --- |
| Type | `receiver:windowsperfcounters` |
| URN | `urn:otel:receiver:windowsperfcounters` |
| Feature | `windows-perf-counters` |
| Platform | Windows only |
| Stability | Experimental |

## Overview

Reads configured exact or instance-wildcard Windows performance-counter paths
through PDH and emits OTel Gauges or UpDownCounters. Each data point includes
the configured metric identity and concrete `windows.perf_counter.path`
attribute. Wildcard points also include their configured path template and
instance identity. The resource includes `os.type=windows`.

The receiver constructs PDH paths from configured performance objects,
instances, and counters, and periodically refreshes wildcard instances.
Configured objects and counters must be installed and enabled on the target
host.

## Configuration

```yaml
type: receiver:windowsperfcounters
config:
  metrics:
    windows.memory.available:
      unit: By
      description: Physical memory immediately available for allocation.
      gauge: {}
    windows.processor.time:
      unit: "%"
      description: Average processor utilization across all logical processors.
      gauge: {}
    windows.process.private:
      unit: By
      description: Committed private memory for each process instance.
      up_down_counter: {}
  perfcounters:
    - object: Memory
      counters:
        - name: Available Bytes
          metric: windows.memory.available
    - object: Processor
      instances: ["_Total"]
      counters:
        - name: "% Processor Time"
          metric: windows.processor.time
    - object: Process
      instances: ["*"]
      counters:
        - name: Private Bytes
          metric: windows.process.private
  initial_delay: 1s
  collection_interval: 30s
  wildcard_refresh_interval: 2m
```

### Receiver options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `metrics` | Yes | None | Metric metadata keyed by OTel metric name |
| `perfcounters` | Yes | None | Performance objects and counter mappings |
| `initial_delay` | No | `1s` | Delay before the first collection request |
| `collection_interval` | No | `30s` | Interval from `1s` through `24h` |
| `wildcard_refresh_interval` | No | See below | Wildcard discovery cadence |
| `max_instances_per_wildcard` | No | `256` | Per-path expansion limit |
| `max_expanded_counters` | No | `4096` | Receiver-wide expansion limit |

`wildcard_refresh_interval` defaults to `collection_interval` and must be
between `collection_interval` and `24h`. Both expansion limits must be between
`1` and `16384`, and the per-wildcard limit cannot exceed the receiver-wide
limit. Normalization produces between 1 and 256 exact or wildcard paths, and
`initial_delay` accepts `0s` through `24h`.

### Metric options

Each `metrics` key is an OTel metric name. Its value requires `description`,
`unit`, and exactly one empty `gauge: {}` or `up_down_counter: {}` object.
The kind key may also use YAML shorthand such as `gauge:`. Metric names follow
the OTel instrument-name syntax and are limited to 255 characters. Units must
be printable ASCII and at most 63 characters.
Multiple counter mappings may reference one metric and contribute points
distinguished by their configured attributes. Gauges have point-in-time
semantics and no start timestamp. UpDownCounters are cumulative non-monotonic
Sums whose start timestamp is the opening time of the current PDH query.

### Performance object and counter options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `object` | Yes | None | Windows performance object |
| `instances` | No | None | One instance, a list, or `"*"` |
| `aggregation_name` | No | `_Total` | Provider aggregation instance |
| `counters` | Yes | None | Counter mappings for the object |

Each counter mapping requires `name` and `metric`. Optional `attributes`
contains static string key/value pairs added to each emitted point.
`scale_power10` defaults to zero and accepts values from `-18` through `18`.
Attribute keys beginning with `windows.perf_counter.` are reserved for
receiver-generated counter identity.
Counter names may contain parentheses, as required by counters such as
`Avg. Disk sec/Read (Base)`, but cannot contain path separators or wildcard
path syntax.

Configure performance object and counter names in English. Exact paths are
added through PDH's language-neutral English API. Before wildcard expansion,
the receiver asks PDH to translate the English template to its localized full
path, expands that localized template, and adds the resulting concrete paths
through the localized API.

Omit `instances` for objects without instances. Specify one name or a list for
exact instances. Specify `"*"` to discover all concrete instances while
omitting the configured aggregation instance. Specify `["*", "_Total"]` to
retain `_Total`, or select `"_Total"` alone to collect only that exact
instance. Set `aggregation_name` when a provider uses another name such as
`_Global_`. Filtering occurs before expansion limits, and explicit wildcard
inclusion does not create a duplicate aggregate query.

Explicit PDH duplicate-instance indexes are canonicalized before path and
duplicate checks: the first occurrence omits `#0`, and leading zeroes are
removed from later indexes.

Unknown fields, empty metadata, undefined or unused metrics, duplicate
instances, duplicate normalized paths, invalid intervals, and unsupported
scales are configuration errors. Surrounding whitespace is removed from metric
identities, metadata, path segments, references, and attribute keys before
duplicate detection. Embedded NUL characters are rejected. At most 256 metric
definitions and 256 normalized exact or wildcard counter paths are accepted;
the path total is checked before materialization. Repeated metric descriptions
and mapping attributes are shared across explicit instance expansions.
Configured paths are limited to 2047 UTF-16 code units. Expanded paths that
reach PDH's 2048-unit native limit are omitted and diagnosed.

## Supported counter families

| Family | Native types | Samples | Output |
| --- | --- | --- | --- |
| Direct values | `PERF_COUNTER_RAWCOUNT`, `PERF_COUNTER_LARGE_RAWCOUNT`, and hexadecimal variants | One | Integer |
| Rates | `PERF_COUNTER_COUNTER`, `PERF_COUNTER_BULK_COUNT` | Two | Double |
| Deltas and samples | `PERF_COUNTER_DELTA`, `PERF_COUNTER_LARGE_DELTA`, `PERF_SAMPLE_COUNTER` | Two | Double |
| Queue lengths | DWORD, large, 100-nanosecond, and object-time queue variants | Two | Double |
| Timer percentages | System, 100-nanosecond, object-time, inverse, and precision timer variants | Two | Double |
| Raw fractions | `PERF_RAW_FRACTION`, `PERF_LARGE_RAW_FRACTION` | One | Double |
| Sample fractions | `PERF_SAMPLE_FRACTION` | Two | Double |
| Averages | `PERF_AVERAGE_TIMER`, `PERF_AVERAGE_BULK` | Two | Double |

The configured metric kind is independent of PDH's numeric formatting. Use a
Gauge for non-additive current values such as utilization and an UpDownCounter
for additive current values such as committed private memory. Metric names and
descriptions must preserve the source counter's semantics; Windows Private
Bytes includes committed memory that may reside in RAM or the page file and
must not be labeled as the physical-memory metric `process.memory.usage`.

PDH performs rate, timer, fraction, and average calculations and associates
visible fraction/average numerators with their provider-defined base counters.
Configure only the visible numerator path. Standalone base counters are not
metrics and are rejected.

Elapsed-time and multi-timer counter families remain unsupported because they
require output semantics beyond the existing regular PDH formatting path.

The receiver requests regular PDH formatted counter values and emits them as
OTel metrics. Geneva-specific Full or Factored event formats are transport and
schema choices outside this receiver's OTAP metric contract.

Unsupported exact counter types fail startup. A missing or inaccessible exact
counter is omitted and retried, including when no configured counter is
initially available. Unsupported expanded instances are omitted and diagnosed
without suppressing healthy counters. Diagnostics include the native type.

## Scaling

The receiver requests unscaled values and applies only `scale_power10`; the
configured unit must describe the scaled result.

- Zero scale preserves direct values as exact integers.
- Any nonzero integer scale emits a double using standard `f64` rounding.
- Calculated and scaled values preserve finite subnormal values and signed zero.
- Non-finite inputs or results are omitted and diagnosed without suppressing
  healthy points.

For example, `\Memory\Available Bytes` remains an exact byte count with unit
`By` at the default scale.

## Collection behavior

The receiver primes the PDH query during startup, waits `initial_delay`, then
performs its first scheduled scrape. This delay does not replace two-sample
counter warm-up or startup retries.

- Wildcards are expanded at startup and then at
  `wildcard_refresh_interval`. Concrete paths are joined by configured counter
  and full case-insensitive path, including PDH's `#n` duplicate index.
- One-sample counters can emit immediately. Two-sample counters warm
  independently and become eligible at the next configured interval.
- Removed instances stop emitting after the next discovery refresh.
- When a sample-fraction or average base does not advance, no relevant
  operation occurred; that value is omitted rather than replaced with zero. A
  decreasing base also omits the point and resets its baseline.
- A counter-local invalid PDH status, non-finite output, or scaling failure
  omits only that point; healthy peers remain in the batch.
- Counter add/read failures remove only the affected handle and retry with
  exponential backoff capped by `wildcard_refresh_interval`.
- Counter availability does not fail startup. An empty active set emits no
  points until a configured counter becomes available.
- A query-level collection failure emits no batch and retries the existing
  query with bounded exponential backoff so transient failures preserve
  history. Three consecutive collection failures rebuild the worker-owned
  query and all counters.
- Each scrape is bounded by `collection_interval`. If a native collection call
  remains blocked after that timeout, later ticks fail fast while the worker is
  busy; no additional collection request is queued.
- Wildcard expansion is sorted before applying the configured limits. Excess
  instances are omitted, reported explicitly, and reconsidered at the next
  discovery refresh; they are never presented as a complete expansion.

Each emitted point uses the collection timestamp. UpDownCounter points use the
current PDH query's opening time as their cumulative start time; a query rebuild
or backward wall-clock adjustment starts a new cumulative sequence. Gauge
points do not use or validate cumulative start time.

## Examples

From `rust\otap-dataflow`:

```powershell
cargo run --features windows-perf-counters --bin df_engine -- -c configs\windowsperfcounters-console.yaml
```

The basic
[`windowsperfcounters-console.yaml`](../../../../../configs/windowsperfcounters-console.yaml)
example contains Available Bytes and total Processor utilization.

The
[`windowsperfcounters-calculations-console.yaml`](../../../../../configs/windowsperfcounters-calculations-console.yaml)
example demonstrates rates, fractions, and averages.

The focused
[`windowsperfcounters-wildcard-console.yaml`](../../../../../configs/windowsperfcounters-wildcard-console.yaml)
example combines an exact Memory counter with
`\Process(*)\Private Bytes` and refreshes discovery every five seconds. It
uses only built-in Windows performance counters and demonstrates bounded
per-process expansion without requiring a separate test executable.

To check the configuration structure without starting collection:

```powershell
.\df_engine.exe --validate-and-exit -c .\windowsperfcounters-calculations-console.yaml
```

Provider availability and native types are checked when the receiver starts,
not by `--validate-and-exit`.

## Limits

- The source pipeline must allocate one core.
- Multiple receiver nodes may use independent intervals. Each node owns one
  worker and PDH query; avoid unintentionally configuring duplicate collection.
- Use narrow wildcard patterns where possible. The configured per-path and
  receiver-wide limits bound active handles and emitted cardinality.
- One scrape can be in flight. Missed ticks are skipped rather than queued.
- Downstream backpressure delays later scrapes instead of creating an
  unbounded buffer.
- Synchronous PDH calls cannot be cancelled. The receiver waits at most one
  second for worker cleanup while preserving time for pipeline completion. A
  blocked provider call may retain its query resources until it returns.

## Telemetry

Counter-local failures emit
`otelcol.node.windowsperfcounters.counter.fail` with a configured path template
and low-cardinality reason. Expansion overflow, instance changes, recovery,
query-level failures, scrape timeouts, shutdown timeouts, and query-close
failures use the same `otelcol.node.windowsperfcounters.*` event namespace.
Repeated scrape and counter warnings are suppressed until the affected
condition recovers.

The `receiver.windowsperfcounters.scrapes` measurement metric set attributes
collection attempts by outcome. The `receiver.windowsperfcounters` health
metric set records configured and active counters, scrape success/failure,
overruns and duration, discovery refreshes, instance
adds/removals/overflow, failed counter values, retries/recoveries, query
rebuilds, and warm-up omissions.

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
