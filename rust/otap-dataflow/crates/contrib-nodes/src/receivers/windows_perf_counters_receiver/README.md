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
through PDH and emits OTel Gauges or cumulative or delta Sums. Each data point includes
the configured metric identity and concrete `windows.perf_counter.path`
attribute. Instance points emit Go-compatible `instance` identity plus
extended `windows.perf_counter.*` identity attributes. Wildcard points also
include their configured path template. The resource includes
`os.type=windows`.

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
      sum:
        aggregation: cumulative
        monotonic: false
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

<!-- markdownlint-disable MD013 -->

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `metrics` | No | Empty | Optional metric metadata keyed by OTel metric name |
| `perfcounters` | Yes | None | Performance objects and counter mappings |
| `initial_delay` | No | `1s` | Delay before the first collection request; non-positive means immediate |
| `collection_interval` | No | `1m` | Any positive collection interval |
| `timeout` | No | `collection_interval` | Scrape lateness warning threshold; `0s` uses the collection interval |
| `wildcard_refresh_interval` | No | See below | Wildcard discovery cadence |
| `max_instances_per_wildcard` | No | `256` | Per-path expansion limit |
| `max_expanded_counters` | No | `4096` | Receiver-wide exact plus expanded native-counter limit |

<!-- markdownlint-enable MD013 -->

`wildcard_refresh_interval` defaults to the greater of `collection_interval`
and `60s`, and an explicit value must be at least `collection_interval`. Both
counter limits must be between
`1` and `16384`, and the per-wildcard limit cannot exceed the receiver-wide
limit. `max_expanded_counters` is retained for configuration compatibility but
limits the combined exact and expanded native counters owned by the receiver.
Exact paths reserve entries even while unavailable so later retry cannot exceed
the limit. Wildcard templates do not consume entries until they resolve to
concrete paths. Zero or negative `initial_delay` starts collection immediately.
Sub-second collection intervals are accepted, matching Go, but rate and other
two-sample counters generally need about one second or more between samples to
produce meaningful values.

### Metric options

Each `metrics` key is an OTel metric name. `description` defaults to empty and
`unit` defaults to `"1"`. Definitions may be unused. A definition may select
one `gauge: {}` or `sum:` object; omitting both defaults to Gauge. A Sum
requires `aggregation: cumulative` or `aggregation: delta`; `monotonic`
defaults to `false`. Empty and null Sum configurations are rejected. Metrics
that configure both Gauge and Sum are also rejected. Gauge may use YAML
shorthand such as `gauge:`. Metric definition names preserve their exact
configured spelling, including whitespace and control characters, and only the
literal empty name is invalid. Names are OTLP stream names rather than SDK
instrument names, so names such as `\Memory\Available Bytes` are accepted.
Metric identity and lookup are case-sensitive, so definitions such as `cpu`
and `CPU` remain distinct. Every valid group that differs only by Unicode
lowercase identity emits one configuration warning because it may be a typo.
Only a literal empty unit defaults to `"1"`; every other unit and every
description remains exactly as configured, including whitespace and control
characters. Non-empty units have no receiver-specific length limit.
Multiple counter mappings may reference one metric and contribute points
distinguished by their configured attributes. Gauges have point-in-time
semantics and no start timestamp. Cumulative monotonic Sums are limited to
non-negative direct raw counts. Cumulative non-monotonic Sums are available
for every supported numeric family. Delta Sums are limited to
`PERF_COUNTER_DELTA` and `PERF_COUNTER_LARGE_DELTA`; PDH calculates the
difference and the receiver emits it as a double without subtracting raw
totals.

```yaml
windows.operations:
  description: Operations completed since the provider sequence began.
  unit: "{operation}"
  sum:
    aggregation: cumulative
    monotonic: true
```

### Performance object and counter options

| Option | Required | Default | Description |
| --- | --- | --- | --- |
| `object` | Yes | None | Windows performance object |
| `instances` | No | None | One instance, a list, or `"*"` |
| `aggregation_name` | No | `_Total` | Provider aggregation instance |
| `counters` | Yes | None | Counter mappings for the object |

Each counter mapping requires `name`; `metric` is optional. Without `metric`,
the receiver uses the full configured PDH path, including its instance pattern,
as the metric name. If that exact, case-sensitive name matches a metric
definition, the receiver uses its metadata and Gauge or Sum kind. Otherwise it
emits a Gauge with unit `"1"` and an empty description. Mapping `attributes`
are ignored in either implicit mode.

For mappings that reference a metric, optional `attributes` contains static
string key/value pairs added to each emitted point. Attribute keys and values
preserve their exact configured spelling; only a literal empty key is invalid,
so whitespace-only and control-character keys remain configured identity. Keys
such as "host" and " host" remain distinct. The `instance` key is allowed and
overwrites the receiver-generated Go-compatible value. On an object with
multiple or wildcard instances, this override produces a configuration warning
because a fixed value can collapse distinct points into the same OTel series
when no other attributes distinguish them. This is a series collision, not
automatic summation; internal counter histories and point values remain
independent. Metric-less mappings do not warn because their configured
attributes are ignored.
One normalized PDH path may feed multiple distinct metric mappings. The
receiver opens an independent handle for each mapping and routes each sample by
its configured mapping identity. Repeating the same path, metric name, and
static attributes is rejected because it would emit duplicate points for one
series.
`scale_power10` defaults to zero and accepts values from `-18` through `18`.
`recreate_query` defaults to `false`. When `true`, the mapping owns an isolated
PDH query. After every successful scrape, including an empty scrape, the
receiver creates and primes a replacement before releasing the old query. If
replacement construction or priming fails, collection continues with the old
query and reports the bounded mapping failure. Wildcard-expanded instances and
explicit instance paths from the same mapping share its isolated query;
all mappings without query recreation share one persistent query even when
isolated mappings appear between them in configuration, and unrelated mappings
keep their own sampling histories. Each successful
replacement starts a new cumulative sequence, so cumulative Sum points from a
mapping with `recreate_query: true` have a new start time on every scrape. For
monotonic Sums, the first point published from each replacement reports
`start_time_unix_nano == time_unix_nano` after it is successfully delivered;
priming and discarded scrapes do not expose an earlier unpublished start.
Attribute keys beginning with `windows.perf_counter.` are reserved for
receiver-generated counter identity.
Object, instance, aggregation, and counter components preserve their configured
spelling, including whitespace and non-NUL control characters. Object, explicit
instance, aggregation, and counter components reject only the literal empty
value where required; an empty `aggregation_name` selects its default. Embedded
NUL is rejected before the native call because a null-terminated PDH path cannot
represent it. Delimiters such as backslashes and parentheses are passed to PDH
because providers may use them as part of valid native identity. An invalid or
unavailable constructed path is diagnosed for that counter without rejecting
unrelated mappings.

Configure performance object and counter names in English. Exact paths are
added through PDH's language-neutral English API. Before wildcard expansion,
the receiver asks PDH to translate the English template to its localized full
path, expands that localized template, and adds the resulting concrete paths
through the localized API. Localized exact object, instance, or counter names
are not accepted as configuration aliases, and exact-path collection does not
fall back to the localized API.

Omit `instances`, or use an empty list, for objects without instances. Specify
one name or a list for exact instances. Partial patterns such as `"worker*"` are
expanded independently. If a list contains `"*"`, Go-compatible selection uses
the full wildcard and ignores other explicit non-aggregate names. Specify
`["*", "_Total"]` to retain `_Total`, or select `"_Total"` alone to collect only
that exact instance. Aggregate names are matched case-sensitively. Only a
literal empty `aggregation_name` uses `_Total`; whitespace-only and every other
nonempty value remain configured identity. Filtering occurs before expansion
limits, and explicit wildcard inclusion does not create a duplicate aggregate
query.

Points for named instances emit `instance=<name>`. Exact selectors derive that
identity from the configured native path, while wildcard-expanded selectors use
the concrete identity returned by PDH. Duplicate PDH instances use the
canonical suffix, such as `instance=process#1`; index zero omits `#0`.
An exact aggregate entry omits `instance`, including when other exact entries
are configured beside it, matching Go's per-entry watcher behavior. An
aggregate explicitly included with `"*"` retains it. The receiver also emits
`windows.perf_counter.instance`, optional
`windows.perf_counter.parent_instance`, and
`windows.perf_counter.instance_index` for lossless PDH identity.

Explicit PDH duplicate-instance selectors preserve their configured spelling:
`name`, `name#0`, `name#00`, and `name#01` remain distinct native paths and
lookup identities and emitted exact-path instance attributes. Wildcard
expansion still uses the concrete identity returned by PDH.
An instance such as `#5` also keeps that literal native name. Numeric indexes
are positional, not durable entity identifiers. If same-named processes churn
while their count stays unchanged, PDH can reuse `name#n` for a different
process and a two-sample calculation can span the old and new process. The
receiver cannot detect that entity change from the reused instance identity
alone. Where the target host provides it, prefer `Process V2`, whose instances
include the process ID as `name:PID`; provider availability varies by Windows
version and installed performance-counter set.

Unknown fields, explicit references to undefined metrics, duplicate instances,
invalid intervals, unsupported scales, and duplicate outputs are configuration
errors. A duplicate output has the same path ignoring case, the same metric
name, and the same static attributes. Optional metadata may be empty and metric
definitions may be unused. Metric definition names, explicit references, and
attribute keys and values preserve their exact configured spelling. Lookup is
case-sensitive and does not trim, so "cpu" and " cpu" are distinct names.
Omitted or literal empty references select implicit path naming;
every other reference uses exact lookup, so whitespace-only references report
an undefined metric unless an identical definition exists. Literal empty units
default to `"1"`; every nonempty unit and every description remains verbatim. A
definition is path-shaped for usage diagnostics only when its first configured
character is `\`. Exact references resolve any matching definition key,
including padded, malformed, alias, or overlong path-shaped keys; no match
reports an undefined metric. A path-shaped definition that is not selected by
an implicit configured path/template or an explicit reference produces one
usage warning. Metric definitions and configured wildcard templates have no
separate receiver-specific count limit. Exact paths and concrete wildcard
expansions share `max_expanded_counters`; exact paths that
exceed it are rejected during configuration. Metric names, units, descriptions,
and mapping attributes share immutable backing across explicit instance
expansions; configuration clones preserve that backing.
Configured paths are limited to 2047 UTF-16 code units. Expanded paths that
reach PDH's 2048-unit native limit are omitted and diagnosed.

## Supported counter families

<!-- markdownlint-disable MD013 -->

| Family | Native types | Samples | Output |
| --- | --- | --- | --- |
| Direct values | `PERF_COUNTER_RAWCOUNT`, `PERF_COUNTER_LARGE_RAWCOUNT`, and hexadecimal variants | One | Integer |
| Rates | `PERF_COUNTER_COUNTER`, `PERF_COUNTER_BULK_COUNT` | Two | Double |
| Delta counts | `PERF_COUNTER_DELTA`, `PERF_COUNTER_LARGE_DELTA` | Two | Double |
| Samples | `PERF_SAMPLE_COUNTER` | Two | Double |
| Queue lengths | DWORD, large, 100-nanosecond, and object-time queue variants | Two | Double |
| Timer percentages | System, 100-nanosecond, object-time, inverse, and precision timer variants | Two | Double |
| Multi-timer percentages | System and 100-nanosecond normal and inverse variants | Two | Double |
| Elapsed time | `PERF_ELAPSED_TIME` | One | Double |
| Raw fractions | `PERF_RAW_FRACTION`, `PERF_LARGE_RAW_FRACTION` | One | Double |
| Sample fractions | `PERF_SAMPLE_FRACTION` | Two | Double |
| Averages | `PERF_AVERAGE_TIMER`, `PERF_AVERAGE_BULK` | Two | Double |

<!-- markdownlint-enable MD013 -->

Use a Gauge for rates, ratios, utilization, and other non-additive current
values. A monotonic Sum asserts that the selected PDH counter is a running
count; it is accepted only for `PERF_COUNTER_RAWCOUNT`,
`PERF_COUNTER_LARGE_RAWCOUNT`, and their hexadecimal variants. A
non-monotonic Sum remains available for every supported native type, including
additive current values such as committed private memory. Metric names, units,
and descriptions must preserve the emitted semantics; Windows Private Bytes
includes committed memory that may reside in RAM or the page file and must not
be labeled as the physical-memory metric `process.memory.usage`.

A delta Sum accepts only the two native delta-count types. Its first valid
sample establishes an interval baseline and emits no point. Each later point
uses the immediately preceding valid sample timestamp as its start time.
Transient statuses, collection errors, timestamp failures, and handle
replacement clear readiness and require a fresh baseline. Equal or decreasing
timestamps are omitted and become the next baseline. A negative formatted
delta is emitted only when `monotonic: false`; a monotonic delta omits the
negative interval but remains ready for the next sample.

PDH performs rate, timer, fraction, and average calculations and associates
visible fraction, average, and multi-timer numerators with their
provider-defined base counters. Configure only the visible numerator path.
PDH also owns raw-fraction denominator interpretation and status handling; the
receiver emits or omits its formatted result without a separate raw read.
Multi-timers require a positive native `MultiCount`; an invalid zero base omits
the point and reports a counter-local failure. The raw sample still becomes the
baseline for the next interval because that calculation uses the next sample's
current `MultiCount`. Standalone base counters are not metrics and are rejected.
Elapsed-time counters format from the current sample and do not require warm-up.

All mappings emit regular PDH formatted values. Raw reads retain successive
sample readiness and guard the native `MultiCount` used by multi-timers.
Geneva-specific Full or Factored event formats are transport and schema choices
outside this receiver's OTAP metric contract.

Unsupported exact or expanded counter types are omitted and diagnosed without
suppressing healthy counters. They are not retried because their native type
cannot change while the handle exists. A missing or inaccessible exact counter
is omitted and retried, including when no configured counter is initially
available. Diagnostics include the native type.

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

The receiver opens the PDH query on its worker and performs one startup
collection without emitting points. This prime seeds valid native and
receiver-side sample history before `initial_delay` and the first scheduled
scrape. It records monotonic values for later reset detection but does not
establish their exported cumulative start. A priming collection, timestamp, or
refresh failure is retained as bounded diagnostics while the worker starts and
follows its normal retry and query-rebuild policy. No-data, invalid, or
counter-local failures retain their normal omission and diagnostic behavior
without fabricating readiness.
The initial pass has no fixed timeout. Drain or shutdown remains responsive and
requests worker cleanup, but a native call already in progress must return
before its query resources can be released. A transient `PdhOpenQueryW`
failure leaves that query slot pending with bounded exponential retry; healthy
slots can become ready after the same initial pass, and an entirely pending
set can recover on later scrapes. Thread-start, configuration, and invariant
failures still reject startup explicitly.

- Wildcards are expanded at startup and then at
  `wildcard_refresh_interval`. Concrete paths are joined by configured counter
  and full case-insensitive path, including PDH's `#n` duplicate index.
- One-sample counters can emit immediately. A valid startup prime normally
  makes two-sample counters eligible on the first scheduled scrape. Insufficient
  elapsed time or an invalid/missing prime still requires fresh warm-up.
- Delta Sum intervals require `0 < start_time < time_unix_nano`; each series
  tracks its own previous valid native sample timestamp.
- Removed instances stop emitting after the next discovery refresh.
- When a sample-fraction or average base does not advance, no relevant
  operation occurred; that value is omitted rather than replaced with zero. A
  decreasing base also omits the point and resets its baseline. This differs
  from the Go receiver and Windows Performance Monitor, which report an idle
  average as zero.
- `PDH_INVALID_DATA`, `PDH_NO_DATA`, and `PDH_CALC_NEGATIVE_*` statuses mean
  no observation. They emit no point, do not count as failures, and preserve
  the counter handle and monotonic Sum sequence. Saved PDH base history is
  cleared, affected two-sample counters restart warm-up, and delta interval
  baselines are invalidated so later values are not compared with stale PDH
  samples. Other counter-local invalid statuses, non-finite output, or scaling
  failures omit only that point; healthy peers remain in the batch.
- The native worker validates timestamps, Sum intervals, and finite values
  before projection. Broken sample/configuration identity and Arrow schema
  errors remain fatal receiver invariants.
- Exact counter add failures retry with exponential backoff capped by
  `collection_interval`; wildcard-expanded add failures are capped by
  `wildcard_refresh_interval`. Counter read failures omit only the affected
  point and retain the existing handle for the next scheduled collection.
- Counter availability does not fail startup. An empty active set emits no
  points until a configured counter becomes available.
- Query-level `PDH_NO_DATA` and the other transient no-observation statuses
  emit no batch and do not count as failures. They clear saved PDH base history,
  invalidate delta intervals, and restart two-sample warm-up while preserving
  cumulative monotonic Sum sequences. Other query-level collection failures
  and post-collection timestamp failures apply the same reset before returning
  an error. Query collection failures retry the existing query with bounded
  exponential backoff capped by `collection_interval`, independently of
  wildcard discovery. Three consecutive failures rebuild the affected
  worker-owned query and its counters.
- The receiver waits for every accepted scrape and delivers late results with
  their original point timestamps. Crossing the lateness threshold immediately
  records an overrun and emits a warning without cancelling the scrape. A
  positive `timeout` is the threshold even when it is shorter or longer than
  `collection_interval`; omitting it or setting `0s` uses the next scheduled
  collection tick. The warning re-arms only after an on-time scrape. No
  concurrent request is queued while the native call remains in flight.
- Wildcard expansion is sorted before applying the configured limits. Excess
  instances are omitted, reported explicitly, and reconsidered at the next
  discovery refresh; they are never presented as a complete expansion.
- New or removed wildcard members and terminal wildcard counter failures are
  reconsidered only at `wildcard_refresh_interval`, so a longer discovery
  cadence intentionally trades membership latency for lower provider cost.

Each emitted point uses the collection timestamp. Non-monotonic Sum points use
the current PDH query's opening time as their cumulative start time; a query
rebuild or backward wall-clock adjustment starts a new cumulative sequence.
Monotonic Sum points track sequence start independently for each exact or
expanded handle. The first point successfully delivered after open, recreation,
or rebuild uses its own timestamp as the unknown cumulative start; later
delivered points retain that start. A decrease, handle recreation, newly
discovered instance, or backward wall-clock adjustment relative to that
series' previous observation starts a new sequence for only that point.
Query-level clock rollback clears all monotonic Sum sequences; a discarded or
omitted series starts fresh at its next successfully delivered observation.
Equal timestamps do not reset a sequence. A value wrap is treated as a
decrease; the receiver does not compensate for it. Negative monotonic Sum
values are omitted as invalid. Gauge points do not use or validate cumulative
start time.

## Examples

From `rust\otap-dataflow`:

```powershell
cargo run --features windows-perf-counters --bin df_engine -- -c configs\windows-perf-counters-console.yaml
```

The basic
[`windows-perf-counters-console.yaml`](../../../../../configs/windows-perf-counters-console.yaml)
example contains Available Bytes and total Processor utilization.

The
[`windows-perf-counters-calculations-console.yaml`](../../../../../configs/windows-perf-counters-calculations-console.yaml)
example demonstrates rates, fractions, and averages.

The focused
[`windows-perf-counters-wildcard-console.yaml`](../../../../../configs/windows-perf-counters-wildcard-console.yaml)
example combines an exact Memory counter with
`\Process(*)\Private Bytes` and refreshes discovery every five seconds. It
uses only built-in Windows performance counters and demonstrates bounded
per-process expansion without requiring a separate test executable.

On hosts that expose `Process V2`, using that object instead gives each process
instance a `name:PID` identity that avoids positional duplicate-name reuse.
Verify availability and collection on the target host before changing the
example:

```powershell
Get-Counter -ListSet 'Process V2'
Get-Counter '\Process V2(*)\Private Bytes' -SampleInterval 1 -MaxSamples 1
```

To check the configuration structure without starting collection:

```powershell
.\df_engine.exe --validate-and-exit -c .\windows-perf-counters-calculations-console.yaml
```

Provider availability and native types are checked when the receiver starts,
not by `--validate-and-exit`.

## Limits

- The source pipeline must allocate one core.
- Multiple receiver nodes may use independent intervals. Each node owns one
  worker and one or more PDH queries; mappings with `recreate_query: true` use
  dedicated queries. Avoid unintentionally configuring duplicate collection.
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
`windows_perf_counters_receiver.counter.fail` with a configured path template
and low-cardinality reason. Other runtime events are:

- `windows_perf_counters_receiver.instance_limit.exceeded`
- `windows_perf_counters_receiver.instances.change`
- `windows_perf_counters_receiver.recovery.change`
- `windows_perf_counters_receiver.scrape.fail`
- `windows_perf_counters_receiver.scrape.timeout`
- `windows_perf_counters_receiver.shutdown.timeout`
- `windows_perf_counters_receiver.query.close.fail`

Repeated scrape and counter warnings are suppressed until the affected
condition recovers. Configuration events are:

- `windows_perf_counters_receiver.config.instance_override`
- `windows_perf_counters_receiver.config.metric_definition.unused_path`
- `windows_perf_counters_receiver.config.metric_definition.case_collision`

The instance-override warning reports a static `instance` value that can
collapse multiple selected native instances. Unused-path warnings retain the
original raw path-shaped definition key and do not claim that PDH would reject
it. Case-collision warnings list every configured spelling and are deduplicated
once per Unicode lowercase identity group.

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
