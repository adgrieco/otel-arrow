# Windows Performance Counters Receiver

<!-- markdownlint-disable MD013 -->

## Metadata

- Type: `receiver:windowsperfcounters` (`urn:otel:receiver:windowsperfcounters`)
- Feature gate: `windowsperfcounters`
- Stability: Experimental

## Overview

The receiver uses Windows Performance Data Helper (PDH) to collect performance
counters and emit OTAP metrics. Use canonical, locale-independent Windows
object and counter names rather than localized display names. For counters
divided into instances, specify the exact instance or instances to collect.

## Getting Started

Start with one emitted metric mapped to one Windows performance counter:

```yaml
type: receiver:windowsperfcounters
config:
  metrics:
    windows.memory.available:
      description: Physical memory immediately available for allocation.
      unit: By
      gauge: {}
  perfcounters:
    - object: Memory
      counters:
        - name: Available Bytes
          metric: windows.memory.available
```

The two configuration sections have different roles:

- `metrics` defines the OpenTelemetry metric name, description, unit, and kind.
- `perfcounters` selects Windows values and maps each counter to a metric.

In this example, the receiver reads `\Memory\Available Bytes` and maps the raw
value to `windows.memory.available`. The collected value is emitted as an
OpenTelemetry Gauge into the OTAP pipeline with these logical fields:

```text
resource:
  attributes:
    os.type: windows
metric:
  name: windows.memory.available
  description: Physical memory immediately available for allocation.
  unit: By
  type: gauge
  data_point:
    value: <available bytes>
    attributes:
      windows.perf_counter.path: \Memory\Available Bytes
```

## Configuration

### Receiver Fields

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `metrics` | map | **required** | OpenTelemetry metric definitions keyed by emitted metric name. |
| `perfcounters` | list | **required** | Windows performance-counter objects and counter-to-metric mappings. |
| `collection_interval` | duration | `30s` | Receiver-wide time between collections; must be from `1s` through `24h` (`1day`). |
| `initial_delay` | duration | `1s` | Receiver-wide delay before the first collection; must be from `0s` through `24h` (`1day`). |

`metrics` and `perfcounters` must each contain at least one entry. Every metric
definition must be referenced by a counter mapping.

`collection_interval` and `initial_delay` set one schedule for all counters in
the receiver. Use separate receiver nodes for groups that need different
schedules.

### Metric Definitions

Each entry in `metrics` supports these fields:

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `description` | string | **required** | Non-empty OpenTelemetry metric description. |
| `unit` | string | **required** | Non-empty OpenTelemetry metric unit. |
| `gauge` | object | *none* | Emits an OpenTelemetry Gauge. |
| `up_down_counter` | object | *none* | Emits a cumulative, non-monotonic OpenTelemetry Sum. |

Each metric must contain exactly one of `gauge: {}` or
`up_down_counter: {}`.

### Performance Counter Objects

Each entry in `perfcounters` supports these fields:

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `object` | string | **required** | Canonical Windows performance object name. |
| `instances` | string or list | *none* | Exact instance name or list of names; omit for counters without instances. |
| `counters` | list | **required** | One or more counter-to-metric mappings. |

Each entry in `counters` supports these fields:

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `name` | string | **required** | Canonical Windows performance counter name. |
| `metric` | string | **required** | Metric defined in `metrics`. |
| `attributes` | string map | `{}` | Static attributes added to each emitted data point. |
| `scale_power10` | integer | `0` | Multiplies each value by `10^scale_power10`; range `-18` through `18`. |

Every emitted data point includes `windows.perf_counter.path`. The resource
includes `os.type=windows`.

### Examples

#### Collecting Multiple Counters

This example combines counters without instances, one exact instance, both
metric kinds, and explicit collection timing:

```yaml
type: receiver:windowsperfcounters
config:
  metrics:
    windows.memory.available:
      description: Physical memory immediately available for allocation.
      unit: By
      gauge: {}
    windows.processor.time:
      description: Average processor utilization across all logical processors.
      unit: "%"
      gauge: {}
    windows.system.processes:
      description: Current number of processes in the system.
      unit: "{process}"
      up_down_counter: {}
  perfcounters:
    - object: Memory
      counters:
        - name: Available Bytes
          metric: windows.memory.available
    - object: Processor
      instances: _Total
      counters:
        - name: "% Processor Time"
          metric: windows.processor.time
    - object: System
      counters:
        - name: Processes
          metric: windows.system.processes
  collection_interval: 15s
  initial_delay: 2s
```

The configuration above produces:

| Windows counter | Emitted metric | Configured kind | Emitted metric type |
| --- | --- | --- | --- |
| `\Memory\Available Bytes` | `windows.memory.available` | `gauge` | Gauge |
| `\Processor(_Total)\% Processor Time` | `windows.processor.time` | `gauge` | Gauge |
| `\System\Processes` | `windows.system.processes` | `up_down_counter` | Sum (cumulative, non-monotonic) |

#### Selecting Multiple Instances

Omit `instances` for counters without instances, use a string for one exact
instance, or use a YAML list for several exact named instances. For example,
on a host with `C:` and `D:` volumes:

```yaml
type: receiver:windowsperfcounters
config:
  metrics:
    windows.logical_disk.free_space:
      description: Percentage of free space on the logical disk.
      unit: "%"
      gauge: {}
  perfcounters:
    - object: LogicalDisk
      instances: ["C:", "D:"]
      counters:
        - name: "% Free Space"
          metric: windows.logical_disk.free_space
```

#### Adding Data Point Attributes

Add `attributes` to counter mappings to attach static, queryable dimensions to
their data points:

```yaml
type: receiver:windowsperfcounters
config:
  metrics:
    windows.processor.time:
      description: Processor time by state.
      unit: "%"
      gauge: {}
  perfcounters:
    - object: Processor
      instances: _Total
      counters:
        - name: "% Processor Time"
          metric: windows.processor.time
          attributes:
            state: active
        - name: "% Idle Time"
          metric: windows.processor.time
          attributes:
            state: idle
```

Both mappings emit `windows.processor.time`. The `state` attribute lets
downstream consumers filter, query, or group its `active` and `idle` points
directly without changing their values.

Each attribute is a user-defined string key/value pair. In `state: active`,
`state` names the dimension and `active` classifies the point. Names beginning
with `windows.perf_counter.` are reserved for receiver-generated attributes.

#### Scaling Values

Use `scale_power10` to multiply the native counter value by a base-10 power.
This example converts bytes to decimal megabytes:

```yaml
type: receiver:windowsperfcounters
config:
  metrics:
    windows.memory.available:
      description: Available physical memory in decimal megabytes.
      unit: MBy
      gauge: {}
  perfcounters:
    - object: Memory
      counters:
        - name: Available Bytes
          metric: windows.memory.available
          scale_power10: -6
```

For example, `86,580,518,912` bytes multiplied by `10^-6` is emitted as
`86,580.518912 MBy`.

Direct counters remain integers with zero or positive decimal scaling. Negative
scaling, rates, timers, fractions, and averages are emitted as finite doubles.

## Telemetry

The receiver exposes the `receiver.windowsperfcounters.scrapes` and
`receiver.windowsperfcounters` metric sets. Common engine runtime metric sets
may also be attached by the pipeline telemetry policy.

### Metric Set

| Metric set | Metric | Attributes | Unit | Description |
| --- | --- | --- | --- | --- |
| `receiver.windowsperfcounters.scrapes` | `attempts` | `outcome`: `success` or `failure` | `{scrape}` | PDH collection attempts by terminal outcome. |
| `receiver.windowsperfcounters` | `failed_counter_values` | *none* | `{metric}` | Counter values omitted because their read or calculation failed. |
| `receiver.windowsperfcounters` | `scrape_overruns` | *none* | `{scrape}` | Scrapes that timed out or were skipped while the PDH worker remained busy. |

### Events

| Event | Severity | Description |
| --- | --- | --- |
| `otelcol.node.windowsperfcounters.scrape.fail` | `warn` | Windows counter collection began failing, so the current scrape was skipped. Repeated failures are suppressed until collection recovers. |
| `otelcol.node.windowsperfcounters.scrape.timeout` | `warn` | A PDH collection exceeded the collection interval. Repeated timeout and busy warnings are suppressed until collection recovers. |
| `otelcol.node.windowsperfcounters.counter.fail` | `warn` | One counter began failing while healthy counters continued. Repeated failures are suppressed until that counter recovers. |
| `otelcol.node.windowsperfcounters.shutdown.timeout` | `warn` | The worker did not finish cleanup within the shutdown allowance. |
| `otelcol.node.windowsperfcounters.close.fail` | `warn` | Windows reported an error while closing the counter query. |

## Limits

- Windows only.
- Object and counter names must use the canonical, locale-independent Windows
  names. Localized display names are not accepted.
- Object, counter, and instance names must be exact. Wildcards and automatic
  instance discovery are not supported.
- Invalid counter paths and unsupported native types fail during runtime
  receiver initialization. `--validate-and-exit` validates configuration
  structure but does not open the PDH query or detect these errors. If PDH
  accepts an unavailable exact instance, its read failure is omitted and
  retried on later scrapes.
- A read or calculation failure omits only the affected counter while healthy
  counters continue. The receiver tries that counter again on later scrapes.
- Each scrape is limited to the collection interval. After a timeout, later
  ticks are skipped while the single PDH worker remains busy, and collection
  resumes if the native call returns. A blocked native PDH call cannot be
  cancelled, but no additional scrape work is queued.
- The receiver does not rebuild its PDH query after startup.
- The source pipeline must use one core because the counters describe the whole
  host rather than one pipeline core.
- Only one scrape can be in progress, and samples are not buffered between the
  receiver and downstream node.

### Native Counter Type Support

This first increment intentionally supports only the native PDH types listed
below. A configured counter with any other native type fails startup rather
than emitting a potentially misleading value.

| Status | Native PDH types | Notes |
| --- | --- | --- |
| Supported | `PERF_COUNTER_RAWCOUNT`, `PERF_COUNTER_LARGE_RAWCOUNT`, `PERF_COUNTER_RAWCOUNT_HEX`, `PERF_COUNTER_LARGE_RAWCOUNT_HEX` | Direct integer counts. |
| Supported | `PERF_COUNTER_COUNTER`, `PERF_COUNTER_BULK_COUNT`, `PERF_COUNTER_DELTA`, `PERF_COUNTER_LARGE_DELTA`, `PERF_SAMPLE_COUNTER` | Rates, deltas, and sample counters calculated from two samples. |
| Supported | `PERF_COUNTER_TIMER`, `PERF_COUNTER_TIMER_INV`, `PERF_100NSEC_TIMER`, `PERF_100NSEC_TIMER_INV`, `PERF_OBJ_TIME_TIMER`, `PERF_PRECISION_SYSTEM_TIMER`, `PERF_PRECISION_100NS_TIMER`, `PERF_PRECISION_OBJECT_TIMER` | Non-multi system, 100-nanosecond, object-time, inverse, and precision timers, including `% Disk Time`. |
| Supported | `PERF_COUNTER_QUEUELEN_TYPE`, `PERF_COUNTER_LARGE_QUEUELEN_TYPE`, `PERF_COUNTER_100NS_QUEUELEN_TYPE`, `PERF_COUNTER_OBJ_TIME_QUEUELEN_TYPE` | Queue-length counters, including common counters such as `Avg. Disk Queue Length`. |
| Supported | `PERF_RAW_FRACTION`, `PERF_LARGE_RAW_FRACTION`, `PERF_SAMPLE_FRACTION`, `PERF_AVERAGE_TIMER`, `PERF_AVERAGE_BULK` | Raw fractions, sampled fractions, and averages. |
| Deferred from this increment | `PERF_ELAPSED_TIME` | Requires an explicit contract for elapsed-duration versus start-timestamp semantics and corresponding tests. Future support is not yet committed. |
| Deferred from this increment | `PERF_COUNTER_MULTI_TIMER`, `PERF_COUNTER_MULTI_TIMER_INV`, `PERF_100NSEC_MULTI_TIMER`, `PERF_100NSEC_MULTI_TIMER_INV` | Requires dedicated multiplier/base handling and live validation. Future support is not yet committed. |
| Rejected as standalone metrics | Text and standalone base counter types | These are non-numeric values or auxiliary inputs to another counter rather than independently meaningful numeric metrics. |

## Related Docs

- [Runnable example](../../../../../configs/windowsperfcounters-console.yaml)
- [Receiver proposal and planned scope](https://github.com/open-telemetry/otel-arrow/issues/4074)
- [Configuration model](../../../../../docs/configuration-model.md)
- [Contrib node catalog](../../../README.md)
