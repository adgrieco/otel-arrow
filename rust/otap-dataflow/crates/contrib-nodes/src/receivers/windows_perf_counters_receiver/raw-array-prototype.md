# Raw-array prototype

## Decision

Keep the per-handle production backend. The bounded N13 prototype reduced
native calls and handles, but a raw-array read materialized every provider
instance rather than only the receiver's bounded selection. On the measured
high-cardinality workload, that increased steady-state collection time,
retained memory, and the longest non-cancellable native operation.

Production replacement remains deferred until an alternative can preserve the
receiver's resource bounds and prove the per-instance history and provider
semantics listed below.

## Method

The temporary release-mode probe compared two paths on the same Windows host
and PDH collections:

- Per-handle: expand the wildcard, retain the first 256 concrete counters, and
  collect and format each concrete counter as production does.
- Raw array: add one wildcard counter, read the provider-wide raw array, retain
  the same 256 occurrence-aware identities, and calculate formatted values
  from the matching raw samples.

Each workload used five measured collections after setup. The probe recorded
wall and process CPU time, native call count, retained buffer size, handle
count, maximum single collection duration, observations, and omissions.
Occurrence-aware identity distinguished duplicate instance names.

The workloads were:

- `\Process(*)\% Processor Time`
- `\Thread(*)\Context Switches/sec`
- `\Thread(*)\ID Thread` for direct-value semantic comparison

The probe was removed after measurement; it was never a production backend.

## Representative results

The final comparison run retained 256 configured instances.

| Measurement | Process per-handle | Process raw array | Thread per-handle | Thread raw array |
| --- | ---: | ---: | ---: | ---: |
| Provider instances read | 256 | 423 | 256 | 7,938 |
| Native handles | 256 | 1 | 256 | 1 |
| Retained bytes | 14,152 | 51,846 | 14,695 | 1,034,738 |
| Setup wall time | 1.522 ms | 0.037 ms | 63.938 ms | 0.038 ms |
| Prime wall time | 25.051 ms | 38.645 ms | 100.250 ms | 520.358 ms |
| Five-sample wall time | 43.045 ms | 46.004 ms | 353.258 ms | 2,416.156 ms |
| Five-sample CPU time | 0.000 ms | 78.125 ms | 328.125 ms | 2,421.875 ms |
| Maximum sample operation | 8.812 ms | 9.480 ms | 71.884 ms | 483.987 ms |
| Native calls | 3,078 | 1,296 | 3,078 | 1,296 |

Windows process CPU accounting is quantized on this host; wall time and the
repeated CPU results establish the high-cardinality difference more reliably
than the zero Process per-handle value in this individual run.

Two additional completed comparisons measured the Thread path at 337-339 ms
per-handle and 2,419-2,444 ms by raw array. The raw-array path was about
6.8-7.2 times slower while retaining about 1.03-1.04 MB instead of about
14.7 KB. Process results remained close: 31-43 ms per-handle versus 36-46 ms
by raw array.

## Semantic and control gates

| Gate | Result |
| --- | --- |
| Rate values for the same 256 Thread identities | Passed: 256 compared, zero unmatched, maximum absolute difference 0 |
| Direct values for the same 256 Thread identities | Passed: 256 compared, zero unmatched |
| Observation and omission parity | Passed on a stable comparison; both paths produced 1,274 observations and 6 omissions |
| Duplicate instance identity | Passed for the probe through occurrence-aware matching |
| Receiver-wide 256-entry retention bound | Passed for emitted results |
| Provider-side work and memory bound | Failed: the raw-array API materialized about 7,940 Thread items |
| Native handle reduction | Passed: 256 handles reduced to 1 |
| Native call reduction | Passed: 3,078 calls reduced to 1,296 |
| Discovery latency | No improvement: bounded selection still required wildcard discovery, measured near 2.5 seconds for Thread |
| Retry and control responsiveness | Regressed: maximum non-cancellable work increased from about 72 ms to 484 ms |
| Membership churn and replacement history | Not established |
| Reset and rollback history | Not established |
| Counter-family status and base semantics | Not established beyond the measured direct and rate families |
| Provider ordering stability | Not established |

The prototype also observed changing Process and Thread membership between
reads. That reinforces the need to preserve stable receiver identity and
history independently of provider array position.

## Recommendation

Do not replace the production backend with the measured raw-array design.
Fewer calls and handles do not compensate for provider-wide work, substantially
higher high-cardinality latency and memory, a longer non-cancellable control
window, and the remaining semantic gaps. Any future replacement should first
bound provider-side materialization or move it off the control path, then prove
membership churn, retry, reset, counter-family, ordering, and history behavior
against the per-handle backend.
