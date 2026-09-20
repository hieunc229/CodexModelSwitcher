# Context compaction audit

Date: 2026-09-18. Baseline: `3bc6eca`. Implementation branch: `codex/context-audit`.

## Root issue and remaining uncertainty

The router hardening patch is already activated. Its heartbeats, stream failure diagnostics, catalog provenance and native image routing do not control automatic compaction. Codex owns compaction and Harbor routes the resulting request. The earlier connection resets are a separate incident; the old records did not identify their initiating peer. New failure records distinguish upstream and downstream observations without retaining content.

A local read-only audit of the affected rollout reproduces eight consecutive short cycles beginning at lines 15822 through 15974. Each has one normal provider request, with input between 140,277 and 140,870 tokens against an observed effective context window of 121,600. Compaction repeats after 19–41 seconds. The next cycle begins under the same small window but continues through the later correction, so it is not another one-request loop.

Later cycles observe 258,400 effective tokens. This is evidence from task usage, not an inference from a catalog file. It is consistent with 95% of the 272,000 default. The 872,000 advertised maximum is neither the selected context nor independently verified Azure capacity.

Three completed examples, all on September 18 UTC:

| Cycle start | Local estimate | First provider total | Cached subset | Uncached input | Uncached minus local estimate | Last normal input | Normal requests | Seconds |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 21:30:40.736 | 20,653 | 144,035 | 129,025 | 15,010 | -5,643 | 244,545 | 20 | 261.688 |
| 21:35:02.424 | 20,074 | 143,558 | 129,025 | 14,533 | -5,541 | 233,813 | 29 | 420.919 |
| 21:42:03.343 | 18,380 | 141,882 | 129,025 | 12,857 | -5,523 | 238,545 | 24 | 261.872 |

The first example includes 27 tool outputs totaling 626,137 compact ASCII JSON bytes. This is a serialization metric, not a token count or the exact provider wire size. The compactor's own response is excluded by response ID in all three examples.

The provider usage records attribute the dominant difference to a fixed cached prefix. Every sampled first request in both the 121,600- and 258,400-window periods reports 129,025 cached input tokens. Cached input is already included in total provider input and must not be added again. After subtracting it once, the sampled post-compaction requests contain 11,824–15,010 uncached input tokens, while the local estimates are 16,249–20,653. The remaining difference is negative by 4,957–5,643 tokens. That proves the two counters use different scopes; it does not prove missing history or duplicated context.

The fixed value is evidence of a stable cached request prefix. It does not identify which request component produced those tokens. The rollout also records a stable full world-state payload whose permissions section contains 567 approved command prefixes, but serialized bytes are not token counts. Request metrics added by this patch can measure instructions, tools, inputs, images and encrypted payloads at the gateway boundary when `MODEL_HARBOR_REQUEST_METRICS=1`. They remain disabled by default, retain only 32 content-free records and do not alter request bytes.

At the final audit checkpoint, the active task reported an effective window of 475,000 while the published catalog reported a 500,000 default and maximum. The latest completed cycles above predate that observation and retain their contemporaneous 258,400 window. The native capture did not contain the currently selected model, and desktop catalog adoption remained unknown. The audit does not substitute catalog metadata for task evidence.

## Implementation and files

- `scripts/audit-context.py` streams an explicitly supplied local rollout read-only and emits JSON with recent compaction cycles.
- `ModelHarbor/Support/request_metrics.py` records bounded content-free request component sizes and provider usage when explicitly enabled.
- `scripts/verify-context-capacity.py` verifies one exact synthetic payload against one explicitly supplied deployment. It does not discover credentials or modify catalogs.
- `experiments/chat_compat.py` is an isolated Responses-to-Chat-Completions pilot. No production route imports it.
- The focused Python tests cover accounting, confidentiality, malformed and partial records, request-byte parity, concurrency, capacity gating, translation, streaming, usage and fail-closed unsupported features.
- `Package.swift` excludes the new Python test from SwiftPM.
- `docs/codex-router-comparison.md` links this follow-up without reopening the deployed patch.

Run `python3 scripts/audit-context.py /explicit/local/rollout.jsonl --limit 10`. No automatic session discovery, source path, title, prompt, tool output, response ID or raw model string is emitted. Model changes are represented by numeric epochs instead of untrusted identifiers. The input is not rewritten.

Memory grows with the largest single JSONL record, up to 1,000 retained cycle summaries, and the last 256 response IDs. It does not grow with the total file length. A record containing an image can still be large.

## Accounting and risks

Only distinct identified `token_usage_record` entries count as provider requests. Repeated `token_count` snapshots and cumulative thread totals do not. Cached input is already part of input tokens. Missing usage stays unknown; the script never invents tokens from bytes.

The latest distinct provider record is held until the next record or compaction so the compactor can be excluded by response ID. Nonadjacent matching usage is explicitly marked unresolved, not silently claimed excluded. An open cycle may still include a compactor whose completion record has not arrived. Deduplication covers only the last 256 IDs.

A model change clears the currently observed window until a new observation arrives. Earlier request measurements retain their contemporaneous window and estimate. A trailing incomplete record is reported separately from malformed interior JSON. Reading an actively appended file is not an atomic snapshot. Reports reflect the records read during that invocation.

These constraints prevent content disclosure and false precision. No runtime, routing, thresholds, catalogs, conversation history, or global settings were changed.

## Focused issues and disposition

1. [SAY-3359](https://linear.app/sayvant/issue/SAY-3359): implemented the reproducible content-free rollout audit.
2. [SAY-3360](https://linear.app/sayvant/issue/SAY-3360): attributed the dominant difference to a fixed cached prefix and added opt-in gateway composition metrics. No request component was removed because the evidence does not show redundant content.
3. [SAY-3361](https://linear.app/sayvant/issue/SAY-3361): implemented single-snapshot validation, source-race refusal, atomic publication through the existing private writer and exclusion of merged catalog metadata from gateway route-binding revisions. Manual limits and hidden routes remain intact.
4. [SAY-3362](https://linear.app/sayvant/issue/SAY-3362): implemented separate task, published, native and current-client evidence with explicit unknown, stale, mismatch and model-change states.
5. [SAY-3363](https://linear.app/sayvant/issue/SAY-3363): implemented the verification gate and confirmed there is no evidence to enable a larger option yet. Harbor leaves it unavailable until a bounded synthetic payload passes against the exact deployment.
6. [SAY-3364](https://linear.app/sayvant/issue/SAY-3364): implemented and tested the isolated compatibility pilot. It remains intentionally unwired because no Chat-Completions-only production provider was identified.

Each Linear issue contains its implementation map, acceptance criteria, risks and required tests. Instrumentation precedes any pressure-reduction change. Catalog and compatibility work must not silently change active task bindings or retry uncertain provider work.

## Verification

- Seven initial regression tests failed because the CLI did not exist, then passed with the implementation.
- An added model-switch regression exposed loss of a pending request's estimate. The implementation now captures estimates with each pending request.
- The audit, request-metrics and capacity suites pass: 32 tests.
- The isolated compatibility suite passes: 13 tests. The router-hardening regression suite passes: 9 tests.
- Real rollout audit at one checkpoint: 43 compactions, no malformed records. The task was still appending; totals are not a permanent task-wide count.
- The final full Python suite passes: 478 tests in 126.941 seconds. The final full Swift suite passes: 108 tests with no failures.
- The Xcode app build succeeds. Strict deep signature verification passes for the built app.
- Independent review found three material gaps. The final patch restricts the credential-bearing capacity probe to direct Azure resource hosts, rolls catalog publication back if a source changes during the write, and normalizes the Responses API's nested cached-token field. The added regressions pass: 11 focused Python tests and 23 Swift safety tests.

## Execution checklist

- [x] Audit source, deployment evidence and local task usage before editing.
- [x] Create six focused Linear issues with dependencies.
- [x] Implement and test the read-only audit in an isolated worktree.
- [x] Reproduce the short-loop and corrected-window patterns without publishing task content.
- [x] Attribute the request difference with cached-input evidence and nonmutating measurements.
- [x] Reconcile catalogs and report observed versus advertised context separately.
- [x] Add a fail-closed larger-context verification gate; keep the option unavailable because capacity is unverified.
- [x] Evaluate the optional compatibility adapter in isolation; keep it out of production routing.
