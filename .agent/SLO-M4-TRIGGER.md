# M4 trigger gate — pipeline-worker extraction

**Status:** Defined 2026-05-30 (hub#1005). Auditable. Trip when both
PromQL clauses below hold simultaneously for the stated window.

## Why this exists

ROADMAP R5 flagged "P6 trigger ambiguity" as a real risk: the spoken
trigger condition for M4 — *"HTTP p99 incidents under pipeline load"* —
was narrative, not decidable. Two reasonable operators would have
disagreed about whether the trigger had fired in any given week,
which is a slow path to either declaring extraction too early (cost
without payoff) or too late (production stress before the fix).

This doc replaces the narrative with a tracked PromQL expression
backed by two committed metrics. When the expression holds, M4 work
is justified and the migration kicks off the planning checklist
already drafted in ROADMAP. When it doesn't hold, M4 stays parked.

## Metrics this depends on

Both committed in `src/observability/metrics.ts` as of hub#1005:

| Metric | Type | Labels | Source |
|---|---|---|---|
| `pipeline_executor_busy_seconds_total` | counter | `service`, `node_id` | `recordPipelineExecutorBusy(seconds)` called from the in-process pipeline executor when it completes a run unit. `rate(...[1m])` gives fraction-busy in 0..1. |
| `http_handler_duration_seconds` | histogram | `service`, `node_id`, `route_group` | `src/middleware/http-metrics.ts` observes every request after `res.on('finish')`. The `route_group` label is a bounded enum (see `deriveRouteGroup`). |

The pipeline executor busy-counter wiring is a follow-up — the metric
is pre-registered so a scrape sees zero before traffic, but the call
sites inside `pipeline-runtime` / the in-process executor need to land
along with the M4 planning work. Until then, the trigger gate is
effectively disabled (busy fraction is always 0), which is the correct
"don't fire prematurely" default.

## Fire condition (PromQL)

The gate fires when **both** clauses below hold for 5 consecutive
minutes:

```promql
# Clause 1 — HTTP p99 latency on /api/* request groups exceeds 500ms.
histogram_quantile(0.99,
  sum by (le, route_group) (
    rate(http_handler_duration_seconds_bucket{
      service="platform-api",
      route_group=~"api_.*"
    }[5m])
  )
) > 0.5

AND

# Clause 2 — pipeline executor is busy more than 80% of wall-clock time.
sum(rate(pipeline_executor_busy_seconds_total{service="platform-api"}[5m])) > 0.8
```

Operationally this reads as: *"when the in-process pipeline executor
is saturating wall-clock time (≥80%) AND that saturation correlates
with degraded HTTP p99 (> 500ms) on the user-facing routes, the
in-process model has run out of headroom."*

5 minutes is the dwell so a single spike doesn't trigger; a sustained
condition does.

## What "triggering" means

- The operator captures the firing window (timestamps + PromQL
  screenshot) and links it from this doc.
- The M4 planning checklist in ROADMAP becomes the next focus.
- Scope freeze: no new in-process pipeline executor features land
  while extraction is in flight.

The gate is one-shot per release line — once fired and acted on, the
checklist drives the actual cutover; the gate doesn't fire repeatedly
to re-justify each work item.

## What does NOT trigger M4

- Single-request slow paths (the histogram is over `route_group`, not
  per route, so a slow `/api/pipelines/p1/runs` POST is absorbed into
  the group p99).
- Background work alone (executor busy at 90% without HTTP latency
  degradation means the in-process model is still keeping its
  contract).
- Latency from upstream services we already route through breakers
  (LVS, etc.) — those have their own SLOs and don't justify extraction.

## Anti-flap

5-minute dwell is the primary anti-flap. If false positives appear
during ordinary maintenance (deploys, registry bootstraps), raise
the dwell to 10 minutes here before adding exceptions to the PromQL
expression — clean conditions beat clever ones.

## Open questions (resolve before first trip)

- Where exactly does `recordPipelineExecutorBusy(seconds)` get called?
  The in-process executor doesn't have a single "I'm doing work now"
  span today; we may want to wrap it in `pipeline-runtime` so the
  metric is consistent across consumers.
- The 0.8 busy threshold is a starting hypothesis. After the executor
  wiring lands, run a 2-week baseline before trusting the gate; tune
  if real workload p99-busy lives in a different band.

These are tracking notes — not blockers for the gate definition.
