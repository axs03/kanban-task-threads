# Known issues

## C5-FIX4: deferred recovery can revive a resolved preactivation blocker

**Status:** confirmed regression; repair deferred by the owner on October 21,
2026. The owner requested: “lets skip this regression for now, document this
potential repair that needs to be done”. This is a scoped exception for C5,
not a passing correctness review. The owner subsequently authorized committing
and pushing the repair to the fork's existing feature branch with C5 deferred.
No deployment, restart, or live Discord test is authorized by those decisions.

### Affected behavior and impact

In `first_blocked` mode, a forum-setup failure can leave a persisted activation
watermark ahead of the publisher cursor. If a later contradictory `gave_up`
event quarantines an unmapped task, the deferred queue can retain older events
that normal first-block eligibility had already excluded. After explicit manual
reconciliation and restart, recovery can create an unwanted thread and publish
resolved preactivation blocker history. No genuine postactivation blocker is
required for this failure.

This is an extra-thread/historical-notification defect. The reproduction does
not mutate native task status or rewind activation, scans, or the board cursor.
It is demonstrated offline, not observed or exercised on the live installation.
The recovery repair containing this issue was not deployed when documented.

### Reproduction

1. Retain `blocked` event 1 and `completed` event 2 for a currently done task.
2. Enable `first_blocked`. Allow activation watermark 2 to persist, then fail
   forum setup before the publisher advances its cursor from 0. The task is not
   a current-block bootstrap obligation.
3. Append contradictory `gave_up` event 3 and retry the consumer. Normal
   eligibility excludes events 1/2 and quarantines event 3, but deferred retention
   also saves events 1/2.
4. Explicitly reconcile the task quarantine, reopen plugin state, and poll.
5. Actual result: a thread is created with `blocked`/`completed` replies from
   events 1/2. Required result: no thread and no historical replies, because no
   qualifying postactivation blocker exists.

### Root cause and proposed repair

The forward quarantine path retains the entire fetched task batch. Deferred
rows use independent `event_id - 1` replay positions; replay eligibility for an
unmapped task does not preserve the original activation floor. Retaining exact
source events therefore loses their original eligibility boundary.

Preserve the unmapped first-block activation/eligibility boundary when retaining
and replaying deferred forward batches. Account for already persisted deferred
entries as well as newly retained ones. Do not blindly replay or discard all
history: keep valid postactivation continuation, current-block bootstrap,
mapped-thread ordered updates and durable receipts intact.

Do not repair this by rewinding activation/cursors/scans, resetting migration,
creating from an old trigger, weakening blocker classification, or automatically
rearming poison events, unknown creates, tombstones, or dead letters.

### Required future verification

- Add permanent behavior regressions and record RED before production changes.
- Exercise both minimal persisted-state and actual forum-failure/retry sequences,
  including explicit reconciliation and restart.
- Check events equal to and below activation remain excluded for resolved,
  unmapped tasks; valid events above activation remain recoverable.
- Preserve eventless/current-block bootstrap, mapped update ordering, acknowledged
  receipts, quarantine holds and normal destination/lease/backoff guards.
- Keep the original activation638/cursor703/scan703/`gave_up`703 incident recovery
  working without using the older blocked580/unblocked595 history.
- Re-run the earlier C1–C4 and security regressions, then obtain independent
  correctness acceptance for the repaired snapshot. Revalidate security for any
  changed production paths.

### Evidence and review status

The independently reviewed fix4 source manifest is
`8f6cdcf804ee389ad2000eaa4399bbcce6d471e525bb2dfdf4deeb3f045e029f`.
It covers 265 source files and 14 changed/new feature paths; this documentation
was added afterward and is not part of that reviewed snapshot.

Local evidence root:
`/opt/data/workspace/kanban-task-threads-multiboard-review/blocked-event-coverage/`.

- `correctness-review/fix4-independent/REPORT.md` and `verdict.json`:
  C1–C4 closed; correctness **FAIL** solely on blocking finding C5-FIX4.
- `correctness-review/fix4-independent/test_activation_boundary.py`:
  both activation-boundary probes failed on fix4, with zero application network
  attempts. Canonical output: `logs/activation-natural-and-minimal.log`.
- `security-review/fix4-independent/REPORT.md` and `verdict.json`:
  independent security **PASS** on the exact fix4 snapshot.
- `implementation/RESULTS-fix4.md` and `results-fix4.json`:
  full offline suite 957 passed, native producers 6 passed, startup 7 passed with
  1 expected skip. These green gates did not detect C5 and do not override it.

### Separate retained-evidence limitation (N1)

If retained native evidence is modified or purged, recovery fails closed and may
quarantine all active obligations for the task. The current manual reconciliation
operation is task-wide: it suppresses all quarantined originals for that task,
including otherwise intact entries in that quarantined set. It is **not** a
lossless granular repair after corrupt history. This is a documented limitation,
not the intact-history activation regression above; granular recovery is separate
future work.
