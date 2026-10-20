# Multi-board / first-block extension

This fork adds opt-in behavior; it does not install or enable itself. The version
remains the pinned upstream version until a separately reviewed release.

## Central publisher, existing profiles

One existing publisher (for example `default`) reads each configured board's
**durable `task_events`**, regardless of creator, assignee, reassignment, actor or
worker profile. Worker hooks are optional accelerators, not delivery dependencies.
Explicit-registry kicks retain the publisher's registration context; they do not
adopt a worker's board/fence/secret ContextVars. A fenced registration is still
rejected, not unfenced by the plugin.
No new profile, cron job, task subscription or second plugin installation is
needed. `publisher_profile` still controls which loaded profile can publish; an
empty value retains lease candidacy rather than promising profile failover when
only one profile has the complete registry and scoped credentials.

See [`multi-board.example.yaml`](multi-board.example.yaml) for three boards sharing
one synthetic forum and one named webhook reference. Changing each route's public
forum ID and secret reference supports separate forums too. A shared endpoint,
including two secret names resolving to the same webhook, is valid. Each board
must nevertheless provide its own explicit complete route: no catch-all route,
current-board fallback or ambient/global bot token is used in `boards` mode.

## Configuration contract

- **Omit `boards`** to retain the legacy `board`/current-board resolver and legacy
  webhook/bot variable names. With neither new option, rendering, provisioning,
  secrets and delivery policy retain upstream semantics.
- **Present `boards`** requires a nonempty list of objects. `null`, `[]`, booleans,
  strings, duplicate board entries, unknown keys and incomplete routes are errors,
  never a request to fall back. Shared forums/webhooks are not duplicate boards.
- Required entry keys: `board`, `forum_channel_id`, `webhook_secret`.
  Optional: `bot_token_secret`, `discord_applied_tag_ids`, `dashboard_url`.
- Board slugs are canonical lowercase ASCII alphanumeric/hyphen/underscore, 1–64
  characters, beginning with an alphanumeric. No stripping/lowercasing or traversal
  repair occurs. Boards and native databases must already exist.
- Forum and tag IDs are nonzero decimal **strings**, up to 20 digits. Tag lists
  cannot contain duplicates. Global nonempty `board` and global destination tag
  IDs conflict with `boards`; put tags in the entries. A global dashboard URL can
  be overridden per entry; `{board}` and `{task_id}` are substituted as before.
- Secret fields are uppercase environment-style **names** (`[A-Z_][A-Z0-9_]*`,
  at most 128 characters), never URLs, `${...}`, `op://...` or literal tokens.
  Store secret sources in the publisher's supported Hermes secret setup. The
  scoped `agent.secret_scope.get_secret()` API resolves each name. A missing
  explicitly named bot secret invalidates the registry; an omitted bot field
  means webhook-only, even if a global bot secret exists.
- `thread_creation: all` (default) preserves upstream all-card provisioning.
  `thread_creation: first_blocked` enables the behavior below, with or without
  an explicit `boards` registry. Any other value is invalid.
- Shared cosmetic options, templates, `reply_on`, avatars, heartbeat staleness,
  workspace privacy and comment excerpts remain available.

The manifest cannot express conditional/dynamic secret requirements. Therefore it
advertises the legacy names under `optional_env`, while **runtime still requires
the legacy webhook when `boards` is absent**. Missing scoped context and transient
preflight errors defer startup; missing named credentials in an established scope
or invalid routes disable activation until reload. No multi-board activation
requires an unrelated legacy webhook variable.

## First block and initial catch-up

Each `(board, task_id)` has one active mapping. New thread titles include
`board:task_id`, preserving identity even when card IDs and titles collide in a
shared forum. Reblocks, reassignment, completion and later events reuse that
thread; never-blocked cards have neither threads nor pre-block replies.

On first activation, one native SQLite read snapshot supplies the event-ID
watermark and the set of **currently blocked cards**. The plugin transactionally
persists that watermark plus bootstrap obligations in its own state DB before
advancing event consumption. It does not reset existing cursors/mappings.
Already-resolved historical blockers do not flood the forum. Existing mapped
cards retain their history and normal lifecycle delivery. Bootstrap obligations
survive a 429, restart, and a card resolving before the next successful attempt.

After activation, a durable `blocked` event qualifies even if an `unblocked` or
`completed` event follows before the next poll. Replies start at the qualifying
block, not at unrelated pre-block activity. A concurrently committed block above
the snapshot watermark is consumed normally; a card blocked in the snapshot
remains an obligation even if it resolves during activation. Current status cards
show current state, so a rapidly resolved blocker can open with a ready/done card
and append its blocked/unblocked event history. Existing mapped cards continue
publishing even when they are not currently blocked.

## Ownership, leases and fail-closed routing

The full registry, all credentials and all read-only destination preflights are
validated before any new state file or publication is created. The discovered
webhook ID must match its credential URL and its channel must equal the configured
forum. With a bot reference, the public channel must also be a Discord forum
(type 15). Webhook-only preflight does not prove every permission/channel property.
Established board failures are isolated; a sequential round-robin pass continues
with other boards. Bounded synchronous HTTP/lock waits can delay them: this is
not a hard latency-isolation guarantee.

Explicit registry resolution uses Hermes' board APIs without writes or current
board fallback. Worker/delegation/pin-first fenced contexts are refused, never
cleared. Missing safety APIs in an older Hermes runtime fail closed; legacy mode
remains available. Canonical native/state paths, device/inode identities, regular
file status and hardlink counts are checked; symlink aliases (including parent
indirection and SQLite sidecars), duplicate physical databases, state/native
collisions and later file replacements are rejected. A concurrently created
*previously absent* state
file can be pinned once during startup, then adopted only under its board lease.

State stays at
`<kanban_home>/kanban/plugins/kanban-task-threads/<board>.db`, with existing SQL
keys, thread/message IDs, cursors, retries, tombstones, dirty cards and pending
creates retained. Additive tables store only public board/destination binding,
activation watermark, bootstrap obligations and pre-block scan positions. The old
plugin can still read the original tables; native Kanban schema is never changed.

New explicit routes bind metadata under `consume:<board>` and its fencing token.
Foreign-board rows, conflicting bindings or unknown legacy destinations freeze
the board. Known matching legacy destinations can be adopted without replaying
old replies. A credential rotation retaining the same public webhook/forum
identity is not a destination change. A changed destination freezes rather than
resetting history, editing old threads or creating replacements. File/binding/
lease ownership is checked again at the HTTP boundary, including dirty repaint,
dependent announcements, tags, title/archive repair and metadata audits.

Full-list forum tag setup additionally uses an OS advisory lock keyed by the
verified forum ID directly under the shared plugin-state root
(`forum-<id>.lock`). The lock's parent directory identity is pinned at build
time and re-verified before and after acquisition, so renaming and recreating
the state root cannot split the lock into two simultaneous holders.
Different boards/processes sharing that root cannot race the tag read/replace
sequence. Locks release on process death; files are not deleted. Lock acquisition
is bounded (10 seconds) and retries on later passes. This uses POSIX `flock` and a
shared local filesystem; it is not a cross-host distributed lock or support for
independent Kanban roots publishing to the same forum. The shared state root
must be owned and non-writable by untrusted principals; path checks are not a
sandbox against an attacker racing filesystem syscalls. Do not remove lock files
while publishers run. Per-board transport/tag/audit caches and credentials remain
separate. All candidates for these boards must use the same shared state root.

Shutdown stops starting new board passes, keeps upstream's bounded runtime join
and lease takeover behavior, and closes coordinator state connections when
in-flight work returns. It cannot cancel an HTTP request already accepted remotely.
Do not mutate/replace state files while a publisher is running; identity checks
are not a sandbox against an attacker racing filesystem syscalls.

## Failures and operator reconciliation

Unknown-outcome creates remain quarantined. A 5xx response to creation is not
proof no thread was created. Do not clear it without checking the remote forum.
429 backoff, permanent payload parking, human-deletion tombstones, mention
suppression, actor attribution, tags and archive behavior remain upstream's.
Replies are **at-least-once** across a crash between remote acceptance and the
local checkpoint, not exactly-once delivery. Error diagnostics in explicit-route
mode retain HTTP status and numeric policy fields, not arbitrary remote response
text, credential-bearing URLs or underlying exception reprs.

Quiesce publishers before reconciling. The state CLI still takes the exact board
and task ID. Mutations refuse aliased files, a foreign bound board, and active
publisher leases. `adopt` accepts an optional final public destination argument:

```text
python scripts/reconcile.py <state.db> adopt <board> <task_id> <thread_id> <starter_id> discord:webhook:<webhook_id>@<forum_id>
```

For a bound board, omission uses the already verified public binding; conflicting
destinations are refused. For unbound legacy unknown-destination posts, supply
explicit matching public evidence after verifying the actual forum/thread. This
command never contacts Discord. It does not verify a human's supplied thread ID.
`clear`, `rearm` and `recreate` retain upstream operator semantics; deliberately
abandoning a mapping can permit a later replacement. They are not automatic
migration tools and never reset another board's cursor. Foreign/corrupt ownership
metadata is not silently repaired by the publisher.

## Unapplied cutover / rollback gate

1. Obtain separate deployment authorization and review this source snapshot.
2. Quiesce every old/new candidate. Back up each state DB consistently, including
   SQLite WAL data, and retain native board backups/route evidence. Do not delete
   state, mappings or cursors to get a green startup.
3. Replace only the approved artifact, configure explicit routes and publisher
   secrets using supported Hermes setup, and preserve existing state paths.
4. Check local diagnostics first; any live Discord acceptance test requires its
   own authorization. Do not run the demo/probe scripts as an offline check.
5. Roll back only while quiesced, preserving additive state/mappings and a backup
   of the latest state. Old code reads the old tables but does not enforce the new
   registry/binding/first-block rules. Restore the matching legacy configuration;
   never run old and new publishers concurrently during rollback.

Offline tests do not establish live Discord permissions, Docker/Compose restart
continuity, cross-host lock semantics or a full Python-version support matrix.

Package source files only. Do not ship a development `.test-venv`, `.codegraph`,
Git metadata or caches: validators may scan their unrelated dependency code and
interpreter symlinks. The implementation evidence stages and hashes a clean
source-only artifact before running the real installed Hermes validator.
