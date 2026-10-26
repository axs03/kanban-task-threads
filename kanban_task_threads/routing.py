"""Explicit immutable routes. Validation/preflight is read-only and all-or-nothing.

Credentials live only in transports, never in route metadata or diagnostics.
"""

import os
import re
import sqlite3
import stat
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from urllib.parse import urlsplit

from .avatars import AvatarConfigError, AvatarSet
from .consumer import Consumer, Report
from .coordination import forum_lock
from .runtime import RetryableStartup
from .store import BindingError, StateStore
from .transport import DiscordTransport


class RoutingError(ValueError):
    """Unsafe or incomplete routing; reload after correcting configuration."""


@dataclass(frozen=True)
class BoardRoute:
    board: str
    forum: str
    webhook_secret: str
    bot_secret: str | None = None
    tags: tuple[str, ...] = ()
    dashboard_url: str = ""


def _text(value, pattern):
    if not isinstance(value, str) or re.fullmatch(pattern, value) is None:
        raise RoutingError("invalid board route field")
    return value


def creation_mode(ctx):
    mode = ctx.get_config("thread_creation", "all")
    if not isinstance(mode, str) or mode not in ("all", "first_blocked"):
        raise RoutingError("invalid thread_creation; use all or first_blocked")
    return mode


def parse_routes(ctx):
    value = ctx.get_config("boards")
    if not isinstance(value, list) or not value:
        raise RoutingError("boards must be a nonempty list")
    if ctx.get_config("board") or ctx.get_config("discord_applied_tag_ids"):
        raise RoutingError("boards conflicts with legacy board or global destination tags")
    creation_mode(ctx)
    routes, seen = [], set()
    for entry in value:
        if not isinstance(entry, dict) or set(entry) - {
            "board",
            "forum_channel_id",
            "webhook_secret",
            "bot_token_secret",
            "discord_applied_tag_ids",
            "dashboard_url",
        }:
            raise RoutingError("invalid board route keys")
        board = _text(entry.get("board"), r"[a-z0-9][a-z0-9_-]{0,63}")
        if board in seen:
            raise RoutingError("duplicate board route")
        seen.add(board)
        forum = _text(entry.get("forum_channel_id"), r"[1-9][0-9]{0,19}")
        secret = _text(entry.get("webhook_secret"), r"[A-Z_][A-Z0-9_]{0,127}")
        bot = (
            _text(entry["bot_token_secret"], r"[A-Z_][A-Z0-9_]{0,127}")
            if "bot_token_secret" in entry
            else None
        )
        tags = entry.get("discord_applied_tag_ids", [])
        if not isinstance(tags, list):
            raise RoutingError("board tags must be a list")
        tags = tuple(_text(tag, r"[1-9][0-9]{0,19}") for tag in tags)
        if len(tags) != len(set(tags)):
            raise RoutingError("duplicate board tags")
        dashboard = entry.get("dashboard_url", ctx.get_config("dashboard_url", "") or "")
        if not isinstance(dashboard, str):
            raise RoutingError("invalid dashboard URL")
        if dashboard:
            try:
                parsed = urlsplit(dashboard)
                if (
                    parsed.scheme not in ("http", "https")
                    or not parsed.hostname
                    or parsed.username
                    or parsed.password
                ):
                    raise ValueError
            except ValueError:
                raise RoutingError("invalid dashboard URL") from None
        routes.append(BoardRoute(board, forum, secret, bot, tags, dashboard))
    return tuple(routes)


_SIDECAR_SUFFIXES = ("-wal", "-shm", "-journal")


def check_sidecars(path: Path) -> None:
    """Refuse aliased SQLite companions (-wal/-shm/-journal) before opening.

    A hardlink or symlink from a sidecar to another database lets a write path
    mutate bytes outside the state DB. This mirrors the ownership boundary for
    the main file and is reused by reconciliation.
    """
    for suffix in _SIDECAR_SUFFIXES:
        companion = Path(str(path) + suffix)
        if companion.is_symlink():
            raise RoutingError("board/state SQLite sidecar is aliased")
        try:
            info = companion.stat()
        except FileNotFoundError:
            continue
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise RoutingError("board/state SQLite sidecar is aliased")


def file_identity(path: Path, *, missing=False):
    """Refuse link indirection (including parents), hardlinks and non-files."""
    path = path.absolute()
    if path.resolve() != path:
        raise RoutingError("board/state path contains a symlink or escapes ownership boundary")
    check_sidecars(path)
    try:
        st = path.stat()
    except FileNotFoundError:
        if missing:
            return None
        raise RoutingError("configured board database is missing") from None
    if not stat.S_ISREG(st.st_mode) or st.st_nlink != 1:
        raise RoutingError("board/state database must be an unaliased regular file")
    return st.st_dev, st.st_ino


@dataclass
class BoardFiles:
    native: Path
    state: Path
    native_id: tuple
    state_id: tuple | None
    http_guard: object = None

    def check(self):
        current_state = file_identity(self.state, missing=True)
        if self.state_id is None and current_state is not None:
            # Another candidate may create this exact state file during our
            # read-only preflight. Pin it once; binding still runs under lease.
            self.state_id = current_state
        if file_identity(self.native) != self.native_id or current_state != self.state_id:
            raise RoutingError("board/state file identity changed; validated reload required")

    def check_http(self):
        self.check()
        if self.http_guard is not None:
            self.http_guard()


def resolve_files(routes, db):
    # Never clear a worker/delegation fence to become a cross-board publisher.
    fence = getattr(db, "_explicit_board_intent_pinned", None)
    if (
        fence is None
        or fence()
        or any(
            os.environ.get(key) for key in ("HERMES_KANBAN_TASK", "HERMES_DELEGATED_CHILD_CONTEXT")
        )
    ):
        raise RoutingError("multi-board publishing is unavailable in a pinned/fenced context")
    root = Path(db.kanban_home()).absolute()
    if root.resolve() != root:
        raise RoutingError("Kanban home must not be a symlink")
    result, identities = [], set()
    for route in routes:
        if not db.board_exists(route.board):
            raise RoutingError("configured board does not exist")
        expected = root / (
            "kanban.db" if route.board == "default" else f"kanban/boards/{route.board}/kanban.db"
        )
        native = Path(db.kanban_db_path(route.board)).absolute()
        if native != expected:
            raise RoutingError("explicit board resolver returned a different database")
        state = root / f"kanban/plugins/kanban-task-threads/{route.board}.db"
        native_id, state_id = file_identity(native), file_identity(state, missing=True)
        for identity in (native_id, state_id):
            if identity is not None:
                if identity in identities:
                    raise RoutingError("duplicate physical board/state databases")
                identities.add(identity)
        try:
            with sqlite3.connect(native.as_uri() + "?mode=ro", uri=True) as conn:
                conn.execute("SELECT id, status FROM tasks LIMIT 0")
                conn.execute(
                    "SELECT id, task_id, kind, payload, created_at FROM task_events LIMIT 0"
                )
        except sqlite3.Error:
            raise RoutingError("invalid native board database schema") from None
        result.append(BoardFiles(native, state, native_id, state_id))
    return tuple(result)


class Coordinator:
    """Sequential, fair passes; no mutable task/transport caches are shared."""

    def __init__(self, consumers):
        self.consumers = tuple(consumers)
        self._offset = 0
        self._stopping = False
        self._closed = False
        self._stop_event = None

    def set_stop_event(self, event):
        self._stop_event = event

    def request_stop(self):
        self._stopping = True

    def run_once(self, now=None):
        report = Report()
        n = len(self.consumers)
        for index in range(n):
            if self._stopping or (self._stop_event is not None and self._stop_event.is_set()):
                break
            consumer = self.consumers[(index + self._offset) % n]
            try:
                part = consumer.run_once(now=now)
                report.errors.extend(f"{consumer._board}: {line}" for line in part.errors)
                report.warnings.extend(f"{consumer._board}: {line}" for line in part.warnings)
                report.opened.extend(part.opened)
                report.edited.extend(part.edited)
                report.replied += part.replied
            except (BindingError, RoutingError) as error:
                # These exception classes contain only local, fixed diagnostics.
                report.errors.append(f"{consumer._board}: {error}")
            except Exception:
                # Exception repr/tracebacks can contain credential-bearing URLs.
                report.errors.append(f"{consumer._board}: board pass failed; will retry")
        self._offset = (self._offset + 1) % n
        return report

    def close(self):
        self.request_stop()
        if not self._closed:
            for consumer in self.consumers:
                consumer._store._conn.close()
            self._closed = True


def _safe_http(http, credentials, check):
    def redact(value):
        if isinstance(value, str):
            for secret in credentials:
                if secret:
                    value = value.replace(secret, "[redacted]")
            return re.sub(r"https?://[^\s\"\'<>]+", "[redacted-url]", value)
        if isinstance(value, dict):
            return {redact(k): redact(v) for k, v in value.items()}
        if isinstance(value, list):
            return [redact(v) for v in value]
        return value

    def call(method, url, body, headers=None):
        check()
        try:
            status, response = http(method, url, body, headers=headers)
        except Exception:
            raise OSError("transport request failed (details suppressed)") from None
        if not 200 <= status < 300:
            # Only these machine-readable fields drive delivery policy. Remote
            # error text can echo credentials we never supplied to this route.
            response = {
                key: value
                for key, value in (response.items() if isinstance(response, dict) else ())
                if key in ("code", "retry_after") and type(value) in (int, float)
            }
        return status, redact(response)

    return call


def _warn_unconfigured_boards(routes, db, logger):
    """Boards on disk but absent from `boards:` are silently never published."""
    try:
        configured = {r.board for r in routes}
        root = Path(db.boards_root())
        on_disk = {"default"} | {p.name for p in root.iterdir() if (p / "kanban.db").is_file()}
        missing = sorted(on_disk - configured)
    except Exception:
        return
    if missing:
        logger.warning(
            "kanban-task-threads: boards not configured, no Discord threads or pings: %s",
            ", ".join(missing),
        )


def build_multiboard(ctx, logger):
    """No effect-capable consumers exist until the entire registry is verified."""
    from agent import secret_scope
    from hermes_cli import kanban_db as db

    from . import transport as transport_module

    routes = parse_routes(ctx)
    files = resolve_files(routes, db)
    _warn_unconfigured_boards(routes, db, logger)
    secrets = []
    try:
        for route in routes:
            webhook = secret_scope.get_secret(route.webhook_secret)
            bot = secret_scope.get_secret(route.bot_secret) if route.bot_secret else None
            if not webhook or (route.bot_secret and not bot):
                if secret_scope.current_secret_scope() is None:
                    raise RetryableStartup("publisher scoped credentials unavailable")
                raise RoutingError("a named board credential is unavailable")
            # Do not send a scoped credential to a user-configured URL/host.
            if (
                not isinstance(webhook, str)
                or re.fullmatch(
                    r"https://discord\.com/api(?:/v[0-9]+)?/webhooks/[0-9]+/[A-Za-z0-9._-]+",
                    webhook,
                )
                is None
            ):
                raise RoutingError("named webhook credential has invalid format")
            if bot is not None and (not isinstance(bot, str) or not bot.strip()):
                raise RoutingError("named bot credential has invalid format")
            secrets.append((webhook, bot))
    except RoutingError:
        raise
    except Exception:
        raise RetryableStartup("publisher scoped credentials unavailable") from None

    avatars = None
    if ctx.get_config("avatars_enabled", True):
        try:
            base = ctx.get_config("avatar_base_url", "")
            avatars = (
                AvatarSet.custom(base)
                if base
                else AvatarSet.official(
                    theme=ctx.get_config("avatar_theme", "duotone"),
                    palette=ctx.get_config("avatar_palette", "colored"),
                )
            )
        except AvatarConfigError:
            logger.warning(
                "kanban-task-threads: invalid avatar settings; continuing without avatars"
            )
    mention_user_id = ctx.get_config("mention_user_id", "") or None
    if mention_user_id is not None:
        mention_user_id = str(mention_user_id)
        _text(mention_user_id, r"[1-9][0-9]{0,19}")
    verified = []
    for route, paths, (webhook, bot) in zip(routes, files, secrets, strict=True):
        safe_http = _safe_http(
            transport_module.urllib_http,
            (webhook, webhook.rsplit("/", 1)[-1], bot),
            paths.check_http,
        )
        transport = DiscordTransport(
            safe_http,
            webhook,
            bot_token=bot,
            applied_tag_ids=route.tags,
            forum_channel_id=route.forum,
            avatars=avatars,
            mention_user_id=mention_user_id,
        )
        try:
            info = transport.webhook_info()
            if (
                str(info.get("channel_id")) != route.forum
                or str(info.get("id")) != webhook.rsplit("/", 2)[-2]
            ):
                raise RoutingError("discovered webhook destination does not match configured forum")
            if bot:
                channel = transport._refresh_forum_channel()
                if str(channel.get("id")) != route.forum or channel.get("type") != 15:
                    raise RoutingError(
                        "configured bot destination is not the expected forum channel"
                    )
        except RoutingError:
            raise
        except Exception as error:
            status = getattr(error, "status", 0)
            if 400 <= status < 500 and status != 429:
                raise RoutingError("Discord rejected a configured board credential") from None
            raise RetryableStartup("board destination preflight unavailable") from None
        verified.append((route, paths, transport, info))

    # Validate local options before opening any state DB.
    options = dict(
        stale_after=int(ctx.get_config("stale_after_seconds", 600) or 600),
        include_workspace_path=bool(ctx.get_config("include_workspace_path", False)),
        card_template=ctx.get_config("card_template"),
        reply_templates=ctx.get_config("reply_templates") or {},
        comment_excerpt_chars=int(ctx.get_config("comment_excerpt_chars", 180) or 0),
        thread_creation=creation_mode(ctx),
        qualify_identity=True,
        require_binding=True,
    )
    reply_on = ctx.get_config("reply_on")
    if reply_on:
        options["reply_on"] = tuple(reply_on)
    consumers = []
    try:
        for route, paths, transport, info in verified:
            paths.check()
            state = StateStore(paths.state)
            paths.state_id = file_identity(paths.state)
            consumer = Consumer(
                paths.native,
                state,
                transport,
                board=route.board,
                forum_lock=partial(
                    forum_lock,
                    paths.state.parent / f"forum-{route.forum}.lock",
                    parent=(
                        paths.state.parent.stat().st_dev,
                        paths.state.parent.stat().st_ino,
                    ),
                ),
                holder=f"{ctx.profile_name}:{os.getpid()}",
                destination=f"discord:webhook:{info['id']}@{route.forum}",
                dashboard_url=route.dashboard_url,
                guild_id=str(info.get("guild_id") or ""),
                **options,
            )
            consumer._route_check = paths.check
            paths.http_guard = consumer._assert_effect_ownership
            consumers.append(consumer)
        return Coordinator(consumers)
    except BaseException:
        for consumer in consumers:
            consumer._store._conn.close()
        raise
