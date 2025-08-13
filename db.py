import os, asyncpg, random, string, time
from typing import List, Optional, Dict
from datetime import datetime, timedelta, timezone

UTC = timezone.utc
def now(): return datetime.now(UTC)

def codegen():
    alphabet = "ABCDEFGHJKMNPQRSTUVWXYZ23456789"
    def grp(n=4): return "".join(random.choice(alphabet) for _ in range(n))
    return f"{grp()}-{grp()}-{grp()}"

class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: asyncpg.Pool | None = None

    async def connect(self):
        # tamaños conservadores para hosts tipo Render/Railway
        self.pool = await asyncpg.create_pool(dsn=self.dsn, min_size=1, max_size=5)

    async def close(self):
        if self.pool:
            await self.pool.close()

    async def init(self):
        q = """
        CREATE TABLE IF NOT EXISTS config(
          guild_id BIGINT PRIMARY KEY,
          staff_role_id BIGINT,
          category_id BIGINT,

          -- Bugs
          bug_input_channel_id BIGINT,
          bug_log_channel_id BIGINT,
          bug_notice_channel_id BIGINT,
          bug_notice_message_id BIGINT,
          bug_ping_mode TEXT DEFAULT 'none',               -- none|here|everyone|staff
          bug_ping_role_id BIGINT,
          bug_window_hours INT DEFAULT 2,
          bug_mute_minutes INT DEFAULT 10,

          -- Anti-ping
          antiping_threshold INT DEFAULT 1,                -- 1 = primera vez avisa, segunda mutea
          antiping_timeout_minutes INT DEFAULT 10,
          antiping_window_hours INT DEFAULT 6
        );

        CREATE TABLE IF NOT EXISTS prizes(
          name TEXT PRIMARY KEY,
          description TEXT DEFAULT '',
          weight INT DEFAULT 1
        );

        CREATE TABLE IF NOT EXISTS keys(
          code TEXT PRIMARY KEY,
          mode TEXT NOT NULL,           -- 'fixed' | 'random'
          prize_name TEXT,              -- si fixed
          expires_at TIMESTAMPTZ,
          used BOOLEAN DEFAULT FALSE,
          used_by BIGINT,
          used_at TIMESTAMPTZ
        );

        CREATE TABLE IF NOT EXISTS tickets(
          id BIGSERIAL PRIMARY KEY,
          guild_id BIGINT NOT NULL,
          opener_id BIGINT NOT NULL,
          kind TEXT NOT NULL,           -- comprar | soporte
          channel_id BIGINT UNIQUE NOT NULL,
          status TEXT DEFAULT 'open',   -- open | closed
          claimed_by BIGINT,
          purchase_plan TEXT,
          payment_method TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          closed_at TIMESTAMPTZ,
          last_activity TIMESTAMPTZ DEFAULT NOW(),
          warned_30 BOOLEAN DEFAULT FALSE,
          warned_10 BOOLEAN DEFAULT FALSE,
          close_reason TEXT,
          closed_by BIGINT
        );

        CREATE TABLE IF NOT EXISTS ticket_messages(
          id BIGSERIAL PRIMARY KEY,
          ticket_id BIGINT NOT NULL REFERENCES tickets(id) ON DELETE CASCADE,
          channel_id BIGINT NOT NULL,
          author_id BIGINT NOT NULL,
          content TEXT,
          attachments TEXT[],
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_ticket_messages_ticket ON ticket_messages(ticket_id);

        -- Roles extra que pueden ver tickets
        CREATE TABLE IF NOT EXISTS ticket_allowed_roles(
          guild_id BIGINT NOT NULL,
          role_id BIGINT NOT NULL,
          PRIMARY KEY(guild_id, role_id)
        );

        -- Paneles publicados (para evitar duplicados)
        CREATE TABLE IF NOT EXISTS ticket_panels(
          guild_id BIGINT NOT NULL,
          channel_id BIGINT NOT NULL,
          message_id BIGINT NOT NULL UNIQUE,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );

        -- Bugs: almacenamiento de reportes
        CREATE TABLE IF NOT EXISTS bug_reports(
          id BIGSERIAL PRIMARY KEY,
          guild_id BIGINT NOT NULL,
          reporter_id BIGINT NOT NULL,
          source_channel_id BIGINT NOT NULL,
          source_message_id BIGINT NOT NULL,
          content TEXT NOT NULL,
          status TEXT NOT NULL DEFAULT 'open', -- open|resolved
          registry_channel_id BIGINT,
          registry_message_id BIGINT,
          resolve_reason TEXT,
          resolved_by BIGINT,
          created_at TIMESTAMPTZ DEFAULT NOW(),
          resolved_at TIMESTAMPTZ
        );
        CREATE INDEX IF NOT EXISTS idx_bug_reports_guild ON bug_reports(guild_id, status);

        -- Bugs: rate limit por usuario
        CREATE TABLE IF NOT EXISTS bug_report_limits(
          guild_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          last_report_at TIMESTAMPTZ,
          violations INT DEFAULT 0,
          PRIMARY KEY(guild_id, user_id)
        );

        -- Anti-ping: usuarios protegidos del ping
        CREATE TABLE IF NOT EXISTS antiping_targets(
          guild_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          PRIMARY KEY(guild_id, user_id)
        );

        -- Anti-ping: contadores por infractor
        CREATE TABLE IF NOT EXISTS antiping_violations(
          guild_id BIGINT NOT NULL,
          offender_id BIGINT NOT NULL,
          count INT DEFAULT 0,
          last_seen TIMESTAMPTZ,
          PRIMARY KEY(guild_id, offender_id)
        );

        -- Giveaways
        CREATE TABLE IF NOT EXISTS giveaways(
          id BIGSERIAL PRIMARY KEY,
          guild_id BIGINT NOT NULL,
          channel_id BIGINT NOT NULL,
          message_id BIGINT,
          prize TEXT NOT NULL,
          winners INT NOT NULL DEFAULT 1,
          ends_at TIMESTAMPTZ NOT NULL,
          ping_role_id BIGINT,
          created_by BIGINT,
          status TEXT NOT NULL DEFAULT 'running', -- running|ended|cancelled
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE INDEX IF NOT EXISTS idx_giveaways_due ON giveaways(status, ends_at);

        CREATE TABLE IF NOT EXISTS giveaway_entries(
          giveaway_id BIGINT NOT NULL REFERENCES giveaways(id) ON DELETE CASCADE,
          user_id BIGINT NOT NULL,
          joined_at TIMESTAMPTZ DEFAULT NOW(),
          PRIMARY KEY(giveaway_id, user_id)
        );

        -- GOLD Accounts: separadas por guild y admin
        CREATE TABLE IF NOT EXISTS gold_accounts(
          id BIGSERIAL PRIMARY KEY,
          guild_id BIGINT NOT NULL,
          admin_id BIGINT NOT NULL,
          account_name TEXT NOT NULL,
          ugphone TEXT NOT NULL,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
        CREATE UNIQUE INDEX IF NOT EXISTS uq_goldacc_unique
          ON gold_accounts(guild_id, admin_id, account_name);
        """
        async with self.pool.acquire() as c:
            await c.execute(q)

    # ---------- Config general ----------
    async def get_config(self, guild_id: int) -> dict:
        async with self.pool.acquire() as c:
            row = await c.fetchrow("SELECT * FROM config WHERE guild_id=$1", guild_id)
            return dict(row) if row else {}

    async def set_staff_role(self, guild_id: int, role_id: int):
        async with self.pool.acquire() as c:
            await c.execute("""
                INSERT INTO config(guild_id, staff_role_id)
                VALUES($1,$2)
                ON CONFLICT (guild_id) DO UPDATE SET staff_role_id=EXCLUDED.staff_role_id
            """, guild_id, role_id)

    async def set_category(self, guild_id: int, category_id: int):
        async with self.pool.acquire() as c:
            await c.execute("""
                INSERT INTO config(guild_id, category_id)
                VALUES($1,$2)
                ON CONFLICT (guild_id) DO UPDATE SET category_id=EXCLUDED.category_id
            """, guild_id, category_id)

    # ---------- Bugs: canales y aviso fijado ----------
    async def set_bug_channels(self, guild_id: int, input_channel_id: int | None, log_channel_id: int | None):
        async with self.pool.acquire() as c:
            cur = await c.fetchrow("SELECT 1 FROM config WHERE guild_id=$1", guild_id)
            if not cur:
                await c.execute("INSERT INTO config(guild_id, bug_input_channel_id, bug_log_channel_id) VALUES($1,$2,$3)",
                                guild_id, input_channel_id, log_channel_id)
            else:
                if input_channel_id is not None:
                    await c.execute("UPDATE config SET bug_input_channel_id=$2 WHERE guild_id=$1", guild_id, input_channel_id)
                if log_channel_id is not None:
                    await c.execute("UPDATE config SET bug_log_channel_id=$2 WHERE guild_id=$1", guild_id, log_channel_id)

    async def get_bug_channels(self, guild_id: int) -> dict:
        cfg = await self.get_config(guild_id)
        return {
            "bug_input_channel_id": cfg.get("bug_input_channel_id"),
            "bug_log_channel_id": cfg.get("bug_log_channel_id")
        }

    async def get_bug_notice(self, guild_id: int) -> dict:
        cfg = await self.get_config(guild_id)
        return {
            "bug_notice_channel_id": cfg.get("bug_notice_channel_id"),
            "bug_notice_message_id": cfg.get("bug_notice_message_id")
        }

    async def set_bug_notice(self, guild_id: int, channel_id: int, message_id: int):
        async with self.pool.acquire() as c:
            await c.execute("""
                INSERT INTO config(guild_id, bug_notice_channel_id, bug_notice_message_id)
                VALUES($1,$2,$3)
                ON CONFLICT (guild_id) DO UPDATE SET
                    bug_notice_channel_id=EXCLUDED.bug_notice_channel_id,
                    bug_notice_message_id=EXCLUDED.bug_notice_message_id
            """, guild_id, channel_id, message_id)

    async def clear_bug_notice(self, guild_id: int):
        async with self.pool.acquire() as c:
            await c.execute("""
                UPDATE config SET bug_notice_channel_id=NULL, bug_notice_message_id=NULL
                WHERE guild_id=$1
            """, guild_id)

    # ---------- Bugs: settings y ping ----------
    async def get_bug_settings(self, guild_id: int):
        cfg = await self.get_config(guild_id)
        return {
            "window_hours": int(cfg.get("bug_window_hours", 2) or 2),
            "mute_minutes": int(cfg.get("bug_mute_minutes", 10) or 10),
            "ping_mode": (cfg.get("bug_ping_mode") or "none"),
            "ping_role_id": cfg.get("bug_ping_role_id")
        }

    async def set_bug_settings(self, guild_id: int, window_hours: int | None, mute_minutes: int | None):
        async with self.pool.acquire() as c:
            if window_hours is not None:
                await c.execute("""
                    INSERT INTO config(guild_id, bug_window_hours) VALUES($1,$2)
                    ON CONFLICT (guild_id) DO UPDATE SET bug_window_hours=EXCLUDED.bug_window_hours
                """, guild_id, int(window_hours))
            if mute_minutes is not None:
                await c.execute("""
                    INSERT INTO config(guild_id, bug_mute_minutes) VALUES($1,$2)
                    ON CONFLICT (guild_id) DO UPDATE SET bug_mute_minutes=EXCLUDED.bug_mute_minutes
                """, guild_id, int(mute_minutes))

    async def set_bug_ping_mode(self, guild_id: int, mode: str, role_id: int | None):
        async with self.pool.acquire() as c:
            await c.execute("""
                INSERT INTO config(guild_id, bug_ping_mode, bug_ping_role_id)
                VALUES($1,$2,$3)
                ON CONFLICT (guild_id) DO UPDATE SET
                bug_ping_mode=EXCLUDED.bug_ping_mode,
                bug_ping_role_id=EXCLUDED.bug_ping_role_id
            """, guild_id, mode, role_id)

    # ---------- Bugs: rate limiting ----------
    async def check_bug_rate(self, guild_id: int, user_id: int, window_hours: int):
        """Devuelve 'ok' | 'warn' | 'mute' y actualiza contador."""
        async with self.pool.acquire() as c:
            rec = await c.fetchrow("SELECT last_report_at, violations FROM bug_report_limits WHERE guild_id=$1 AND user_id=$2",
                                   guild_id, user_id)
            now_ts = now()
            if not rec:
                await c.execute("INSERT INTO bug_report_limits(guild_id,user_id,last_report_at,violations) VALUES($1,$2,$3,0)",
                                guild_id, user_id, now_ts)
                return "ok"
            last_at, viol = rec["last_report_at"], int(rec["violations"] or 0)
            if last_at and (now_ts - last_at) < timedelta(hours=window_hours):
                if viol <= 0:
                    await c.execute("UPDATE bug_report_limits SET violations=1, last_report_at=$3 WHERE guild_id=$1 AND user_id=$2",
                                    guild_id, user_id, now_ts)
                    return "warn"
                else:
                    await c.execute("UPDATE bug_report_limits SET violations=violations+1, last_report_at=$3 WHERE guild_id=$1 AND user_id=$2",
                                    guild_id, user_id, now_ts)
                    return "mute"
            else:
                await c.execute("UPDATE bug_report_limits SET violations=0, last_report_at=$3 WHERE guild_id=$1 AND user_id=$2",
                                guild_id, user_id, now_ts)
                return "ok"

    # ---------- Prizes ----------
    async def add_prize(self, name: str, description: str, weight: int):
        try:
            async with self.pool.acquire() as c:
                await c.execute("INSERT INTO prizes(name, description, weight) VALUES($1,$2,$3)", name, description, weight)
            return {"ok": True, "prize": {"name": name, "description": description, "weight": weight}}
        except asyncpg.UniqueViolationError:
            return {"ok": False, "reason": "duplicate"}

    async def list_prizes(self):
        async with self.pool.acquire() as c:
            rows = await c.fetch("SELECT name, description, weight FROM prizes ORDER BY name ASC")
            return [dict(r) for r in rows]

    async def remove_prize(self, name: str):
        async with self.pool.acquire() as c:
            res = await c.execute("DELETE FROM prizes WHERE name=$1", name)
            return {"ok": res.endswith("DELETE 1")}

    async def _get_prize(self, name: str):
        async with self.pool.acquire() as c:
            r = await c.fetchrow("SELECT name, description, weight FROM prizes WHERE name=$1", name)
            return dict(r) if r else None

    async def _choose_random_prize(self):
        async with self.pool.acquire() as c:
            rows = await c.fetch("SELECT name, description, weight FROM prizes")
        items = [dict(r) for r in rows]
        if not items:
            return None
        weights = [max(1, int(p.get("weight", 1))) for p in items]
        return random.choices(items, weights=weights, k=1)[0]

    # ---------- Keys ----------
    async def create_keys(self, amount: int, mode: str, prize: str | None, created_by: int | None, expires_days: int | None):
        if mode not in ("random", "fixed"):
            mode = "random"
        if mode == "fixed":
            if not prize:
                return {"ok": False, "reason": "prize_required"}
            p = await self._get_prize(prize)
            if not p:
                return {"ok": False, "reason": "prize_not_found"}

        codes = []
        exp_at = None
        if expires_days:
            exp_at = now() + timedelta(days=int(expires_days))
        async with self.pool.acquire() as c:
            async with c.transaction():
                for _ in range(amount):
                    while True:
                        code = codegen()
                        exists = await c.fetchval("SELECT 1 FROM keys WHERE code=$1", code)
                        if not exists:
                            break
                    await c.execute(
                        "INSERT INTO keys(code, mode, prize_name, expires_at, used) VALUES($1,$2,$3,$4,FALSE)",
                        code, mode, prize if mode == "fixed" else None, exp_at
                    )
                    codes.append(code)
        return {"ok": True, "codes": codes, "mode": mode, "prize": prize, "expires_days": expires_days}

    async def check_key(self, code: str, user_id: int | None):
        async with self.pool.acquire() as c:
            row = await c.fetchrow("SELECT * FROM keys WHERE code=$1", code)
            if not row:
                return {"ok": False, "reason": "not_found"}
            d = dict(row)
            if d["used"]:
                return {"ok": False, "reason": "used"}
            if d["expires_at"] and d["expires_at"] < now():
                return {"ok": False, "reason": "expired"}
            if d["mode"] == "fixed":
                prize = await self._get_prize(d["prize_name"])
            else:
                prize = await self._choose_random_prize()
                if not prize:
                    return {"ok": False, "reason": "no_prizes"}
            if not prize:
                return {"ok": False, "reason": "prize_missing"}
            await c.execute("UPDATE keys SET used=TRUE, used_by=$1, used_at=$2 WHERE code=$3", user_id, now(), code)
            return {"ok": True, "mode": d["mode"], "prize": prize, "code": code}

    # ---------- Tickets ----------
    async def create_ticket(self, guild_id: int, opener_id: int, kind: str, channel_id: int):
        async with self.pool.acquire() as c:
            await c.execute("""
                INSERT INTO tickets(guild_id, opener_id, kind, channel_id, status, created_at, last_activity)
                VALUES($1,$2,$3,$4,'open',NOW(),NOW())
            """, guild_id, opener_id, kind, channel_id)

    async def fetch_ticket_by_channel(self, channel_id: int):
        async with self.pool.acquire() as c:
            r = await c.fetchrow("SELECT * FROM tickets WHERE channel_id=$1", channel_id)
            return dict(r) if r else None

    async def close_ticket_by_channel(self, channel_id: int, closed_by: int, reason: str):
        async with self.pool.acquire() as c:
            t = await c.fetchrow("SELECT * FROM tickets WHERE channel_id=$1", channel_id)
            if not t or t["status"] != "open":
                return None
            tid = t["id"]
            await c.execute("""
                UPDATE tickets SET status='closed', closed_at=NOW(), close_reason=$2, closed_by=$3
                WHERE id=$1
            """, tid, reason, closed_by)
            msgs = await c.fetch("SELECT author_id, content, attachments, created_at FROM ticket_messages WHERE ticket_id=$1 ORDER BY created_at ASC", tid)
            t2 = await c.fetchrow("SELECT * FROM tickets WHERE id=$1", tid)
            return (dict(t2), [dict(x) for x in msgs])

    async def reopen_ticket_by_channel(self, channel_id: int):
        async with self.pool.acquire() as c:
            await c.execute("UPDATE tickets SET status='open', closed_at=NULL, close_reason=NULL WHERE channel_id=$1", channel_id)
            r = await c.fetchrow("SELECT * FROM tickets WHERE channel_id=$1", channel_id)
            return dict(r) if r else None

    async def set_claim(self, channel_id: int, claimed_by: int | None):
        async with self.pool.acquire() as c:
            await c.execute("UPDATE tickets SET claimed_by=$2 WHERE channel_id=$1", channel_id, claimed_by)

    async def set_purchase_plan(self, channel_id: int, plan: str):
        async with self.pool.acquire() as c:
            await c.execute("UPDATE tickets SET purchase_plan=$2 WHERE channel_id=$1", channel_id, plan)

    async def set_payment_method(self, channel_id: int, method: str):
        async with self.pool.acquire() as c:
            await c.execute("UPDATE tickets SET payment_method=$2 WHERE channel_id=$1", channel_id, method)

    async def log_ticket_message(self, channel_id: int, author_id: int, content: str, attachments: List[str]):
        async with self.pool.acquire() as c:
            t = await c.fetchrow("SELECT id FROM tickets WHERE channel_id=$1", channel_id)
            if not t:
                return
            tid = t["id"]
            await c.execute("""
                INSERT INTO ticket_messages(ticket_id, channel_id, author_id, content, attachments, created_at)
                VALUES($1,$2,$3,$4,$5,NOW())
            """, tid, channel_id, author_id, content, attachments)
            await c.execute("UPDATE tickets SET last_activity=NOW() WHERE id=$1", tid)

    async def list_open_tickets(self):
        async with self.pool.acquire() as c:
            rows = await c.fetch("SELECT * FROM tickets WHERE status='open'")
            return [dict(r) for r in rows]

    async def mark_warning(self, ticket_id: int, minutes: int):
        async with self.pool.acquire() as c:
            if minutes == 30:
                await c.execute("UPDATE tickets SET warned_30=TRUE WHERE id=$1", ticket_id)
            elif minutes == 10:
                await c.execute("UPDATE tickets SET warned_10=TRUE WHERE id=$1", ticket_id)

    async def count_open_tickets(self, guild_id: int) -> int:
        async with self.pool.acquire() as c:
            v = await c.fetchval("SELECT COUNT(*) FROM tickets WHERE guild_id=$1 AND status='open'", guild_id)
            return int(v or 0)

    async def count_user_opened(self, user_id: int) -> int:
        async with self.pool.acquire() as c:
            v = await c.fetchval("SELECT COUNT(*) FROM tickets WHERE opener_id=$1", user_id)
            return int(v or 0)

    # ---------- Tickets: roles permitidos ----------
    async def add_allowed_role(self, guild_id: int, role_id: int):
        async with self.pool.acquire() as c:
            await c.execute("""
                INSERT INTO ticket_allowed_roles(guild_id, role_id)
                VALUES($1,$2) ON CONFLICT DO NOTHING
            """, guild_id, role_id)

    async def remove_allowed_role(self, guild_id: int, role_id: int):
        async with self.pool.acquire() as c:
            await c.execute("DELETE FROM ticket_allowed_roles WHERE guild_id=$1 AND role_id=$2", guild_id, role_id)

    async def list_allowed_roles(self, guild_id: int) -> List[int]:
        async with self.pool.acquire() as c:
            rows = await c.fetch("SELECT role_id FROM ticket_allowed_roles WHERE guild_id=$1", guild_id)
            return [int(r["role_id"]) for r in rows]

    # ---------- Paneles ----------
    async def find_panel_in_channel(self, guild_id: int, channel_id: int):
        async with self.pool.acquire() as c:
            r = await c.fetchrow("SELECT message_id FROM ticket_panels WHERE guild_id=$1 AND channel_id=$2 ORDER BY created_at DESC LIMIT 1",
                                 guild_id, channel_id)
            return int(r["message_id"]) if r else None

    async def add_panel_record(self, guild_id: int, channel_id: int, message_id: int):
        async with self.pool.acquire() as c:
            await c.execute("INSERT INTO ticket_panels(guild_id, channel_id, message_id) VALUES($1,$2,$3)",
                            guild_id, channel_id, message_id)

    async def remove_panel_record(self, guild_id: int, message_id: int):
        async with self.pool.acquire() as c:
            await c.execute("DELETE FROM ticket_panels WHERE guild_id=$1 AND message_id=$2", guild_id, message_id)

    # ---------- Bugs: CRUD ----------
    async def add_bug_report(self, guild_id: int, reporter_id: int, source_channel_id: int, source_message_id: int, content: str):
        async with self.pool.acquire() as c:
            r = await c.fetchrow("""
                INSERT INTO bug_reports(guild_id, reporter_id, source_channel_id, source_message_id, content, status, created_at)
                VALUES($1,$2,$3,$4,$5,'open',NOW())
                RETURNING id, guild_id, reporter_id, source_channel_id, source_message_id, content, status, created_at
            """, guild_id, reporter_id, source_channel_id, source_message_id, content)
            return dict(r)

    async def set_bug_registry_message(self, bug_id: int, registry_channel_id: int, registry_message_id: int):
        async with self.pool.acquire() as c:
            await c.execute("""
                UPDATE bug_reports SET registry_channel_id=$2, registry_message_id=$3 WHERE id=$1
            """, bug_id, registry_channel_id, registry_message_id)

    async def list_bugs(self, guild_id: int, status: str = "open"):
        async with self.pool.acquire() as c:
            rows = await c.fetch("""
                SELECT id, reporter_id, source_channel_id, content, status, created_at
                FROM bug_reports
                WHERE guild_id=$1 AND status=$2
                ORDER BY created_at DESC
            """, guild_id, status)
        return [dict(r) for r in rows]

    async def get_bug(self, guild_id: int, bug_id: int):
        async with self.pool.acquire() as c:
            r = await c.fetchrow("SELECT * FROM bug_reports WHERE id=$1 AND guild_id=$2", bug_id, guild_id)
            return dict(r) if r else None

    async def resolve_bug(self, guild_id: int, bug_id: int, resolver_id: int, reason: str):
        async with self.pool.acquire() as c:
            b = await c.fetchrow("SELECT * FROM bug_reports WHERE id=$1 AND guild_id=$2 AND status='open'", bug_id, guild_id)
            if not b:
                return None
            await c.execute("""
                UPDATE bug_reports SET status='resolved', resolved_by=$3, resolved_at=NOW(), resolve_reason=$4
                WHERE id=$1 AND guild_id=$2
            """, bug_id, guild_id, resolver_id, reason)
            b2 = await c.fetchrow("SELECT * FROM bug_reports WHERE id=$1", bug_id)
            d = dict(b2)
            d["source_message_url"] = f"https://discord.com/channels/{guild_id}/{d['source_channel_id']}/{d['source_message_id']}"
            return d

    # ---------- Anti-ping ----------
    async def antiping_add(self, guild_id: int, user_id: int):
        async with self.pool.acquire() as c:
            await c.execute("INSERT INTO antiping_targets(guild_id,user_id) VALUES($1,$2) ON CONFLICT DO NOTHING", guild_id, user_id)

    async def antiping_remove(self, guild_id: int, user_id: int):
        async with self.pool.acquire() as c:
            await c.execute("DELETE FROM antiping_targets WHERE guild_id=$1 AND user_id=$2", guild_id, user_id)

    async def antiping_list(self, guild_id: int) -> List[int]:
        async with self.pool.acquire() as c:
            rows = await c.fetch("SELECT user_id FROM antiping_targets WHERE guild_id=$1", guild_id)
            return [int(r["user_id"]) for r in rows]

    async def antiping_get_settings(self, guild_id: int):
        cfg = await self.get_config(guild_id)
        return {
            "threshold": int(cfg.get("antiping_threshold", 1) or 1),
            "timeout_minutes": int(cfg.get("antiping_timeout_minutes", 10) or 10),
            "window_hours": int(cfg.get("antiping_window_hours", 6) or 6)
        }

    async def antiping_set_settings(self, guild_id: int, threshold: int | None, timeout_minutes: int | None, window_hours: int | None):
        async with self.pool.acquire() as c:
            if threshold is not None:
                await c.execute("""INSERT INTO config(guild_id, antiping_threshold) VALUES($1,$2)
                                   ON CONFLICT (guild_id) DO UPDATE SET antiping_threshold=EXCLUDED.antiping_threshold""",
                                guild_id, int(threshold))
            if timeout_minutes is not None:
                await c.execute("""INSERT INTO config(guild_id, antiping_timeout_minutes) VALUES($1,$2)
                                   ON CONFLICT (guild_id) DO UPDATE SET antiping_timeout_minutes=EXCLUDED.antiping_timeout_minutes""",
                                guild_id, int(timeout_minutes))
            if window_hours is not None:
                await c.execute("""INSERT INTO config(guild_id, antiping_window_hours) VALUES($1,$2)
                                   ON CONFLICT (guild_id) DO UPDATE SET antiping_window_hours=EXCLUDED.antiping_window_hours""",
                                guild_id, int(window_hours))

    async def antiping_record(self, guild_id: int, offender_id: int, window_hours: int, threshold: int):
        """Devuelve 'warn' o 'mute' según contador/ventana."""
        async with self.pool.acquire() as c:
            r = await c.fetchrow("SELECT count, last_seen FROM antiping_violations WHERE guild_id=$1 AND offender_id=$2",
                                 guild_id, offender_id)
            now_ts = now()
            if not r:
                await c.execute("INSERT INTO antiping_violations(guild_id,offender_id,count,last_seen) VALUES($1,$2,1,$3)",
                                guild_id, offender_id, now_ts)
                return "warn"
            cnt, last = int(r["count"] or 0), r["last_seen"]
            if last and (now_ts - last) > timedelta(hours=window_hours):
                cnt = 0
            cnt += 1
            await c.execute("UPDATE antiping_violations SET count=$3,last_seen=$4 WHERE guild_id=$1 AND offender_id=$2",
                            guild_id, offender_id, cnt, now_ts)
            return "mute" if cnt > threshold else "warn"

    # ---------- Giveaways ----------
    async def giveaway_create(self, guild_id: int, channel_id: int, prize: str, winners: int, ends_ts: int, created_by: int | None, ping_role_id: int | None):
        ends_at = datetime.fromtimestamp(ends_ts, tz=UTC)
        async with self.pool.acquire() as c:
            row = await c.fetchrow("""
                INSERT INTO giveaways(guild_id, channel_id, prize, winners, ends_at, created_by, ping_role_id, status)
                VALUES($1,$2,$3,$4,$5,$6,$7,'running')
                RETURNING id, guild_id, channel_id, prize, winners, ends_at, ping_role_id, status
            """, guild_id, channel_id, prize, int(winners), ends_at, created_by, ping_role_id)
        return dict(row)

    async def giveaway_set_message(self, giveaway_id: int, message_id: int):
        async with self.pool.acquire() as c:
            await c.execute("UPDATE giveaways SET message_id=$2 WHERE id=$1", giveaway_id, message_id)

    async def giveaway_enter(self, giveaway_id: int, user_id: int) -> bool:
        async with self.pool.acquire() as c:
            status = await c.fetchval("SELECT status FROM giveaways WHERE id=$1", giveaway_id)
            if status != "running":
                return False
            try:
                await c.execute("INSERT INTO giveaway_entries(giveaway_id, user_id) VALUES($1,$2) ON CONFLICT DO NOTHING",
                                giveaway_id, user_id)
                return True
            except Exception:
                return False

    async def giveaway_list(self, guild_id: int) -> List[Dict]:
        async with self.pool.acquire() as c:
            rows = await c.fetch("SELECT * FROM giveaways WHERE guild_id=$1 ORDER BY created_at DESC", guild_id)
            return [dict(r) for r in rows]

    async def giveaway_list_due(self) -> List[Dict]:
        async with self.pool.acquire() as c:
            rows = await c.fetch("""
                SELECT * FROM giveaways
                WHERE status='running' AND ends_at <= NOW()
                ORDER BY ends_at ASC
            """)
            return [dict(r) for r in rows]

    async def giveaway_end(self, giveaway_id: int) -> Dict:
        async with self.pool.acquire() as c:
            gw = await c.fetchrow("SELECT * FROM giveaways WHERE id=$1", giveaway_id)
            if not gw or gw["status"] != "running":
                return {"ok": False, "reason": "not_running"}
            entries = await c.fetch("SELECT user_id FROM giveaway_entries WHERE giveaway_id=$1", giveaway_id)
            pool = [int(r["user_id"]) for r in entries]
            winners_count = max(1, int(gw["winners"]))
            if len(pool) == 0:
                sel = []
            elif len(pool) <= winners_count:
                sel = list(dict.fromkeys(pool))  # únicos
            else:
                sel = random.sample(pool, winners_count)
            await c.execute("UPDATE giveaways SET status='ended' WHERE id=$1", giveaway_id)
            return {
                "ok": True,
                "id": int(gw["id"]),
                "guild_id": int(gw["guild_id"]),
                "channel_id": int(gw["channel_id"]),
                "message_id": int(gw["message_id"]) if gw["message_id"] else None,
                "prize": gw["prize"],
                "winners": sel
            }

    # ---------- GOLD ACCOUNTS ----------
    async def gold_add(self, guild_id: int, admin_id: int, account_name: str, ugphone: str) -> dict:
        account_name = account_name.strip()
        ugphone = ugphone.strip()
        if not account_name or not ugphone:
            return {"ok": False, "reason": "bad_input"}
        async with self.pool.acquire() as c:
            try:
                r = await c.fetchrow("""
                    INSERT INTO gold_accounts(guild_id, admin_id, account_name, ugphone)
                    VALUES($1,$2,$3,$4)
                    RETURNING id, guild_id, admin_id, account_name, ugphone, created_at
                """, guild_id, admin_id, account_name, ugphone)
                return {"ok": True, "row": dict(r)}
            except asyncpg.UniqueViolationError:
                return {"ok": False, "reason": "duplicate"}

    async def gold_list(self, guild_id: int, admin_id: int) -> List[dict]:
        async with self.pool.acquire() as c:
            rows = await c.fetch("""
                SELECT id, account_name, ugphone, created_at
                FROM gold_accounts
                WHERE guild_id=$1 AND admin_id=$2
                ORDER BY created_at DESC
            """, guild_id, admin_id)
            return [dict(r) for r in rows]

    async def gold_remove(self, guild_id: int, admin_id: int, account_name: str) -> bool:
        async with self.pool.acquire() as c:
            res = await c.execute("""
                DELETE FROM gold_accounts
                WHERE guild_id=$1 AND admin_id=$2 AND account_name=$3
            """, guild_id, admin_id, account_name.strip())
            return res.endswith("DELETE 1")
