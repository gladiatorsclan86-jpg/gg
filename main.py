# main.py
import os, io, time, asyncio, random, html, base64, re, json
import discord
from discord import app_commands
from aiohttp import web, ClientSession, ClientTimeout
from typing import Optional, List, Dict, Any
from urllib.parse import urlsplit, urlunsplit, parse_qsl, urlencode, unquote
from datetime import datetime, timedelta, timezone

# ---------- Transcript fallback / import ----------
try:
    from transcript_html import render_transcript_html
except Exception:
    async def render_transcript_html(guild, tinfo, messages, opener_name, use_msg_intent):
        rows = []
        rows.append("<html><head><meta charset='utf-8'><title>Transcript</title></head><body style='font-family:system-ui,Segoe UI,Arial,sans-serif;background:#0f172a;color:#e2e8f0'>")
        rows.append(f"<h2 style='margin:16px 0'>🎟️ Transcripción Ticket #{tinfo.get('id','?')} — {guild.name}</h2>")
        rows.append("<div style='padding:12px;border-radius:12px;background:#111827'>")
        for m in messages:
            ts = ""
            try: ts = m["created_at"].strftime("%Y-%m-%d %H:%M:%S")
            except: pass
            content = (m.get("content") or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")
            atts = m.get("attachments") or []
            att = (" " + " ".join(f"<a href='{a}' target='_blank'>{a}</a>" for a in atts)) if atts else ""
            rows.append(f"<div style='margin:6px 0;padding:8px 12px;border-radius:10px;background:#0b1220'><b>{m.get('author_id','?')}</b> <span style='opacity:.7'>[{ts}]</span><br>{content}{att}</div>")
        rows.append("</div></body></html>")
        return "\n".join(rows).encode("utf-8")

from db import Database

# ================== CONFIG ==================
def env_truthy(key: str, default=False):
    v = os.getenv(key)
    if v is None: return default
    return str(v).strip().lower() in {"1","true","yes","y","on"}

TOKEN = os.getenv("DISCORD_TOKEN","").strip()

RAW_DSN = "postgresql://neondb_owner:npg_57UtPXDAETBy@ep-shiny-recipe-ae4mt1ry-pooler.c-2.us-east-2.aws.neon.tech/neondb?sslmode=require&channel_binding=require"
def _sanitize_dsn(dsn: str) -> str:
    s = urlsplit(dsn)
    qs = parse_qsl(s.query, keep_blank_values=True)
    qs = [(k,v) for (k,v) in qs if k.lower()!="channel_binding"]
    if not any(k.lower()=="sslmode" for k,_ in qs):
        qs.append(("sslmode","require"))
    new_q = urlencode(qs)
    return urlunsplit((s.scheme, s.netloc, s.path, new_q, s.fragment))
DATABASE_URL = _sanitize_dsn(os.getenv("DATABASE_URL", RAW_DSN).strip())

API_SECRET = os.getenv("API_SECRET","").strip()
MAX_CODES_INLINE = 25
TICKET_INACTIVE_MIN = 180
USE_MSG_INTENT = env_truthy("MESSAGE_CONTENT_INTENT", False)

TRIVIA_USE_WEB = env_truthy("TRIVIA_USE_WEB", True)
TRIVIA_PROVIDER_DEFAULT = os.getenv("TRIVIA_PROVIDER","auto").lower().strip()

BUG_INPUT_CHANNEL_DEFAULT = int(os.getenv("BUG_INPUT_CHANNEL_ID", "1404332148277776454"))
BUG_LOG_CHANNEL_DEFAULT   = int(os.getenv("BUG_LOG_CHANNEL_ID",   "1398444193336135750"))
LEVEL_UP_CHANNEL_ID       = int(os.getenv("LEVEL_UP_CHANNEL_ID",  "1404709573339910266"))

BUG_NOTICE_TEXT = (
    "🚨 **ACA SOLO SE HABLARÁ DE BUGS.** Si hablas de otra cosa **podrías ser baneado.**\n"
    "✍️ Describe tu bug con detalles, pasos y evidencia. El bot lo registrará automáticamente."
)

print(f"[BOOT] MESSAGE_CONTENT_INTENT raw='{os.getenv('MESSAGE_CONTENT_INTENT')}' -> {USE_MSG_INTENT}")

COLORS = {
    "success": 0x57F287, "error": 0xED4245, "info": 0x5865F2,
    "warn": 0xFEE75C, "accent": 0x2B2D31, "panel": 0x0EA5E9, "ticket": 0x00A6A6
}

# ---------- Discord client + tree ----------
intents = discord.Intents.default()
intents.message_content = USE_MSG_INTENT
intents.members = True
client = discord.Client(intents=intents)
tree = app_commands.CommandTree(client)
db: Database | None = None

# ===================== Helpers UI =====================
def brand_embed(title: str, desc: str|None=None, color: int=COLORS["info"]) -> discord.Embed:
    e = discord.Embed(title=title, description=desc or "", color=color)
    e.set_footer(text="GGhud • Calidad y velocidad ⚡")
    return e

def safe_ch_name(s: str) -> str:
    s = s.lower().replace(" ","-")
    s = "".join(c for c in s if c.isalnum() or c in "-_")
    return s or "ticket"

async def fetch_member_name(guild: discord.Guild, uid: int) -> str:
    m = guild.get_member(uid)
    if m: return m.display_name
    try:
        u = await client.fetch_user(uid); return u.name if u else str(uid)
    except: return str(uid)

def has_manage_guild(inter: discord.Interaction) -> bool:
    try: return inter.user.guild_permissions.manage_guild
    except: return False

# ===================== Utils básico =====================
@tree.command(name="help", description="Muestra ayuda del bot.")
async def help_cmd(inter: discord.Interaction):
    desc = (
        "### 🎫 Tickets\n"
        "`/ticket panel` • `/ticket create` • `/ticket close` • `/ticket reopen` • `/ticket stats`\n"
        "`/ticket adduser` • `/ticket removeuser` • `/ticket rename` • `/ticket allowrole add/remove/list`\n\n"
        "### 🐞 Bugs\n"
        "`/bug set_input` • `/bug set_log` • `/bug settings` • `/bug ping_mode` • `/bug list` • `/bug resolve` • `/bug repost_notice`\n\n"
        "### 🛡️ Anti-Ping\n"
        "`/antiping add` • `/antiping remove` • `/antiping list` • `/antiping settings`\n\n"
        "### 🔑 Keys / 🎁 Premios\n"
        "`/checkkey` • `/genkey` • `/prize add/list/remove`\n\n"
        "### ✨ Gold Accounts\n"
        "`/goldaccount add` • `/goldaccount list` • `/goldaccount remove`\n\n"
        "### 🧠 Trivia\n"
        "`/trivia start` • `/trivia stop` • `/trivia leaderboard`\n\n"
        "### 🎉 Giveaways\n"
        "`/giveaway create` • `/giveaway end` • `/giveaway reroll` • `/giveaway list`\n\n"
        "### 💰 Economía\n"
        "`/balance` • `/daily` • `/work` • `/give`\n\n"
        "### 🆙 Niveles\n"
        "`/level` • `/rank`\n\n"
        "### 👮 Moderación\n"
        "`/ban` • `/purge` • `/warn` • `/infractions` • `/infractions_clear`\n\n"
        "### 🗳️ Encuestas\n"
        "`/poll`\n\n"
        "### ℹ️ Otros\n"
        "`/ping` • `/serverinfo` • `/avatar` • `/userinfo`\n"
    )
    await inter.response.send_message(embed=brand_embed("📖 Ayuda — GGhud", desc, COLORS["panel"]), ephemeral=True)

@tree.command(name="ping", description="Mide latencia.")
async def ping_cmd(inter: discord.Interaction):
    await inter.response.send_message(embed=brand_embed("🏓 Pong!", f"Latencia ≈ **{round(client.latency*1000)} ms**", COLORS["success"]), ephemeral=True)

@tree.command(name="serverinfo", description="Info del servidor.")
async def serverinfo_cmd(inter: discord.Interaction):
    g = inter.guild
    if not g: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    e = brand_embed("🧭 Información del servidor", color=COLORS["info"])
    e.add_field(name="Nombre", value=g.name, inline=True)
    e.add_field(name="Miembros", value=str(g.member_count), inline=True)
    e.add_field(name="Canales", value=str(len(g.channels)), inline=True)
    e.set_thumbnail(url=g.icon.url if g.icon else discord.Embed.Empty)
    await inter.response.send_message(embed=e, ephemeral=True)

@tree.command(name="avatar", description="Muestra avatar de un usuario.")
async def avatar_cmd(inter: discord.Interaction, usuario: Optional[discord.User]=None):
    u = usuario or inter.user
    e = brand_embed(f"🖼️ Avatar — {u}", color=COLORS["info"])
    e.set_image(url=u.display_avatar.url)
    await inter.response.send_message(embed=e, ephemeral=True)

@tree.command(name="userinfo", description="Muestra información de un usuario.")
async def userinfo_cmd(inter: discord.Interaction, usuario: Optional[discord.User]=None):
    u = usuario or inter.user
    e = brand_embed(f"👤 Userinfo — {u}", color=COLORS["info"])
    e.add_field(name="ID", value=str(u.id), inline=True)
    e.add_field(name="Creado", value=u.created_at.strftime("%Y-%m-%d"), inline=True)
    if inter.guild:
        m = inter.guild.get_member(u.id)
        if m:
            e.add_field(name="Se unió", value=m.joined_at.strftime("%Y-%m-%d") if m.joined_at else "—", inline=True)
            roles = [r.mention for r in m.roles if r != inter.guild.default_role]
            if roles:
                e.add_field(name="Roles", value=", ".join(roles)[:1024], inline=False)
    await inter.response.send_message(embed=e, ephemeral=True)

# ===================== Bugs helpers =====================
async def get_bug_ids(g: discord.Guild):
    cfg = await db.get_config(g.id)
    in_ch = cfg.get("bug_input_channel_id") or BUG_INPUT_CHANNEL_DEFAULT
    log_ch = cfg.get("bug_log_channel_id") or BUG_LOG_CHANNEL_DEFAULT
    return int(in_ch), int(log_ch)

# ===================== Categorías Tickets =====================
async def get_or_create_ticket_category(guild: discord.Guild):
    cfg = await db.get_config(guild.id)
    cat = guild.get_channel(cfg.get("category_id") or 0)
    if isinstance(cat, discord.CategoryChannel): return cat
    for c in guild.categories:
        if "ticket" in c.name.lower():
            await db.set_category(guild.id, c.id); return c
    cat = await guild.create_category(name="🎫 tickets")
    await db.set_category(guild.id, cat.id)
    return cat

async def get_or_create_closed_category(guild: discord.Guild) -> discord.CategoryChannel:
    for c in guild.categories:
        if "closed" in c.name.lower() or "cerrado" in c.name.lower() or "🗄️" in c.name:
            return c
    return await guild.create_category(name="🗄️ closed-tickets")

async def resolve_staff_role(guild: discord.Guild) -> Optional[discord.Role]:
    cfg = await db.get_config(guild.id)
    role = guild.get_role(cfg.get("staff_role_id") or 0)
    if role: return role
    for r in sorted(guild.roles, key=lambda x: x.position, reverse=True):
        if r.permissions.administrator: return r
    for name in ("Admin","Administrador","Staff","Moderador"):
        r = discord.utils.get(guild.roles, name=name)
        if r: return r
    return None

# ===================== KEYS =====================
@tree.command(name="checkkey", description="Verifica una key y entrega su premio (marca como usada).")
@app_commands.describe(code="Código a canjear (ej: ABCD-EFGH-1234)")
async def checkkey_cmd(inter: discord.Interaction, code: str):
    res = await db.check_key(code, inter.user.id)
    if not res["ok"]:
        reasons = {
            "not_found":"El código no existe.",
            "used":"Este código ya fue usado.",
            "expired":"El código está expirado.",
            "prize_missing":"El premio asociado ya no existe.",
            "no_prizes":"No hay premios configurados aún."
        }
        return await inter.response.send_message(
            embed=brand_embed("❌ Key inválida", reasons.get(res.get("reason"), "No se pudo validar la key."), COLORS["error"]),
            ephemeral=True
        )
    prize = res["prize"]
    e = brand_embed("🎉 ¡Key verificada!", "Tu premio ha sido revelado.", COLORS["success"])
    e.add_field(name="Código", value=f"`{res['code']}`", inline=True)
    e.add_field(name="Tipo", value=("Fijo" if res["mode"] == "fixed" else "Random"), inline=True)
    e.add_field(name="Premio", value=f"**{prize['name']}**", inline=False)
    if prize.get("description"):
        e.add_field(name="Descripción", value=prize["description"], inline=False)
    await inter.response.send_message(embed=e, ephemeral=True)

@app_commands.default_permissions(manage_guild=True)
@tree.command(name="genkey", description="Genera nuevas keys (admin).")
@app_commands.describe(amount="Cantidad (1-100)", mode="random o fixed", prize="Premio (si fixed)", expires_days="Expira en N días (opcional)")
async def genkey_cmd(inter: discord.Interaction, amount: app_commands.Range[int,1,100], mode: str, prize: str|None=None, expires_days: app_commands.Range[int,1,365]|None=None):
    if not has_manage_guild(inter):
        return await inter.response.send_message("Necesitas **Manage Server**.", ephemeral=True)
    res = await db.create_keys(amount, mode.lower().strip(), prize, inter.user.id, expires_days)
    if not res["ok"]:
        msg = {
            "prize_required":"Indica `prize` cuando `mode=fixed`.",
            "prize_not_found":"No encontré ese premio. Usa `/prize add` o `/prize list`."
        }.get(res.get("reason"), "Error creando claves.")
        return await inter.response.send_message(msg, ephemeral=True)

    e = brand_embed("🧪 Claves generadas", color=COLORS["info"])
    e.add_field(name="Cantidad", value=str(len(res["codes"])), inline=True)
    e.add_field(name="Modo", value=("Fijo" if res["mode"]=="fixed" else "Random"), inline=True)
    if res.get("prize"):
        e.add_field(name="Premio", value=res["prize"], inline=True)
    if res.get("expires_days"):
        e.add_field(name="Expira en", value=f"{res['expires_days']} días", inline=True)
    if len(res["codes"]) <= MAX_CODES_INLINE:
        e.description = "```\n" + "\n".join("• " + c for c in res["codes"]) + "\n```"
        await inter.response.send_message(embed=e, ephemeral=True)
    else:
        content = "\n".join(res["codes"])
        file = discord.File(io.BytesIO(content.encode()), filename=f"keys_{int(time.time())}.txt")
        e.set_footer(text="Adjunté un archivo con las claves")
        await inter.response.send_message(embed=e, file=file, ephemeral=True)

# ----------------------- PRIZE group -----------------------
class PrizeGroup(app_commands.Group):
    def __init__(self):
        super().__init__(name="prize", description="Administra premios (admin).")

    @app_commands.command(name="add", description="Agrega un premio")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(name="Nombre", description="Descripción", weight="Peso (1-100)")
    async def add(self, inter: discord.Interaction, name: str, description: str|None=None, weight: app_commands.Range[int,1,100]=1):
        if not has_manage_guild(inter):
            return await inter.response.send_message("Necesitas **Manage Server**.", ephemeral=True)
        res = await db.add_prize(name, description or "", int(weight))
        if not res["ok"] and res.get("reason") == "duplicate":
            return await inter.response.send_message("Ya existe un premio con ese nombre.", ephemeral=True)
        p = res["prize"]
        e = brand_embed("✅ Premio agregado", color=COLORS["success"])
        e.add_field(name="Nombre", value=p["name"], inline=True)
        e.add_field(name="Peso", value=str(p["weight"]), inline=True)
        if p["description"]:
            e.add_field(name="Descripción", value=p["description"], inline=False)
        await inter.response.send_message(embed=e, ephemeral=True)

    @app_commands.command(name="list", description="Lista los premios")
    @app_commands.default_permissions(manage_guild=True)
    async def list_(self, inter: discord.Interaction):
        prizes = await db.list_prizes()
        if not prizes:
            return await inter.response.send_message("Aún no hay premios.", ephemeral=True)
        body = []
        for i,p in enumerate(prizes, start=1):
            line = f"**{i}. {p['name']}** — peso: {p.get('weight',1)}"
            if p.get("description"):
                line += f"\n> {p['description']}"
            body.append(line)
        await inter.response.send_message(embed=brand_embed("🎁 Premios", "\n\n".join(body), COLORS["info"]), ephemeral=True)

    @app_commands.command(name="remove", description="Elimina un premio")
    @app_commands.default_permissions(manage_guild=True)
    async def remove(self, inter: discord.Interaction, name: str):
        if not has_manage_guild(inter):
            return await inter.response.send_message("Necesitas **Manage Server**.", ephemeral=True)
        res = await db.remove_prize(name)
        if not res["ok"]:
            return await inter.response.send_message("No encontré ese premio.", ephemeral=True)
        await inter.response.send_message(embed=brand_embed("🗑️ Premio eliminado", f"Se eliminó **{name}**.", COLORS["error"]), ephemeral=True)

tree.add_command(PrizeGroup())

# ===================== Moderación =====================
@app_commands.default_permissions(ban_members=True)
@tree.command(name="ban", description="Banea a un miembro (admin).")
@app_commands.describe(user="Usuario", reason="Motivo", delete_days="Borrar mensajes últimos N días (0-7)")
async def ban_cmd(inter: discord.Interaction, user: discord.User, reason: str|None=None, delete_days: app_commands.Range[int,0,7]=0):
    if not inter.guild:
        return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    if not inter.user.guild_permissions.ban_members:
        return await inter.response.send_message("Necesitas **Ban Members**.", ephemeral=True)
    if user.id in (inter.user.id, client.user.id):
        return await inter.response.send_message("No puedes banear a ese usuario.", ephemeral=True)
    try:
        await inter.guild.ban(user, reason=reason, delete_message_days=delete_days)
        e = brand_embed("🔨 Usuario baneado", color=COLORS["error"])
        e.add_field(name="Usuario", value=f"{user} (`{user.id}`)", inline=False)
        e.add_field(name="Motivo", value=reason or "—", inline=True)
        e.add_field(name="Borrar mensajes", value=f"{delete_days} días", inline=True)
        await inter.response.send_message(embed=e, ephemeral=True)
    except discord.Forbidden:
        await inter.response.send_message("No tengo permisos para banear a ese usuario.", ephemeral=True)
    except Exception as ex:
        await inter.response.send_message(f"Error: {ex}", ephemeral=True)

@app_commands.default_permissions(manage_messages=True)
@tree.command(name="purge", description="Borra mensajes recientes del canal (admin).")
@app_commands.describe(amount="Cantidad (1-100)")
async def purge_cmd(inter: discord.Interaction, amount: app_commands.Range[int,1,100]):
    if not inter.guild:
        return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    if not inter.user.guild_permissions.manage_messages:
        return await inter.response.send_message("Necesitas **Manage Messages**.", ephemeral=True)
    if not isinstance(inter.channel, (discord.TextChannel, discord.Thread)):
        return await inter.response.send_message("Solo en canales de texto.", ephemeral=True)
    await inter.response.defer(ephemeral=True, thinking=True)
    try:
        deleted = await inter.channel.purge(limit=int(amount))
        await inter.followup.send(embed=brand_embed("🧹 Purge", f"Se borraron **{len(deleted)}** mensajes.", COLORS["info"]), ephemeral=True)
    except discord.Forbidden:
        await inter.followup.send("No tengo permisos para borrar mensajes aquí.", ephemeral=True)
    except Exception as ex:
        await inter.followup.send(f"Error: {ex}", ephemeral=True)

# ===================== Tickets =====================
BUY_PLANS = [
    ("🕐 1 Day — 80 R$ / $1.00 USD", "1d"),
    ("🕒 3 Days — 200 R$ / $2.50 USD", "3d"),
    ("🗓️ 5 Days — 300 R$ / $3.50 USD", "5d"),
    ("📅 1 Week — 350 R$ / $4.00 USD", "1w"),
    ("🗓️ 2 Weeks — 550 R$ / $6.50 USD", "2w"),
    ("📆 1 Month — 800 R$ / $10.00 USD", "1m"),
    ("🔒 Perma — 2,000 R$ / $15.00 USD", "perma"),
]

class CloseModal(discord.ui.Modal, title="Cerrar ticket"):
    motivo = discord.ui.TextInput(label="Motivo (opcional)", required=False, max_length=200)
    async def on_submit(self, inter: discord.Interaction):
        await perform_close(inter, str(self.motivo) if self.motivo else "")

class BuyPlanSelect(discord.ui.Select):
    def __init__(self):
        opts = [discord.SelectOption(label=label, value=val) for (label, val) in BUY_PLANS]
        super().__init__(placeholder="Elige lo que deseas comprar…", min_values=1, max_values=1, options=opts, custom_id="buy_select")
    async def callback(self, inter: discord.Interaction):
        val = self.values[0]
        await db.set_purchase_plan(inter.channel.id, val)
        await inter.response.send_message("🧾 Plan seleccionado. Elige el método de pago:", ephemeral=True)
        await inter.followup.send(embed=brand_embed("💳 Método de pago", "Selecciona **PayPal** o **Robux**.", COLORS["info"]), view=PaymentMethodView(), ephemeral=True)

class PaymentMethodView(discord.ui.View):
    def __init__(self): super().__init__(timeout=300)
    @discord.ui.button(label="PayPal", style=discord.ButtonStyle.primary, custom_id="pay_paypal")
    async def paypal(self, inter: discord.Interaction, button: discord.ui.Button):
        await db.set_payment_method(inter.channel.id, "paypal")
        await inter.response.send_message("✅ **PayPal** seleccionado. Espera a que el staff te atienda.", ephemeral=True)
        await inter.channel.send("💸 **Pago por PayPal** seleccionado. Un staff te atenderá en breve.")
    @discord.ui.button(label="Robux", style=discord.ButtonStyle.success, custom_id="pay_robux")
    async def robux(self, inter: discord.Interaction, button: discord.ui.Button):
        await db.set_payment_method(inter.channel.id, "robux")
        await inter.response.send_message("✅ **Robux** seleccionado. Envía el **link del Gamepass** cuando lo tengas.", ephemeral=True)
        await inter.channel.send("🟩 **Pago con Robux** seleccionado. Comparte el **link del Gamepass**.")

class ControlsView(discord.ui.View):
    def __init__(self, timeout: Optional[float]=None): super().__init__(timeout=timeout)
    @discord.ui.button(label="🎯 Reclamar", style=discord.ButtonStyle.primary, custom_id="ticket_claim")
    async def claim(self, inter: discord.Interaction, button: discord.ui.Button):
        if not inter.user.guild_permissions.manage_messages:
            return await inter.response.send_message("Solo staff puede reclamar.", ephemeral=True)
        await db.set_claim(inter.channel.id, inter.user.id)
        await inter.response.send_message(f"✅ Ticket reclamado por {inter.user.mention}.", ephemeral=True)
        await inter.channel.send(f"🎯 {inter.user.mention} ha **reclamado** este ticket.")
    @discord.ui.button(label="🚫 Liberar", style=discord.ButtonStyle.secondary, custom_id="ticket_unclaim")
    async def unclaim(self, inter: discord.Interaction, button: discord.ui.Button):
        if not inter.user.guild_permissions.manage_messages:
            return await inter.response.send_message("Solo staff puede liberar.", ephemeral=True)
        await db.set_claim(inter.channel.id, None)
        await inter.response.send_message("✅ Ticket liberado.", ephemeral=True)
        await inter.channel.send("🚫 El ticket fue **liberado**.")
    @discord.ui.button(label="✅ Cerrar", style=discord.ButtonStyle.success, custom_id="ticket_close")
    async def close(self, inter: discord.Interaction, button: discord.ui.Button):
        modal = CloseModal(); await inter.response.send_modal(modal)

class TicketPanel(discord.ui.View):
    def __init__(self, timeout: Optional[float]=None): super().__init__(timeout=timeout)
    @discord.ui.button(label="🛒 Comprar", style=discord.ButtonStyle.success, custom_id="ticket_comprar")
    async def comprar(self, inter: discord.Interaction, button: discord.ui.Button): await self._open_ticket(inter, "comprar")
    @discord.ui.button(label="🛠️ Soporte", style=discord.ButtonStyle.primary, custom_id="ticket_soporte")
    async def soporte(self, inter: discord.Interaction, button: discord.ui.Button): await self._open_ticket(inter, "soporte")
    @discord.ui.button(label="📘 Guía", style=discord.ButtonStyle.secondary, custom_id="ticket_help")
    async def help_(self, inter: discord.Interaction, button: discord.ui.Button):
        guide = ("**Cómo funciona**\n1) Pulsa **🛒 Comprar** o **🛠️ Soporte**\n2) Se crea tu canal privado con el staff\n3) Si es compra: elige plan y método de pago\n4) Usa **✅ Cerrar** cuando termine tu caso")
        await inter.response.send_message(embed=brand_embed("📘 Guía rápida — GGhud Tickets", guide, COLORS['panel']), ephemeral=True)
    async def _open_ticket(self, inter: discord.Interaction, kind: str):
        if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
        user = inter.user
        cat = await get_or_create_ticket_category(inter.guild)
        ch_name = f"ticket-{kind}-{safe_ch_name(user.name)}"[:95]
        overwrites = {
            inter.guild.default_role: discord.PermissionOverwrite(view_channel=False),
            user: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True),
            inter.guild.me: discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_channels=True)
        }
        staff_role = await resolve_staff_role(inter.guild)
        if staff_role:
            overwrites[staff_role] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True, manage_messages=True)
        extra_ids = await db.list_allowed_roles(inter.guild.id)
        for rid in extra_ids:
            r = inter.guild.get_role(rid)
            if r: overwrites[r] = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)
        channel = await inter.guild.create_text_channel(name=ch_name, category=cat, overwrites=overwrites, reason=f"Ticket {kind} de {user}")
        await db.create_ticket(inter.guild.id, user.id, kind, channel.id)
        staff_ping = staff_role.mention if staff_role else (inter.guild.owner.mention if inter.guild.owner else "")
        header = "🛒 **Ticket de Compra**" if kind=="comprar" else "🛠️ **Ticket de Soporte**"
        desc   = "Selecciona lo que deseas comprar en el menú de abajo." if kind=="comprar" else "Cuéntanos tu problema y el staff te ayudará."
        e = brand_embed("🎟️ Ticket creado", f"{header}\n**Usuario:** {user.mention}\n\n{desc}", COLORS["ticket"])
        await channel.send(content=staff_ping, embed=e, view=ControlsView())
        if kind == "comprar":
            lines = "\n".join([f"• {label}" for (label, _) in BUY_PLANS])
            await channel.send(embed=brand_embed("📆 Optional Support Prices", lines, COLORS["accent"]))
            view = discord.ui.View(timeout=600); view.add_item(BuyPlanSelect())
            try:
                await user.send(embed=brand_embed("🛒 Elige tu plan", "Selecciona una opción y luego el método de pago.", COLORS["info"]))
                await user.send(view=view)
                await channel.send(f"{user.mention} te envié un DM para elegir el plan. Si no llega, usa este menú aquí:", view=view)
            except Exception:
                await channel.send(content=f"{user.mention}", embed=brand_embed("🛒 Elige tu plan", "Selecciona una opción y luego el método de pago.", COLORS["info"]), view=view)
        await inter.response.send_message(f"✅ Ticket creado: {channel.mention}", ephemeral=True)

class TicketGroup(app_commands.Group):
    def __init__(self): super().__init__(name="ticket", description="Sistema de tickets")
    @app_commands.command(name="panel", description="Publica el panel de tickets (evita duplicados).")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(repost="Ignorar panel guardado y publicar uno nuevo")
    async def panel(self, inter: discord.Interaction, repost: Optional[bool] = False):
        if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
        prev_mid = await db.find_panel_in_channel(inter.guild.id, inter.channel.id)
        if prev_mid and not repost:
            try:
                await inter.channel.fetch_message(prev_mid)
                link = f"https://discord.com/channels/{inter.guild.id}/{inter.channel.id}/{prev_mid}"
                return await inter.response.send_message(embed=brand_embed("ℹ️ Ya existe un panel aquí", f"[Ir al panel existente]({link})", COLORS["info"]), ephemeral=True)
            except discord.NotFound: pass
        body = ("Bienvenido a **GGhud Tickets**\n• **🛒 Comprar** — packs, planes y beneficios.\n• **🛠️ Soporte** — ayuda técnica.\n• **📘 Guía** — pasos rápidos.\n\n⏱️ *Los tickets inactivos 3h se cierran automáticamente.*")
        emb = brand_embed("🎫 Centro de Tickets", body, COLORS["panel"])
        msg = await inter.channel.send(embed=emb, view=TicketPanel(timeout=None))
        await db.add_panel_record(inter.guild.id, inter.channel.id, msg.id)
        await inter.response.send_message("✅ Panel publicado.", ephemeral=True)

    @app_commands.command(name="create", description="Crea un ticket manualmente.")
    @app_commands.choices(tipo=[app_commands.Choice(name="Comprar", value="comprar"), app_commands.Choice(name="Soporte", value="soporte")])
    async def create(self, inter: discord.Interaction, tipo: app_commands.Choice[str]):
        await TicketPanel()._open_ticket(inter, tipo.value)

    @app_commands.command(name="close", description="Cierra el ticket actual.")
    async def close(self, inter: discord.Interaction, motivo: Optional[str] = None):
        if motivo is None: return await inter.response.send_modal(CloseModal())
        await perform_close(inter, motivo)

    @app_commands.command(name="reopen", description="Reabre un ticket cerrado (staff).")
    @app_commands.default_permissions(manage_messages=True)
    async def reopen(self, inter: discord.Interaction):
        if not inter.guild or not isinstance(inter.channel, discord.TextChannel):
            return await inter.response.send_message("Usa esto dentro del canal del ticket.", ephemeral=True)
        t = await db.fetch_ticket_by_channel(inter.channel.id)
        if not t or t["status"] != "closed":
            return await inter.response.send_message("Este canal no es un ticket cerrado.", ephemeral=True)
        cat = await get_or_create_ticket_category(inter.guild)
        await inter.channel.edit(category=cat, name=f"ticket-reopened-{inter.channel.name}"[:95])
        opener_id = int(t["opener_id"])
        opener_member = inter.guild.get_member(opener_id) or await inter.guild.fetch_member(opener_id)
        if opener_member:
            allow = discord.PermissionOverwrite(view_channel=True, send_messages=True, read_message_history=True)
            await inter.channel.set_permissions(opener_member, overwrite=allow, reason="Ticket reabierto: restaurar acceso del autor")
        await db.reopen_ticket_by_channel(inter.channel.id)
        await inter.response.send_message("✅ Ticket reabierto.", ephemeral=True)

    @app_commands.command(name="adduser", description="Añade un usuario al ticket (staff).")
    @app_commands.default_permissions(manage_messages=True)
    async def adduser(self, inter: discord.Interaction, usuario: discord.User):
        if not isinstance(inter.channel, discord.TextChannel):
            return await inter.response.send_message("Solo en canales de ticket.", ephemeral=True)
        await inter.channel.set_permissions(usuario, view_channel=True, send_messages=True, read_message_history=True)
        await inter.response.send_message(f"✅ {usuario.mention} añadido.", ephemeral=True)

    @app_commands.command(name="removeuser", description="Quita un usuario del ticket (staff).")
    @app_commands.default_permissions(manage_messages=True)
    async def removeuser(self, inter: discord.Interaction, usuario: discord.User):
        if not isinstance(inter.channel, discord.TextChannel):
            return await inter.response.send_message("Solo en canales de ticket.", ephemeral=True)
        await inter.channel.set_permissions(usuario, overwrite=None)
        await inter.response.send_message(f"✅ {usuario.mention} removido.", ephemeral=True)

    @app_commands.command(name="rename", description="Renombra el ticket (staff).")
    @app_commands.default_permissions(manage_channels=True)
    async def rename(self, inter: discord.Interaction, nuevo_nombre: str):
        if not isinstance(inter.channel, discord.TextChannel):
            return await inter.response.send_message("Solo en canales de ticket.", ephemeral=True)
        await inter.channel.edit(name=safe_ch_name(nuevo_nombre)[:95])
        await inter.response.send_message("✅ Renombrado.", ephemeral=True)

    @app_commands.command(name="stats", description="Muestra estadísticas de tickets.")
    async def stats(self, inter: discord.Interaction):
        c = await db.count_open_tickets(inter.guild.id)
        u = await db.count_user_opened(inter.user.id)
        e = brand_embed("📊 Stats de Tickets", f"Abiertos en el server: **{c}**\nHas abierto: **{u}**", COLORS["panel"])
        await inter.response.send_message(embed=e, ephemeral=True)

# allowrole subcomandos
class TicketAllowRoleGroup(app_commands.Group):
    def __init__(self): super().__init__(name="allowrole", description="Roles extras que pueden ver tickets")
    @app_commands.command(name="add", description="Añade rol con acceso a tickets")
    @app_commands.default_permissions(manage_guild=True)
    async def add(self, inter: discord.Interaction, rol: discord.Role):
        await db.add_allowed_role(inter.guild.id, rol.id)
        await inter.response.send_message(f"✅ {rol.mention} podrá ver tickets nuevos.", ephemeral=True)
    @app_commands.command(name="remove", description="Quita rol con acceso a tickets")
    @app_commands.default_permissions(manage_guild=True)
    async def remove(self, inter: discord.Interaction, rol: discord.Role):
        await db.remove_allowed_role(inter.guild.id, rol.id)
        await inter.response.send_message(f"✅ {rol.mention} removido.", ephemeral=True)
    @app_commands.command(name="list", description="Lista roles con acceso a tickets")
    async def list_(self, inter: discord.Interaction):
        ids = await db.list_allowed_roles(inter.guild.id)
        if not ids: return await inter.response.send_message("No hay roles extra.", ephemeral=True)
        txt = "\n".join(f"- <@&{r}>" for r in ids)
        await inter.response.send_message(embed=brand_embed("🎫 Roles con acceso a tickets", txt, COLORS["info"]), ephemeral=True)

tree.add_command(TicketGroup())
tree.add_command(TicketAllowRoleGroup(), guild=None, override=True)

async def apply_closed_effects(channel: discord.TextChannel, opener_id: int):
    try:
        opener_member = channel.guild.get_member(opener_id) or await channel.guild.fetch_member(opener_id)
        if opener_member and not opener_member.guild_permissions.administrator:
            await channel.set_permissions(opener_member, overwrite=None)
            deny = discord.PermissionOverwrite(view_channel=False, send_messages=False, read_message_history=False)
            await channel.set_permissions(opener_member, overwrite=deny, reason="Ticket cerrado: remover acceso del autor")
    except Exception: pass
    try: await channel.set_permissions(channel.guild.default_role, view_channel=False)
    except Exception: pass
    try:
        closed_cat = await get_or_create_closed_category(channel.guild)
        if channel.category_id != closed_cat.id:
            await channel.edit(category=closed_cat)
    except Exception: pass
    try:
        new_name = channel.name
        if not new_name.startswith("closed-"): new_name = f"closed-{new_name}"
        await channel.edit(name=new_name[:95])
    except Exception: pass

async def perform_close(inter: discord.Interaction, motivo: str):
    if not inter.guild or not isinstance(inter.channel, discord.TextChannel):
        return await inter.response.send_message("Usa esto dentro del canal del ticket.", ephemeral=True)
    t = await db.fetch_ticket_by_channel(inter.channel.id)
    if not t or t["status"] != "open":
        return await inter.response.send_message("Este canal no es un ticket abierto.", ephemeral=True)
    allowed = (t["opener_id"] == inter.user.id) or inter.user.guild_permissions.manage_messages
    if not allowed:
        return await inter.response.send_message("No tienes permisos para cerrar este ticket.", ephemeral=True)

    await inter.response.defer(ephemeral=True, thinking=True)
    data = await db.close_ticket_by_channel(inter.channel.id, inter.user.id, motivo or "")
    if not data: return await inter.followup.send("No pude cerrar el ticket.", ephemeral=True)
    tinfo, messages = data

    lines = []
    for m in messages:
        stamp = m["created_at"].strftime("%Y-%m-%d %H:%M:%S")
        content = m["content"] or ""
        atts = m["attachments"] or []
        if atts: content += " " + " ".join(atts)
        lines.append(f"[{stamp}] ({m['author_id']}): {content}")
    txt = "\n".join(lines) if lines else "No hubo mensajes."
    file_txt = discord.File(io.BytesIO(txt.encode("utf-8")), filename=f"ticket_{tinfo['id']}_transcript.txt")

    opener_name = await fetch_member_name(inter.guild, tinfo["opener_id"])
    html_bytes = await render_transcript_html(inter.guild, tinfo, messages, opener_name, USE_MSG_INTENT)
    file_html = discord.File(io.BytesIO(html_bytes), filename=f"ticket_{tinfo['id']}_transcript.html")

    e = brand_embed("✅ Ticket cerrado", f"Motivo: {motivo or '—'}", COLORS["success"])
    await inter.channel.send(embed=e, files=[file_txt, file_html], view=None)
    await apply_closed_effects(inter.channel, int(tinfo["opener_id"]))
    try:
        user = await client.fetch_user(tinfo["opener_id"])
        if user:
            await user.send(embed=brand_embed("Tu ticket fue cerrado", f"Motivo: {motivo or '—'}", COLORS["info"]))
            await user.send(files=[discord.File(io.BytesIO(txt.encode()), filename=f"ticket_{tinfo['id']}.txt"),
                                   discord.File(io.BytesIO(html_bytes), filename=f"ticket_{tinfo['id']}.html")])
    except Exception: pass
    await inter.followup.send("Ticket cerrado, autor sin acceso y transcripciones enviadas.", ephemeral=True)

# =================== BUGS ===================
class BugGroup(app_commands.Group):
    def __init__(self): super().__init__(name="bug", description="Gestión de bugs")
    @app_commands.command(name="set_input", description="Configura el canal donde la gente escribe bugs.")
    @app_commands.default_permissions(manage_guild=True)
    async def set_input(self, inter: discord.Interaction, canal: discord.TextChannel):
        await db.set_bug_channels(inter.guild.id, input_channel_id=canal.id, log_channel_id=None)
        await inter.response.send_message(f"✅ Canal de entrada de bugs: {canal.mention}", ephemeral=True)
    @app_commands.command(name="set_log", description="Configura el canal donde el bot registra los bugs.")
    @app_commands.default_permissions(manage_guild=True)
    async def set_log(self, inter: discord.Interaction, canal: discord.TextChannel):
        await db.set_bug_channels(inter.guild.id, input_channel_id=None, log_channel_id=canal.id)
        await inter.response.send_message(f"✅ Canal de registro de bugs: {canal.mention}", ephemeral=True)
    @app_commands.command(name="repost_notice", description="Repone el mensaje fijo de reglas en el canal de bugs.")
    @app_commands.default_permissions(manage_guild=True)
    async def repost_notice(self, inter: discord.Interaction):
        in_id, _ = await get_bug_ids(inter.guild); ch = inter.guild.get_channel(in_id)
        if not ch or not isinstance(ch, discord.TextChannel):
            return await inter.response.send_message("Configura primero el canal con `/bug set_input`.", ephemeral=True)
        msg = await ch.send(embed=brand_embed("📢 Reglas del canal de bugs", BUG_NOTICE_TEXT, COLORS["error"]))
        try: await msg.pin()
        except Exception: pass
        await db.set_bug_notice(inter.guild.id, ch.id, msg.id)
        await inter.response.send_message("✅ Aviso repuesto y guardado.", ephemeral=True)
    @app_commands.command(name="settings", description="Muestra o ajusta config de bugs.")
    @app_commands.default_permissions(manage_guild=True)
    async def settings(self, inter: discord.Interaction, window_hours: Optional[app_commands.Range[int,1,24]]=None, mute_minutes: Optional[app_commands.Range[int,1,1440]]=None):
        if window_hours is None and mute_minutes is None:
            s = await db.get_bug_settings(inter.guild.id)
            desc = f"⏱️ Ventana: **{s['window_hours']}h**\n🔇 Mute: **{s['mute_minutes']}m**\n🔔 Ping: **{s['ping_mode']}**"
            return await inter.response.send_message(embed=brand_embed("⚙️ Bug Settings", desc, COLORS["info"]), ephemeral=True)
        await db.set_bug_settings(inter.guild.id, window_hours, mute_minutes)
        s = await db.get_bug_settings(inter.guild.id)
        await inter.response.send_message(embed=brand_embed("✅ Actualizado", f"✔️ Ventana: **{s['window_hours']}h** • Mute: **{s['mute_minutes']}m**", COLORS["success"]), ephemeral=True)
    @app_commands.command(name="ping_mode", description="Cómo pingear al registrar bug.")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.choices(modo=[app_commands.Choice(name="none (sin ping)", value="none"), app_commands.Choice(name="@here", value="here"),
                                app_commands.Choice(name="@everyone", value="everyone"), app_commands.Choice(name="rol staff", value="staff")])
    async def ping_mode(self, inter: discord.Interaction, modo: app_commands.Choice[str]):
        await db.set_bug_ping_mode(inter.guild.id, modo.value, None)
        await inter.response.send_message("✅ Config de ping actualizada.", ephemeral=True)
    @app_commands.command(name="list", description="Lista bugs pendientes.")
    async def bug_list(self, inter: discord.Interaction):
        bugs = await db.list_bugs(inter.guild.id, status="open")
        if not bugs:
            return await inter.response.send_message(embed=brand_embed("🐞 Bugs", "No hay bugs pendientes.", COLORS["info"]), ephemeral=True)
        rows = []
        for b in bugs[:20]:
            rows.append(f"**#{b['id']}** — <@{b['reporter_id']}> — <#{b['source_channel_id']}>")
            rows.append(f"> {b['content'][:160]}{'…' if len(b['content'])>160 else ''}")
        await inter.response.send_message(embed=brand_embed("🐞 Bugs pendientes", "\n".join(rows), COLORS["warn"]), ephemeral=True)
    @app_commands.default_permissions(manage_messages=True)
    @app_commands.command(name="resolve", description="Marca un bug como resuelto.")
    @app_commands.describe(bug_id="ID del bug", motivo="Motivo/nota (opcional)")
    async def bug_resolve(self, inter: discord.Interaction, bug_id: int, motivo: Optional[str] = None):
        res = await db.resolve_bug(inter.guild.id, bug_id, inter.user.id, motivo or "")
        if not res: return await inter.response.send_message("No encontré ese bug o ya estaba resuelto.", ephemeral=True)
        await inter.response.send_message(f"Bug #{bug_id} marcado como resuelto.", ephemeral=True)

tree.add_command(BugGroup())

# =================== Anti-Ping ===================
class AntiPingGroup(app_commands.Group):
    def __init__(self): super().__init__(name="antiping", description="Evita pings a usuarios protegidos.")
    @app_commands.command(name="add", description="Protege a un usuario (no le hagan ping).")
    @app_commands.default_permissions(manage_guild=True)
    async def add(self, inter: discord.Interaction, usuario: discord.User):
        await db.antiping_add(inter.guild.id, usuario.id)
        await inter.response.send_message(f"✅ Ahora **{usuario.mention}** está protegido.", ephemeral=True)
    @app_commands.command(name="remove", description="Quita protección de ping a un usuario.")
    @app_commands.default_permissions(manage_guild=True)
    async def remove(self, inter: discord.Interaction, usuario: discord.User):
        await db.antiping_remove(inter.guild.id, usuario.id)
        await inter.response.send_message(f"✅ Quitar protección: {usuario.mention}", ephemeral=True)
    @app_commands.command(name="list", description="Muestra los usuarios protegidos.")
    async def list(self, inter: discord.Interaction):
        ids = await db.antiping_list(inter.guild.id)
        if not ids: return await inter.response.send_message("No hay usuarios protegidos.", ephemeral=True)
        m = [f"<@{uid}> (`{uid}`)" for uid in ids]
        await inter.response.send_message(embed=brand_embed("🛡️ Anti-Ping protegidos", "\n".join(m), COLORS["info"]), ephemeral=True)
    @app_commands.command(name="settings", description="Configura umbral/timeout/ventana del anti-ping.")
    @app_commands.default_permissions(manage_guild=True)
    async def settings(self, inter: discord.Interaction, threshold: Optional[app_commands.Range[int,1,5]]=None,
                       timeout_minutes: Optional[app_commands.Range[int,1,4320]]=None, window_hours: Optional[app_commands.Range[int,1,168]]=None):
        if threshold is None and timeout_minutes is None and window_hours is None:
            s = await db.antiping_get_settings(inter.guild.id)
            desc = f"🔔 Umbral: **{s['threshold']}**\n🔇 Timeout: **{s['timeout_minutes']}m**\n⏱️ Ventana: **{s['window_hours']}h**"
            return await inter.response.send_message(embed=brand_embed("⚙️ Anti-Ping Settings (actual)", desc, COLORS["info"]), ephemeral=True)
        await db.antiping_set_settings(inter.guild.id, threshold, timeout_minutes, window_hours)
        s = await db.antiping_get_settings(inter.guild.id)
        await inter.response.send_message(embed=brand_embed("✅ Anti-Ping Settings actualizado", f"🔔 {s['threshold']} • 🔇 {s['timeout_minutes']}m • ⏱️ {s['window_hours']}h", COLORS["success"]), ephemeral=True)

tree.add_command(AntiPingGroup())

# ============== API HTTP (keys minimal) ==============
async def require_api_key(request):
    if not API_SECRET: return True
    return request.headers.get("x-api-key") == API_SECRET

async def api_ping(request): return web.json_response({"ok": True, "ts": int(time.time()*1000)})

async def api_checkkey(request):
    if not await require_api_key(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        body = await request.json()
        code = body.get("code",""); player_id = body.get("playerId")
        res = await db.check_key(code, player_id)
        return web.json_response(res if res["ok"] else {"ok": False, "reason": res.get("reason")})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)

async def api_genkey(request):
    if not await require_api_key(request):
        return web.json_response({"ok": False, "error": "unauthorized"}, status=401)
    try:
        body = await request.json()
        count = int(body.get("count",1)); mode = str(body.get("mode","random")).lower()
        prize = body.get("prizeName"); expires = body.get("expiresDays")
        res = await db.create_keys(count, mode, prize, created_by=None, expires_days=expires)
        if not res["ok"]:
            return web.json_response({"ok": False, "reason": res.get("reason")}, status=400)
        return web.json_response({"ok": True, "codes": res["codes"]})
    except Exception as e:
        return web.json_response({"ok": False, "error": str(e)}, status=400)

async def run_web():
    app = web.Application()
    app.add_routes([
        web.get("/api/ping", api_ping),
        web.post("/api/checkkey", api_checkkey),
        web.post("/api/genkey", api_genkey),
    ])
    runner = web.AppRunner(app); await runner.setup()
    port = int(os.getenv("PORT","8080")); site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start(); print(f"🌐 API escuchando en puerto {port}")

# ======= Helpers: timeout + tablas extra =======
async def timeout_member(member: discord.Member, minutes: int, reason: str):
    until = datetime.now(timezone.utc) + timedelta(minutes=minutes)
    try: await member.timeout(until=until, reason=reason)
    except AttributeError:
        try: await member.edit(timeout=until, reason=reason)
        except Exception: pass

# ---- Extras (economy/levels/infra/poll tables) ----
async def ensure_extra_tables():
    async with db.pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS economy_users(
          guild_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          balance BIGINT DEFAULT 0,
          last_daily TIMESTAMPTZ,
          last_work  TIMESTAMPTZ,
          PRIMARY KEY(guild_id, user_id)
        );""")
        await c.execute("""
        CREATE TABLE IF NOT EXISTS levels_users(
          guild_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          xp BIGINT DEFAULT 0,
          level INT DEFAULT 1,
          last_xp_at TIMESTAMPTZ,
          PRIMARY KEY(guild_id, user_id)
        );""")
        await c.execute("""
        CREATE TABLE IF NOT EXISTS infractions(
          id BIGSERIAL PRIMARY KEY,
          guild_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          moderator_id BIGINT NOT NULL,
          type TEXT NOT NULL,    -- 'warn'
          reason TEXT,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );""")
        await c.execute("""
        CREATE TABLE IF NOT EXISTS polls(
          message_id BIGINT PRIMARY KEY,
          guild_id BIGINT NOT NULL,
          channel_id BIGINT NOT NULL,
          question TEXT NOT NULL,
          options TEXT[] NOT NULL
        );""")

# ---- Economy helpers ----
async def econ_get(inter: discord.Interaction, uid: int) -> Dict[str,Any]:
    async with db.pool.acquire() as c:
        row = await c.fetchrow("SELECT * FROM economy_users WHERE guild_id=$1 AND user_id=$2", inter.guild.id, uid)
        if not row:
            await c.execute("INSERT INTO economy_users(guild_id,user_id) VALUES($1,$2)", inter.guild.id, uid)
            row = await c.fetchrow("SELECT * FROM economy_users WHERE guild_id=$1 AND user_id=$2", inter.guild.id, uid)
        return dict(row)

async def econ_add(inter: discord.Interaction, uid: int, amount: int):
    async with db.pool.acquire() as c:
        await c.execute("""
        INSERT INTO economy_users(guild_id,user_id,balance)
        VALUES($1,$2,$3)
        ON CONFLICT (guild_id,user_id) DO UPDATE
          SET balance = economy_users.balance + EXCLUDED.balance
        """, inter.guild.id, uid, int(amount))

# ---- Levels helpers ----
def level_xp_needed(level: int) -> int:
    return 100 + (level-1)*50

async def levels_gain_xp(guild_id: int, user_id: int, base_xp: int=10):
    async with db.pool.acquire() as c:
        row = await c.fetchrow("SELECT xp, level, last_xp_at FROM levels_users WHERE guild_id=$1 AND user_id=$2", guild_id, user_id)
        now = datetime.now(timezone.utc)
        if not row:
            xp = base_xp; lvl = 1
            await c.execute("INSERT INTO levels_users(guild_id,user_id,xp,level,last_xp_at) VALUES($1,$2,$3,$4,$5)",
                            guild_id, user_id, xp, lvl, now)
            return None
        last = row["last_xp_at"]
        if last and (now - last).total_seconds() < 60:
            await c.execute("UPDATE levels_users SET last_xp_at=$1 WHERE guild_id=$2 AND user_id=$3", now, guild_id, user_id)
            return None
        xp = int(row["xp"]) + base_xp
        lvl = int(row["level"])
        leveled = None
        while xp >= level_xp_needed(lvl):
            xp -= level_xp_needed(lvl)
            lvl += 1
            leveled = lvl
        await c.execute("UPDATE levels_users SET xp=$1, level=$2, last_xp_at=$3 WHERE guild_id=$4 AND user_id=$5",
                        xp, lvl, now, guild_id, user_id)
        return leveled

async def levels_get(inter: discord.Interaction, uid: int) -> Dict[str,Any]:
    async with db.pool.acquire() as c:
        row = await c.fetchrow("SELECT * FROM levels_users WHERE guild_id=$1 AND user_id=$2", inter.guild.id, uid)
        if not row:
            await c.execute("INSERT INTO levels_users(guild_id,user_id) VALUES($1,$2)", inter.guild.id, uid)
            row = await c.fetchrow("SELECT * FROM levels_users WHERE guild_id=$1 AND user_id=$2", inter.guild.id, uid)
        return dict(row)

# ---- Infractions helpers ----
async def warn_add(inter: discord.Interaction, uid: int, reason: str):
    async with db.pool.acquire() as c:
        await c.execute("INSERT INTO infractions(guild_id,user_id,moderator_id,type,reason) VALUES($1,$2,$3,'warn',$4)",
                        inter.guild.id, uid, inter.user.id, reason)

async def warns_list(inter: discord.Interaction, uid: int) -> List[Dict[str,Any]]:
    async with db.pool.acquire() as c:
        rows = await c.fetch("SELECT * FROM infractions WHERE guild_id=$1 AND user_id=$2 AND type='warn' ORDER BY id DESC", inter.guild.id, uid)
        return [dict(r) for r in rows]

async def warns_clear(inter: discord.Interaction, uid: int):
    async with db.pool.acquire() as c:
        await c.execute("DELETE FROM infractions WHERE guild_id=$1 AND user_id=$2 AND type='warn'", inter.guild.id, uid)

# ---- Encuestas helpers ----
class PollView(discord.ui.View):
    def __init__(self, mid: int, options: List[str]):
        super().__init__(timeout=None)
        self.message_id = mid
        for i,opt in enumerate(options[:5]):
            b = discord.ui.Button(label=opt[:80], style=discord.ButtonStyle.primary, custom_id=f"poll_{mid}_{i}")
            async def make_cb(inter: discord.Interaction, idx=i):
                await inter.response.send_message(f"✅ Voto recibido: **{options[idx]}**", ephemeral=True)
            b.callback = make_cb
            self.add_item(b)

# ===================== Economía =====================
@tree.command(name="balance", description="Muestra tu balance o el de otro usuario.")
async def balance_cmd(inter: discord.Interaction, usuario: Optional[discord.User]=None):
    if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    target = usuario or inter.user
    row = await econ_get(inter, target.id)
    await inter.response.send_message(embed=brand_embed("💰 Balance", f"{target.mention} tiene **{row['balance']}** monedas.", COLORS["panel"]), ephemeral=True)

@tree.command(name="daily", description="Reclama tu recompensa diaria.")
async def daily_cmd(inter: discord.Interaction):
    if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    row = await econ_get(inter, inter.user.id)
    now = datetime.now(timezone.utc)
    last = row.get("last_daily")
    if last and (now - last).total_seconds() < 86400:
        remain = 86400 - int((now-last).total_seconds())
        hrs = remain//3600; mins=(remain%3600)//60
        return await inter.response.send_message(embed=brand_embed("⏳ Daily aún no disponible", f"Vuelve en **{hrs}h {mins}m**.", COLORS["warn"]), ephemeral=True)
    amount = random.randint(100,200)
    async with db.pool.acquire() as c:
        await c.execute("""
        INSERT INTO economy_users(guild_id,user_id,balance,last_daily)
        VALUES($1,$2,$3,$4)
        ON CONFLICT (guild_id,user_id) DO UPDATE
          SET balance = economy_users.balance + EXCLUDED.balance,
              last_daily = EXCLUDED.last_daily
        """, inter.guild.id, inter.user.id, amount, now)
    await inter.response.send_message(embed=brand_embed("🎁 Daily", f"Has recibido **{amount}** monedas.", COLORS["success"]), ephemeral=True)

@tree.command(name="work", description="Trabaja para ganar monedas (cooldown 30m).")
async def work_cmd(inter: discord.Interaction):
    if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    row = await econ_get(inter, inter.user.id)
    now = datetime.now(timezone.utc)
    last = row.get("last_work")
    if last and (now - last).total_seconds() < 1800:
        remain = 1800 - int((now-last).total_seconds())
        mins = remain//60; secs=remain%60
        return await inter.response.send_message(embed=brand_embed("⏳ Aún en cooldown", f"Vuelve en **{mins}m {secs}s**.", COLORS["warn"]), ephemeral=True)
    amount = random.randint(40,90)
    async with db.pool.acquire() as c:
        await c.execute("""
        INSERT INTO economy_users(guild_id,user_id,balance,last_work)
        VALUES($1,$2,$3,$4)
        ON CONFLICT (guild_id,user_id) DO UPDATE
          SET balance = economy_users.balance + EXCLUDED.balance,
              last_work = EXCLUDED.last_work
        """, inter.guild.id, inter.user.id, amount, now)
    await inter.response.send_message(embed=brand_embed("🛠️ Work", f"Ganaste **{amount}** monedas.", COLORS["success"]), ephemeral=True)

@app_commands.default_permissions(manage_guild=True)
@tree.command(name="give", description="(Admin) Da monedas a un usuario.")
async def give_cmd(inter: discord.Interaction, usuario: discord.User, amount: app_commands.Range[int,1,100000]):
    if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    await econ_add(inter, usuario.id, int(amount))
    await inter.response.send_message(embed=brand_embed("💸 Transferencia", f"Se dieron **{amount}** a {usuario.mention}.", COLORS["success"]), ephemeral=True)

# ===================== Niveles =====================
@tree.command(name="level", description="Muestra tu nivel y XP.")
async def level_cmd(inter: discord.Interaction, usuario: Optional[discord.User]=None):
    if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    u = usuario or inter.user
    row = await levels_get(inter, u.id)
    need = level_xp_needed(row["level"])
    e = brand_embed("🆙 Nivel", f"{u.mention} — Nivel **{row['level']}**, XP **{row['xp']}**/**{need}**", COLORS["panel"])
    await inter.response.send_message(embed=e, ephemeral=True)

@tree.command(name="rank", description="Top de niveles en el servidor.")
async def rank_cmd(inter: discord.Interaction, top: app_commands.Range[int,1,20]=10):
    if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    async with db.pool.acquire() as c:
        rows = await c.fetch("""
        SELECT user_id, level, xp FROM levels_users
        WHERE guild_id=$1
        ORDER BY level DESC, xp DESC
        LIMIT $2
        """, inter.guild.id, int(top))
    if not rows:
        return await inter.response.send_message("Aún no hay datos.", ephemeral=False)
    body=[]
    for i,r in enumerate(rows, start=1):
        body.append(f"**{i}.** <@{r['user_id']}> — Nivel **{r['level']}**, XP **{r['xp']}**")
    await inter.response.send_message(embed=brand_embed("🏆 Ranking Niveles", "\n".join(body), COLORS["panel"]), ephemeral=False)

# ===================== Advertencias (Infractions) =====================
@app_commands.default_permissions(manage_messages=True)
@tree.command(name="warn", description="Advierte a un usuario.")
async def warn_cmd(inter: discord.Interaction, usuario: discord.User, razon: Optional[str]=""):
    if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    await warn_add(inter, usuario.id, razon or "—")
    e = brand_embed("⚠️ Advertencia", f"{usuario.mention} fue advertido.\n**Motivo:** {razon or '—'}", COLORS["warn"])
    await inter.response.send_message(embed=e, ephemeral=True)
    try:
        await usuario.send(embed=brand_embed("⚠️ Has sido advertido", f"Servidor: **{inter.guild.name}**\nMotivo: {razon or '—'}", COLORS["warn"]))
    except Exception: pass

@tree.command(name="infractions", description="Muestra advertencias de un usuario.")
async def infractions_cmd(inter: discord.Interaction, usuario: Optional[discord.User]=None):
    if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    u = usuario or inter.user
    rows = await warns_list(inter, u.id)
    if not rows:
        return await inter.response.send_message(embed=brand_embed("📄 Infractions", f"{u.mention} no tiene advertencias.", COLORS["info"]), ephemeral=True)
    lines=[]
    for r in rows[:15]:
        ts = r["created_at"].strftime("%Y-%m-%d %H:%M")
        lines.append(f"**#{r['id']}** — {ts} — por <@{r['moderator_id']}>")
        lines.append(f"> {r['reason'] or '—'}")
    await inter.response.send_message(embed=brand_embed(f"📄 Infractions — {u}", "\n".join(lines), COLORS["info"]), ephemeral=True)

@app_commands.default_permissions(manage_messages=True)
@tree.command(name="infractions_clear", description="Borra todas las advertencias de un usuario.")
async def infractions_clear_cmd(inter: discord.Interaction, usuario: discord.User):
    if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    await warns_clear(inter, usuario.id)
    await inter.response.send_message(embed=brand_embed("🧽 Limpieza", f"Se limpiaron las advertencias de {usuario.mention}.", COLORS["success"]), ephemeral=True)

# ===================== Encuestas =====================
@tree.command(name="poll", description="Crea una encuesta con botones (hasta 5 opciones).")
@app_commands.describe(pregunta="Pregunta", opcion1="Opción 1", opcion2="Opción 2", opcion3="Opción 3", opcion4="Opción 4", opcion5="Opción 5")
async def poll_cmd(inter: discord.Interaction, pregunta: str, opcion1: str, opcion2: str, opcion3: Optional[str]=None, opcion4: Optional[str]=None, opcion5: Optional[str]=None):
    if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
    options = [opcion1, opcion2] + [x for x in [opcion3, opcion4, opcion5] if x]
    if len(options) < 2:
        return await inter.response.send_message("Necesitas al menos 2 opciones.", ephemeral=True)
    e = brand_embed("🗳️ Encuesta", f"**{pregunta}**", COLORS["panel"])
    e.add_field(name="Opciones", value="\n".join([f"• {o}" for o in options]), inline=False)
    msg = await inter.channel.send(embed=e)
    async with db.pool.acquire() as c:
        await c.execute("INSERT INTO polls(message_id,guild_id,channel_id,question,options) VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
                        msg.id, inter.guild.id, inter.channel.id, pregunta, options)
    view = PollView(msg.id, options)
    try:
        await msg.edit(view=view)
    except Exception:
        pass
    await inter.response.send_message("✅ Encuesta publicada.", ephemeral=True)

# =================== on_message: XP + Bugs + Ticket logging + AntiPing ===================
@client.event
async def on_message(message: discord.Message):
    if message.author.bot or not message.guild: return

    # Anti-Ping
    try:
        protected = set(await db.antiping_list(message.guild.id))
        if protected and message.mentions:
            offenders_member = message.guild.get_member(message.author.id)
            if offenders_member and offenders_member.guild_permissions.administrator: pass
            else:
                hit = any(u.id in protected for u in message.mentions if not u.bot)
                if hit:
                    s = await db.antiping_get_settings(message.guild.id)
                    action = await db.antiping_record(message.guild.id, message.author.id, s["window_hours"], s["threshold"])
                    if action == "warn":
                        await message.reply(embed=brand_embed("🚫 Evita pings","Ese usuario **no desea ser etiquetado**. Si vuelves a hacerlo, serás sancionado.", COLORS["warn"]), mention_author=False)
                    else:
                        try:
                            await timeout_member(message.author, s["timeout_minutes"], "Anti-Ping: mencionó a protegido")
                            await message.reply(embed=brand_embed("🔇 Sanción aplicada", f"Has sido muteado **{s['timeout_minutes']}m**.", COLORS["error"]), mention_author=False)
                        except Exception: pass
    except Exception: pass

    # Ticket logging
    try:
        if isinstance(message.channel, discord.TextChannel):
            content = message.content if USE_MSG_INTENT else ""
            await db.log_ticket_message(message.channel.id, message.author.id, content, [a.url for a in message.attachments] if message.attachments else [])
    except Exception: pass

    # Bugs auto
    try:
        in_id, log_id = await get_bug_ids(message.guild)
        if message.channel.id == in_id:
            if not USE_MSG_INTENT:
                await message.channel.send("⚠️ Activa MESSAGE_CONTENT_INTENT para registrar texto de bugs."); return
            content = (message.content or "").strip()
            if not content: return
            settings = await db.get_bug_settings(message.guild.id)
            rate = await db.check_bug_rate(message.guild.id, message.author.id, settings["window_hours"])
            if rate == "warn":
                try: await message.delete()
                except Exception: pass
                await message.channel.send(content=message.author.mention, embed=brand_embed("⛔ Ya registraste un bug", f"No puedes volver a registrar dentro de **{settings['window_hours']}h**.", COLORS["warn"]), delete_after=10, allowed_mentions=discord.AllowedMentions(users=[message.author]))
                return
            elif rate == "mute":
                try: await timeout_member(message.author, settings["mute_minutes"], "Spam de bug reports")
                except Exception: pass
                try: await message.delete()
                except Exception: pass
                await message.channel.send(content=message.author.mention, embed=brand_embed("🔇 Mute por spam de bugs", f"Has sido muteado **{settings['mute_minutes']}m**.", COLORS["error"]), delete_after=15, allowed_mentions=discord.AllowedMentions(users=[message.author]))
                return
            bug = await db.add_bug_report(message.guild.id, message.author.id, message.channel.id, message.id, content)
            # Echo al usuario
            await message.reply(embed=brand_embed("🐞 Bug registrado", f"ID: **#{bug['id']}** — Gracias {message.author.mention}.", COLORS["success"]), mention_author=False)
            # Registro en canal de log
            log_ch = message.guild.get_channel(log_id)
            if isinstance(log_ch, discord.TextChannel):
                s = await db.get_bug_settings(message.guild.id)
                ping = None
                if s["ping_mode"] == "here": ping = "@here"
                elif s["ping_mode"] == "everyone": ping = "@everyone"
                elif s["ping_mode"] == "staff":
                    role = await resolve_staff_role(message.guild)
                    ping = role.mention if role else None
                e = brand_embed("🐞 Nuevo bug", f"**#{bug['id']}** por {message.author.mention}\nCanal: {message.channel.mention}\n\n> {content[:180]}{'…' if len(content)>180 else ''}", COLORS["warn"])
                reg = await log_ch.send(content=ping, embed=e, allowed_mentions=discord.AllowedMentions(everyone=True, roles=True))
                await db.set_bug_registry_message(bug["id"], log_ch.id, reg.id)
    except Exception: pass

    # XP por mensaje + anuncio de subida
    try:
        leveled = await levels_gain_xp(message.guild.id, message.author.id, base_xp=random.randint(8,14))
        if leveled:
            ch = message.guild.get_channel(LEVEL_UP_CHANNEL_ID)
            if isinstance(ch, discord.TextChannel):
                await ch.send(embed=brand_embed("🆙 ¡Subiste de nivel!", f"{message.author.mention} ahora es **Nivel {leveled}** 🎉", COLORS["success"]))
    except Exception:
        pass

# ======= Schedulers =======
async def ticket_watcher():
    await client.wait_until_ready()
    while not client.is_closed():
        try:
            open_ts = await db.list_open_tickets()
            nowts = time.time()
            for t in open_ts:
                last = t["last_activity"].timestamp()
                mins = int((nowts - last) / 60)
                try: user = await client.fetch_user(t["opener_id"])
                except Exception: user = None
                if mins >= (TICKET_INACTIVE_MIN - 30) and not t["warned_30"] and user:
                    try: await user.send(embed=brand_embed("⏳ Inactividad","Te quedan **30 minutos** para responder tu ticket.", COLORS["warn"])); await db.mark_warning(t["id"], 30)
                    except Exception: pass
                if mins >= (TICKET_INACTIVE_MIN - 10) and not t["warned_10"] and user:
                    try: await user.send(embed=brand_embed("⏳ Inactividad","Te quedan **10 minutos** para responder tu ticket.", COLORS["warn"])); await db.mark_warning(t["id"], 10)
                    except Exception: pass
                if mins >= TICKET_INACTIVE_MIN:
                    ch = client.get_channel(t["channel_id"])
                    if isinstance(ch, discord.TextChannel):
                        data = await db.close_ticket_by_channel(ch.id, closed_by=client.user.id, reason="Cierre automático por inactividad")
                        if data:
                            tinfo, messages = data
                            lines = []
                            for m in messages:
                                stamp = m["created_at"].strftime("%Y-%m-%d %H:%M:%S")
                                content = m["content"] or ""
                                atts = m["attachments"] or []
                                if atts: content += " " + " ".join(atts)
                                lines.append(f"[{stamp}] ({m['author_id']}): {content}")
                            txt = "\n".join(lines) if lines else "No hubo mensajes."
                            file_txt = discord.File(io.BytesIO(txt.encode()), filename=f"ticket_{tinfo['id']}_transcript.txt")
                            opener_name = await fetch_member_name(ch.guild, tinfo["opener_id"])
                            html_bytes = await render_transcript_html(ch.guild, tinfo, messages, opener_name, USE_MSG_INTENT)
                            file_html = discord.File(io.BytesIO(html_bytes), filename=f"ticket_{tinfo['id']}_transcript.html")
                            await ch.send(embed=brand_embed("🛑 Ticket cerrado por inactividad (3h)", color=COLORS["error"]), files=[file_txt, file_html])
                            await apply_closed_effects(ch, int(tinfo["opener_id"]))
                            try:
                                if user:
                                    await user.send(embed=brand_embed("Tu ticket fue cerrado por inactividad", color=COLORS["info"]))
                                    await user.send(files=[discord.File(io.BytesIO(txt.encode()), filename=f"ticket_{tinfo['id']}.txt"),
                                                           discord.File(io.BytesIO(html_bytes), filename=f"ticket_{tinfo['id']}.html")])
                            except Exception: pass
        except Exception: pass
        await asyncio.sleep(300)

# ======= GIVEAWAYS =======
class GiveawayEnterView(discord.ui.View):
    def __init__(self, giveaway_id: int):
        super().__init__(timeout=None)
        self.giveaway_id = giveaway_id
    @discord.ui.button(label="🎉 Participar", style=discord.ButtonStyle.success, custom_id="gw_enter")
    async def enter(self, inter: discord.Interaction, button: discord.ui.Button):
        ok = await db.giveaway_enter(self.giveaway_id, inter.user.id)
        if ok: await inter.response.send_message("✅ ¡Estás dentro!", ephemeral=True)
        else: await inter.response.send_message("⚠️ No fue posible entrar (quizá ya estabas o terminó).", ephemeral=True)

class GiveawayGroup(app_commands.Group):
    def __init__(self): super().__init__(name="giveaway", description="Sorteos (giveaways)")
    @app_commands.command(name="create", description="Crea un giveaway")
    @app_commands.default_permissions(manage_guild=True)
    @app_commands.describe(prize="Premio", duration_minutes="Duración en minutos", winners="Ganadores", channel="Canal destino", ping_role="Rol a pingear (opcional)")
    async def create(self, inter: discord.Interaction, prize: str, duration_minutes: app_commands.Range[int,1,10080], winners: app_commands.Range[int,1,20]=1, channel: Optional[discord.TextChannel]=None, ping_role: Optional[discord.Role]=None):
        if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
        ch = channel or (inter.channel if isinstance(inter.channel, discord.TextChannel) else None)
        if not ch: return await inter.response.send_message("Especifica un canal de texto.", ephemeral=True)
        ends_ts = int(time.time() + duration_minutes*60)
        gw = await db.giveaway_create(inter.guild.id, ch.id, prize, winners, ends_ts, inter.user.id, ping_role.id if ping_role else None)
        ends_at = datetime.fromtimestamp(ends_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
        e = brand_embed("🎉 GIVEAWAY", f"**Premio:** {prize}\n**Ganadores:** {winners}\n**Termina:** {ends_at}", COLORS["panel"])
        view = GiveawayEnterView(gw["id"])
        ping_text = ping_role.mention if ping_role else None
        msg = await ch.send(content=ping_text, embed=e, view=view, allowed_mentions=discord.AllowedMentions(roles=True))
        await db.giveaway_set_message(gw["id"], msg.id)
        await inter.response.send_message(f"✅ Giveaway creado en {ch.mention}.", ephemeral=True)
    @app_commands.command(name="end", description="Termina un giveaway ahora")
    @app_commands.default_permissions(manage_guild=True)
    async def end(self, inter: discord.Interaction, message_id: str):
        rows = await db.giveaway_list(inter.guild.id)
        target = None
        for r in rows:
            if str(r.get("message_id")) == str(message_id): target = r; break
        if not target: return await inter.response.send_message("No encontré el giveaway.", ephemeral=True)
        res = await db.giveaway_end(target["id"])
        if not res["ok"]: return await inter.response.send_message("No se pudo terminar.", ephemeral=True)
        await announce_giveaway_result(res)
        await inter.response.send_message("✅ Giveaway terminado.", ephemeral=True)
    @app_commands.command(name="reroll", description="Vuelve a sortear un giveaway terminado")
    @app_commands.default_permissions(manage_guild=True)
    async def reroll(self, inter: discord.Interaction, message_id: str):
        rows = await db.giveaway_list(inter.guild.id)
        target = None
        for r in rows:
            if str(r.get("message_id")) == str(message_id): target = r; break
        if not target: return await inter.response.send_message("No encontré el giveaway.", ephemeral=True)
        res = await db.giveaway_end(target["id"])
        if not res["ok"]: return await inter.response.send_message("No se pudo rerollear.", ephemeral=True)
        await announce_giveaway_result(res, reroll=True)
        await inter.response.send_message("✅ Reroll hecho.", ephemeral=True)
    @app_commands.command(name="list", description="Lista giveaways del servidor")
    async def list_(self, inter: discord.Interaction):
        rows = await db.giveaway_list(inter.guild.id)
        if not rows: return await inter.response.send_message("No hay giveaways.", ephemeral=True)
        body = []
        for r in rows[:15]:
            ends = r["ends_at"].strftime("%Y-%m-%d %H:%M UTC")
            body.append(f"ID: **{r['id']}** • Msg: `{r.get('message_id')}` • **{r['prize']}** • {r['winners']}w • {r['status']} • termina: {ends}")
        await inter.response.send_message(embed=brand_embed("🎉 Giveaways", "\n".join(body), COLORS["info"]), ephemeral=True)

async def announce_giveaway_result(res: Dict, reroll: bool=False):
    ch = client.get_channel(res["channel_id"])
    winners = res["winners"]
    if not winners:
        if isinstance(ch, discord.TextChannel):
            await ch.send(embed=brand_embed("🎉 Giveaway", "No hubo participantes válidos.", COLORS["warn"]))
        return
    mentions = " ".join(f"<@{u}>" for u in winners)
    title = "🔁 Reroll" if reroll else "🎉 Ganadores"
    e = brand_embed(title, f"{mentions}\n**Premio:** {res['prize']}", COLORS["success"])
    if isinstance(ch, discord.TextChannel):
        try:
            msg = await ch.fetch_message(res["message_id"])
            await msg.reply(embed=e)
        except Exception:
            await ch.send(embed=e)
    for uid in winners:
        try:
            u = await client.fetch_user(uid)
            await u.send(embed=brand_embed("🎉 ¡Ganaste!", f"Premio: **{res['prize']}**", COLORS["success"]))
        except Exception: pass

async def giveaway_watcher():
    await client.wait_until_ready()
    while not client.is_closed():
        try:
            due = await db.giveaway_list_due()
            for gw in due:
                res = await db.giveaway_end(gw["id"])
                if res.get("ok"):
                    await announce_giveaway_result(res)
        except Exception: pass
        await asyncio.sleep(20)

# ========================= TRIVIA =========================
class TriviaFetcher:
    def __init__(self): self.session: Optional[ClientSession] = None
    async def _session(self) -> ClientSession:
        if self.session and not self.session.closed: return self.session
        self.session = ClientSession(timeout=ClientTimeout(total=12)); return self.session
    async def close(self):
        try:
            if self.session and not self.session.closed: await self.session.close()
        except Exception: pass
        self.session = None
    async def fetch_triviaapi(self, category: Optional[str], difficulty: Optional[str]) -> Optional[Dict]:
        if not TRIVIA_USE_WEB: return None
        s = await self._session()
        params = {"limit": 1}
        if category and category.lower()!="any": params["categories"] = category
        if difficulty and difficulty.lower()!="any": params["difficulties"] = difficulty.lower()
        try:
            async with s.get("https://the-trivia-api.com/api/questions", params=params) as r:
                if r.status!=200: return None
                data = await r.json()
                if not data: return None
                d = data[0]
                q = str(d.get("question") or "").strip()
                correct = str(d.get("correctAnswer") or "")
                inc = [str(x) for x in d.get("incorrectAnswers") or []]
                opts = inc + [correct]; random.shuffle(opts); a = opts.index(correct)
                return {"q": q, "opts": opts, "a": a, "category": d.get("category") or "General", "difficulty": (d.get("difficulty") or "medium").lower(), "source": "triviaapi"}
        except Exception: return None
    async def fetch_opentdb(self, category: Optional[str], difficulty: Optional[str]) -> Optional[Dict]:
        if not TRIVIA_USE_WEB: return None
        s = await self._session(); params = {"amount":1,"type":"multiple","encode":"url3986"}
        if difficulty and difficulty.lower()!="any": params["difficulty"] = difficulty.lower()
        try:
            async with s.get("https://opentdb.com/api.php", params=params) as r:
                if r.status!=200: return None
                raw = await r.json()
                if not raw or raw.get("response_code")!=0 or not raw.get("results"): return None
                d = raw["results"][0]
                def dec(sv: str) -> str:
                    try: return html.unescape(unquote(sv))
                    except Exception:
                        try: return base64.b64decode(sv).decode("utf-8", errors="ignore")
                        except Exception: return sv
                q = dec(d.get("question","")); correct = dec(d.get("correct_answer","")); inc = [dec(x) for x in d.get("incorrect_answers") or []]
                opts = inc + [correct]; random.shuffle(opts); a = opts.index(correct)
                return {"q": q, "opts": opts, "a": a, "category": d.get("category") or "General", "difficulty": (d.get("difficulty") or "medium").lower(), "source": "opentdb"}
        except Exception: return None
    async def get_question(self, provider: str, category: Optional[str], difficulty: Optional[str]) -> Optional[Dict]:
        provider = (provider or "auto").lower()
        if provider=="triviaapi":
            q = await self.fetch_triviaapi(category, difficulty); 
            return q or await self.fetch_opentdb(category, difficulty)
        if provider=="opentdb":
            q = await self.fetch_opentdb(category, difficulty);
            return q or await self.fetch_triviaapi(category, difficulty)
        q = await self.fetch_triviaapi(category, difficulty)
        return q or await self.fetch_opentdb(category, difficulty)

TRIVIA_FETCHER = TriviaFetcher()
TRIVIA_BANK_LOCAL: List[Dict] = [
    {"q":"¿Qué lenguaje usa discord.py?", "opts":["Java","Python","Go","Ruby"], "a":1, "category":"Tech", "difficulty":"easy"},
    {"q":"¿Moneda de Roblox?", "opts":["Robux","Minecoins","V-Bucks","Gold"], "a":0, "category":"Gaming", "difficulty":"easy"},
    {"q":"¿Base de datos aquí?", "opts":["MongoDB","SQLite","Postgres","MySQL"], "a":2, "category":"DB", "difficulty":"easy"},
]

async def trivia_ensure_tables():
    async with db.pool.acquire() as c:
        await c.execute("""
        CREATE TABLE IF NOT EXISTS trivia_scores(
          guild_id BIGINT NOT NULL,
          user_id BIGINT NOT NULL,
          points INT DEFAULT 0,
          wins INT DEFAULT 0,
          last_played TIMESTAMPTZ DEFAULT NOW(),
          PRIMARY KEY(guild_id, user_id)
        );""")

async def trivia_add_points(guild_id: int, user_id: int, pts: int):
    async with db.pool.acquire() as c:
        await c.execute("""
        INSERT INTO trivia_scores(guild_id,user_id,points,wins,last_played)
        VALUES($1,$2,$3,0,NOW())
        ON CONFLICT (guild_id,user_id) DO UPDATE
          SET points = trivia_scores.points + EXCLUDED.points,
              last_played = NOW()
        """, guild_id, user_id, int(pts))

async def trivia_add_win(guild_id: int, user_id: int):
    async with db.pool.acquire() as c:
        await c.execute("""
        INSERT INTO trivia_scores(guild_id,user_id,points,wins,last_played)
        VALUES($1,$2,0,1,NOW())
        ON CONFLICT (guild_id,user_id) DO UPDATE
          SET wins = trivia_scores.wins + 1,
              last_played = NOW()
        """, guild_id, user_id)

async def trivia_top(guild_id: int, limit: int=10):
    async with db.pool.acquire() as c:
        rows = await c.fetch("""
        SELECT user_id, points, wins FROM trivia_scores
        WHERE guild_id=$1
        ORDER BY points DESC, wins DESC, last_played DESC
        LIMIT $2
        """, guild_id, int(limit))
        return [dict(r) for r in rows]

class TriviaAnswerView(discord.ui.View):
    def __init__(self, session: "TriviaSession", correct: int, timeout: int):
        super().__init__(timeout=timeout); self.session = session; self.correct = correct
        for i, lab in enumerate(["A","B","C","D"]):
            btn = discord.ui.Button(label=lab, style=discord.ButtonStyle.primary, custom_id=f"trv_{i}")
            async def make_cb(inter: discord.Interaction, idx=i):
                await self._route_answer(inter, idx)
            btn.callback = make_cb; self.add_item(btn)
    async def _route_answer(self, inter: discord.Interaction, idx: int):
        if not self.session.running or self.session.locked:
            return await inter.response.send_message("⏳ La ronda terminó.", ephemeral=True)
        if inter.user.id in self.session.answered:
            return await inter.response.send_message("Ya respondiste esta pregunta.", ephemeral=True)
        self.session.answered.add(inter.user.id)
        if idx == self.correct:
            pts = 10; pos = len(self.session.correct_order)
            if pos == 0: pts += 2
            elif pos == 1: pts += 1
            self.session.scores[inter.user.id] = self.session.scores.get(inter.user.id, 0) + pts
            self.session.correct_order.append(inter.user.id)
            await inter.response.send_message(embed=brand_embed("✅ ¡Correcto!", f"+{pts} puntos"), ephemeral=True)
        else:
            await inter.response.send_message(embed=brand_embed("❌ Incorrecto", "Suerte a la próxima.", COLORS["error"]), ephemeral=True)

class TriviaSession:
    def __init__(self, guild: discord.Guild, channel: discord.TextChannel, rounds: int, qtime: int, provider: str, category: Optional[str], difficulty: Optional[str]):
        self.guild = guild; self.channel = channel; self.rounds = rounds; self.qtime = qtime
        self.provider = provider; self.category = category; self.difficulty = difficulty
        self.running = False; self.locked = False; self.scores = {}; self.answered = set(); self.correct_order = []; self.msg=None
    async def _fetch_question(self) -> Dict:
        q = await TRIVIA_FETCHER.get_question(self.provider, self.category, self.difficulty)
        if q and q.get("q") and len(q.get("opts",[])) == 4: return q
        d = random.choice(TRIVIA_BANK_LOCAL); opts = d["opts"][:]; random.shuffle(opts); a = opts.index(d["opts"][d["a"]])
        return {"q": d["q"], "opts": opts, "a": a, "category": d.get("category","General"), "difficulty": d.get("difficulty","easy"), "source": "local"}
    async def run(self):
        self.running = True
        await self.channel.send(embed=brand_embed("🧠 Trivia iniciada", f"Rondas: **{self.rounds}** • Tiempo: **{self.qtime}s** • Fuente: **{(self.provider or 'auto').upper()}**", COLORS["panel"]))
        for idx in range(1, self.rounds+1):
            self.answered.clear(); self.correct_order.clear(); self.locked = False
            q = await self._fetch_question()
            e = brand_embed(f"❓ Pregunta {idx}/{self.rounds}", q["q"], COLORS["info"])
            e.add_field(name="Opciones", value="\n".join([f"**{chr(65+i)}.** {opt}" for i,opt in enumerate(q['opts'])]), inline=False)
            e.set_footer(text=f"🗂️ {q.get('category','General')} • 🧭 {q.get('difficulty','medium').title()} • 🌐 {q.get('source','web')}")
            view = TriviaAnswerView(self, q["a"], timeout=self.qtime)
            try: self.msg = await self.channel.send(embed=e, view=view)
            except Exception: self.msg=None; return
            await asyncio.sleep(self.qtime)
            self.locked = True
            try:
                if self.msg and view:
                    for it in view.children:
                        if isinstance(it, discord.ui.Button): it.disabled = True
                    await self.msg.edit(view=view)
            except Exception: pass
            correct_letter = chr(65 + q["a"])
            top = sorted(self.scores.items(), key=lambda kv: kv[1], reverse=True)[:5]
            lines = [f"**{i+1}.** <@{uid}> — **{pts}** pts" for i,(uid,pts) in enumerate(top)]
            sol = brand_embed("🧩 Solución", f"La respuesta correcta era **{correct_letter}**.\n\n**Top parcial:**\n" + ("\n".join(lines) if lines else "_Sin respuestas correctas aún_"), COLORS["success"])
            await self.channel.send(embed=sol); await asyncio.sleep(1)
        self.running = False
        if self.scores:
            maxp = max(self.scores.values()); winners = [uid for uid,p in self.scores.items() if p==maxp]
            win_mentions = ", ".join(f"<@{u}>" for u in winners)
            await self.channel.send(embed=brand_embed("🏁 Fin de la Trivia", f"Ganador(es): {win_mentions}\nPuntaje ganador: **{maxp}**", COLORS["success"]))
            for uid, pts in self.scores.items(): await trivia_add_points(self.guild.id, uid, pts)
            for uid in winners: await trivia_add_win(self.guild.id, uid)
        else:
            await self.channel.send(embed=brand_embed("🏁 Fin de la Trivia", "_No hubo respuestas correctas._", COLORS["warn"]))
        await TRIVIA_FETCHER.close()

TRIVIA_SESSIONS: Dict[int, TriviaSession] = {}

class TriviaGroup(app_commands.Group):
    def __init__(self): super().__init__(name="trivia", description="Juegos de trivia con ranking (online).")
    @app_commands.command(name="start", description="Inicia una trivia (obtiene preguntas de Internet).")
    @app_commands.describe(rounds="Rondas (1-10)", seconds_per_question="Segundos por pregunta (10-60)", provider="auto/triviaapi/opentdb", category="Categoría (texto TheTriviaAPI)", difficulty="any/easy/medium/hard")
    async def start(self, inter: discord.Interaction, rounds: app_commands.Range[int,1,10]=5, seconds_per_question: app_commands.Range[int,10,60]=20, provider: Optional[str]=None, category: Optional[str]=None, difficulty: Optional[str]="any"):
        if not inter.guild or not isinstance(inter.channel, discord.TextChannel): return await inter.response.send_message("Úsalo en un canal de texto del servidor.", ephemeral=True)
        if inter.channel.id in TRIVIA_SESSIONS and TRIVIA_SESSIONS[inter.channel.id].running:
            return await inter.response.send_message("Ya hay una trivia corriendo en este canal.", ephemeral=True)
        if not TRIVIA_USE_WEB: await inter.response.send_message("⚠️ `TRIVIA_USE_WEB=0`. Usaré preguntas locales.", ephemeral=True)
        prov = (provider or TRIVIA_PROVIDER_DEFAULT or "auto").lower().strip()
        diff = (difficulty or "any").lower().strip()
        await inter.response.defer(ephemeral=True, thinking=True)
        sess = TriviaSession(inter.guild, inter.channel, rounds, seconds_per_question, prov, category, diff)
        TRIVIA_SESSIONS[inter.channel.id] = sess; asyncio.create_task(sess.run())
        await inter.followup.send("✅ Trivia iniciada.", ephemeral=True)
    @app_commands.command(name="stop", description="Detiene la trivia en curso.")
    @app_commands.default_permissions(manage_messages=True)
    async def stop(self, inter: discord.Interaction):
        sess = TRIVIA_SESSIONS.get(inter.channel.id)
        if not sess or not sess.running: return await inter.response.send_message("No hay trivia corriendo aquí.", ephemeral=True)
        sess.running = False; sess.locked = True; await TRIVIA_FETCHER.close()
        await inter.response.send_message("⛔ Trivia detenida.", ephemeral=True)
    @app_commands.command(name="leaderboard", description="Muestra el ranking de trivia (servidor).")
    async def leaderboard(self, inter: discord.Interaction, top: app_commands.Range[int,1,20]=10):
        if not inter.guild: return await inter.response.send_message("Solo en servidores.", ephemeral=True)
        rows = await trivia_top(inter.guild.id, limit=top)
        if not rows: return await inter.response.send_message("Aún no hay puntuaciones.", ephemeral=True)
        body = [f"**{i+1}.** <@{r['user_id']}> — **{r['points']}** pts • 🏆 {r['wins']} wins" for i, r in enumerate(rows, start=1)]
        await inter.response.send_message(embed=brand_embed("🏆 Trivia Leaderboard", "\n".join(body), COLORS["panel"]), ephemeral=True)

tree.add_command(TriviaGroup())

# =================== GIVEAWAY + VIP COMMANDS ===================
tree.add_command(GiveawayGroup())

# =================== GOLD ACCOUNTS ===================
class GoldAccountGroup(app_commands.Group):
    def __init__(self): super().__init__(name="goldaccount", description="Gestión de cuentas Gold (solo admins).")

    @app_commands.command(name="add", description="Guarda una cuenta Gold (solo admins).")
    @app_commands.default_permissions(manage_guild=True)
    async def add(self, inter: discord.Interaction, nombre: str, ugphone: str):
        if not inter.user.guild_permissions.manage_guild:
            return await inter.response.send_message("Requiere **Manage Server**.", ephemeral=True)
        res = await db.gold_add(inter.guild.id, inter.user.id, nombre, ugphone)
        if not res["ok"]:
            if res.get("reason") == "duplicate":
                return await inter.response.send_message("Ya tienes registrada una cuenta con ese nombre.", ephemeral=True)
            return await inter.response.send_message("No pude guardar. Revisa datos.", ephemeral=True)
        row = res["row"]
        e = brand_embed("✨ Gold Account guardada", f"**{row['account_name']}** • UGPhone: `{row['ugphone']}`", COLORS["success"])
        await inter.response.send_message(embed=e, ephemeral=True)

    @app_commands.command(name="list", description="Lista tus Gold Accounts guardadas (solo admins).")
    @app_commands.default_permissions(manage_guild=True)
    async def list_(self, inter: discord.Interaction):
        rows = await db.gold_list(inter.guild.id, inter.user.id)
        if not rows: return await inter.response.send_message("No tienes cuentas guardadas.", ephemeral=True)
        body = []
        for r in rows[:50]:
            body.append(f"• **{r['account_name']}** — UGPhone: `{r['ugphone']}` — {r['created_at'].strftime('%Y-%m-%d')}")
        await inter.response.send_message(embed=brand_embed("✨ Tus Gold Accounts", "\n".join(body), COLORS["panel"]), ephemeral=True)

    @app_commands.command(name="remove", description="Elimina una Gold Account por nombre (solo admins).")
    @app_commands.default_permissions(manage_guild=True)
    async def remove(self, inter: discord.Interaction, nombre: str):
        ok = await db.gold_remove(inter.guild.id, inter.user.id, nombre)
        if ok: await inter.response.send_message("🗑️ Eliminada.", ephemeral=True)
        else: await inter.response.send_message("No encontrada.", ephemeral=True)

tree.add_command(GoldAccountGroup())

# ----------------- on_ready -----------------
@client.event
async def on_ready():
    try:
        client.add_view(TicketPanel(timeout=None))
        client.add_view(ControlsView(timeout=None))
        # Giveaway button persistente (aunque el ID se asigna por mensaje concreto, mantener la clase registrada)
        client.add_view(GiveawayEnterView(giveaway_id=0))  # dummy para registrar la vista
    except Exception: pass
    try:
        await tree.sync(); print(f"✅ Conectado como {client.user} — slash commands sync. MESSAGE_CONTENT_INTENT={USE_MSG_INTENT}")
    except Exception as e:
        print("Error al sincronizar comandos:", e)
    try: await trivia_ensure_tables()
    except Exception as e: print("Trivia tables error:", e)
    try: await ensure_extra_tables()
    except Exception as e: print("Extra tables error:", e)
    # Aviso bugs pegajoso (si falta)
    try:
        for g in client.guilds:
            in_id, _ = await get_bug_ids(g)
            if not in_id: continue
            ch = g.get_channel(in_id)
            if not ch or not isinstance(ch, discord.TextChannel): continue
            bn = await db.get_bug_notice(g.id); msg_id = bn.get("bug_notice_message_id"); ok=False
            if msg_id:
                try: await ch.fetch_message(int(msg_id)); ok=True
                except discord.NotFound: ok=False
            if not ok:
                m = await ch.send(embed=brand_embed("📢 Reglas del canal de bugs", BUG_NOTICE_TEXT, COLORS["error"]))
                try: await m.pin()
                except Exception: pass
                await db.set_bug_notice(g.id, ch.id, m.id)
    except Exception: pass

# ----------------- main entry -----------------
async def main_async():
    global db
    if not TOKEN: raise SystemExit("❌ Falta DISCORD_TOKEN")
    db = Database(DATABASE_URL); await db.connect(); await db.init()
    try:
        await asyncio.gather(run_web(), client.start(TOKEN), ticket_watcher(), giveaway_watcher())
    finally:
        await db.close(); await TRIVIA_FETCHER.close()

if __name__ == "__main__":
    asyncio.run(main_async())
