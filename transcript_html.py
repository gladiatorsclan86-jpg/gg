from typing import List, Dict
import html, re

CSS = """
<!DOCTYPE html>
<html lang="es"><head><meta charset="utf-8">
<title>Transcripción de Ticket</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  :root {
    --bg:#0b1220; --panel:#0f172a; --card:#0f172a; --text:#e5e7eb; --muted:#9ca3af;
    --accent:#60a5fa; --success:#22c55e; --danger:#ef4444; --warn:#f59e0b; --bubble:#111827;
    --staff:#6366f1; --opener:#0ea5e9;
  }
  html,body{background:var(--bg);color:var(--text);font:15px/1.5 ui-sans-serif,system-ui,-apple-system,Segoe UI,Roboto;margin:0}
  .container{max-width:980px;margin:28px auto;padding:0 16px}
  .header{background:linear-gradient(135deg,#0ea5e9, #6366f1);border-radius:16px;padding:20px 24px;color:white;box-shadow:0 10px 30px rgba(0,0,0,.25)}
  .header h1{margin:0 0 2px;font-size:22px}
  .pill{display:inline-block;padding:4px 10px;border-radius:999px;background:rgba(255,255,255,.15);margin:6px 8px 0 0;font-size:13px}
  .note{margin:10px 0;color:#eef}
  .day{margin:24px 0 12px;color:var(--muted);text-transform:uppercase;font-size:12px;letter-spacing:.08em}
  .msg{display:flex;gap:12px;padding:12px 0;border-bottom:1px solid rgba(255,255,255,.05)}
  .avatar{width:36px;height:36px;border-radius:50%;background:#222;flex:0 0 36px;overflow:hidden}
  .avatar img{width:36px;height:36px;display:block}
  .bubble{background:var(--bubble);border:1px solid rgba(255,255,255,.06);border-left:4px solid transparent;border-radius:12px;padding:10px 12px;flex:1}
  .bubble.staff{border-left-color:var(--staff)}
  .bubble.opener{border-left-color:var(--opener)}
  .head{display:flex;gap:8px;align-items:center;margin-bottom:4px}
  .name{font-weight:600}
  .badge{font-size:12px;padding:2px 8px;border-radius:999px;background:rgba(255,255,255,.08);color:#fff}
  .badge.staff{background:linear-gradient(135deg,#6366f1,#22c55e)}
  .badge.opener{background:linear-gradient(135deg,#0ea5e9,#60a5fa)}
  .time{color:var(--muted);font-size:12px;margin-left:auto}
  .content{white-space:pre-wrap;word-wrap:break-word;color:#f1f5f9}
  .atts{margin-top:6px;font-size:13px}
  .atts a{color:var(--accent);text-decoration:none}
  .footer{margin:28px 0;color:var(--muted);font-size:12px;text-align:center}
</style></head><body><div class="container">
"""

FOOT = """
<div class="footer">Generado por GGHud Tickets • Si falta texto, activa “Message Content Intent” en tu bot.</div>
</div></body></html>
"""

def esc(s:str)->str:
    return html.escape(str(s or ""))

def linkify(text: str) -> str:
    url_re = re.compile(r"(https?://[^\s]+)")
    return url_re.sub(r'<a href="\1" target="_blank">\1</a>', esc(text))

async def render_transcript_html(guild, tinfo: Dict, messages: List[Dict], opener_name: str, use_msg_intent: bool) -> bytes:
    gname = getattr(guild, "name", "Servidor")
    title = f"Ticket #{tinfo['id']} — {gname}"
    kind = tinfo.get("kind","?")
    claimed_by = tinfo.get("claimed_by")
    opener = opener_name

    parts = [CSS]
    parts.append(f'<div class="header"><h1>{esc(title)}</h1>')
    parts.append(f'<span class="pill">Tipo: {esc(kind)}</span>')
    parts.append(f'<span class="pill">Abierto por: {esc(opener)}</span>')
    if claimed_by:
        sname = str(claimed_by)
        try:
            staff = guild.get_member(int(claimed_by))
            if staff:
                sname = staff.display_name
        except Exception:
            pass
        parts.append(f'<span class="pill">Reclamado por: {esc(sname)}</span>')
    created_at = tinfo.get("created_at")
    closed_at = tinfo.get("closed_at") or created_at
    try:
        c1 = created_at.strftime("%Y-%m-%d %H:%M")
        c2 = closed_at.strftime("%Y-%m-%d %H:%M")
    except Exception:
        c1 = c2 = "—"
    parts.append(f'<div class="note">Período: {esc(c1)} → {esc(c2)}</div>')
    if not use_msg_intent:
        parts.append('<div class="note" style="color:#ffb4b4">⚠️ El bot no tenía Message Content Intent activo: los textos pueden aparecer vacíos.</div>')
    parts.append('</div>')

    current_day = None
    for m in messages:
        ts = m.get("created_at")
        day_key = ""
        stamp = ""
        try:
            day_key = ts.strftime("%Y-%m-%d")
            stamp = ts.strftime("%Y-%m-%d %H:%M:%S")
        except Exception:
            pass
        if day_key and day_key != current_day:
            current_day = day_key
            parts.append(f'<div class="day">{esc(current_day)}</div>')

        uid = int(m.get("author_id", 0))
        member = guild.get_member(uid) if guild else None
        name = (member.display_name if member else str(uid))
        avatar = ""
        try:
            if member:
                avatar = member.display_avatar.url
        except Exception:
            avatar = ""

        badges = []
        bubble_cls = ""
        try:
            if member and member.guild_permissions.manage_messages:
                badges.append('<span class="badge staff">Staff</span>')
                bubble_cls = " staff"
        except Exception:
            pass
        if uid == int(tinfo.get("opener_id", -1)):
            badges.append('<span class="badge opener">Autor</span>')
            bubble_cls = (bubble_cls + " opener").strip()

        content = m.get("content","")
        content_html = linkify(content)
        atts = m.get("attachments") or []
        att_html = ""
        if atts:
            links = " • ".join(f'<a href="{esc(url)}" target="_blank">{esc(url)}</a>' for url in atts if url)
            att_html = f'<div class="atts">Adjuntos: {links}</div>'

        parts.append('<div class="msg">')
        if avatar:
            parts.append(f'<div class="avatar"><img src="{esc(avatar)}" alt="avatar"></div>')
        else:
            parts.append(f'<div class="avatar"></div>')
        parts.append(f'<div class="bubble{ " " + bubble_cls if bubble_cls else ""}">')
        parts.append(f'<div class="head"><span class="name">{esc(name)}</span>{" ".join(badges)}<span class="time">{esc(stamp)}</span></div>')
        parts.append(f'<div class="content">{content_html}</div>')
        if att_html:
            parts.append(att_html)
        parts.append('</div></div>')

    parts.append(FOOT)
    return "".join(parts).encode("utf-8")
