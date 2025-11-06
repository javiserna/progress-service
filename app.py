import os, redis, time
from flask import Flask, jsonify, render_template_string, request
from progress import get_sector_median

app = Flask(__name__)

REDIS_URL = os.getenv("REDIS_URL", os.getenv("REDIS_PUBLIC_URL", "redis://localhost:6379/0"))
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
def _key(job_id): return f"job:{job_id}"

HTML = """
<!doctype html><meta charset="utf-8">
<title>Progress {{job_id}}</title>
<style>
  body{font-family:system-ui,-apple-system,Segoe UI,Roboto,Helvetica,Arial,sans-serif; margin:24px; color:#111;}
  h2{margin:0 0 12px;}
  .pct{font-size:3rem; font-weight:700; margin:8px 0;}
  .bar{width:100%; height:14px; background:#eee; border-radius:999px; overflow:hidden;}
  .fill{height:100%; width:0%; background:#0a7; transition:width .6s;}
  .row{margin-top:10px; color:#555;}
  .badge{display:inline-block; padding:2px 8px; border-radius:999px; background:#eef; margin-right:8px; font-size:.9rem;}
  .ok{background:#e6f8ef;}
  .err{background:#fdecec;}
  .muted{color:#666;}
</style>
<h2>Job {{job_id}}</h2>
<div class="pct" id="pct">0%</div>
<div class="bar"><div class="fill" id="fill"></div></div>
<div class="row">
  <span class="badge" id="state">queued</span>
  <span id="msg" class="muted"></span>
</div>
<div class="row" id="etaRow" style="display:none;">
  ⏱ <span id="etaText" class="muted"></span>
</div>

<script>
function fmtSeconds(s){
  if (s == null || isNaN(s) || s < 0) return "";
  const m = Math.floor(s/60), sec = Math.round(s%60);
  if (m >= 60) {
    const h = Math.floor(m/60), mm = m%60;
    return h + "h " + (mm>0? mm+"m ":"") + (sec>0? sec+"s":"");
  }
  return (m>0? m+"m ":"") + (sec>0? sec+"s":"");
}

async function tick(){
  const res = await fetch("/status/{{job_id}}?t={{token}}");
  if (!res.ok) return;
  const j = await res.json();

  const pct = (j.pct ?? 0);
  document.getElementById('pct').textContent = pct + "%";
  document.getElementById('fill').style.width = pct + "%";

  const st = j.state || j.estado || "unknown"; // fallback
  const msg = j.msg || "";
  const stEl = document.getElementById('state');
  stEl.textContent = st;
  stEl.className = "badge " + (st==="done"?"ok":(st==="error"?"err":""));

  document.getElementById('msg').textContent = msg;

  // ETA (servidor pre-calcula j.eta_seconds si puede)
  const etaRow = document.getElementById('etaRow');
  const etaText = document.getElementById('etaText');
  if (j.eta_seconds != null && (st==="running" || st==="queued")){
    etaRow.style.display = "block";
    etaText.textContent = (st==="queued" ? "Estimated queue wait: " : "Estimated time remaining: ") + fmtSeconds(j.eta_seconds);
  } else {
    etaRow.style.display = "none";
    etaText.textContent = "";
  }

  if (st === "done" || st === "error"){ clearInterval(iv); }
}
const iv = setInterval(tick, 1500);
tick();
</script>
"""

@app.get("/status/<job_id>")
def status(job_id):
    token = request.args.get("t")
    saved_token = r.hget(_key(job_id), "token")
    if saved_token and token != saved_token:
        return jsonify({"error":"unauthorized"}), 401
    data = r.hgetall(_key(job_id)) or {}

    # Map fields + compute ETA
    pct = int(float(data.get("pct", 0)))
    state = data.get("state") or data.get("estado") or "unknown"
    msg = data.get("msg", "")
    now = int(time.time())

    # ---------- ETA con mediana por sector (estable) + fallback ----------
    eta_seconds = None
    try:
        if state == "running":
            # 1) Intento: usar mediana histórica por sector
            sector = data.get("current_sector")
            median_sec = None
            if sector:
                try:
                    median_sec, _n = get_sector_median(sector)  # devuelve (mediana, n_muestras) o (None, 0)
                except Exception:
                    median_sec = None

            if median_sec is not None and pct > 0:
                # ETA estable con mediana por sector
                eta_seconds = int(median_sec * (100 - pct) / pct)
            else:
                # 2) Fallback: ETA clásico por elapsed
                started_at = int(data.get("started_at", "0") or "0")
                if started_at > 0 and pct > 0:
                    elapsed = max(1, now - started_at)
                    eta_seconds = int(elapsed * (100 - pct) / pct)

        elif state == "queued":
            # Si mantienes una cola y un promedio por job, úsalo aquí
            avg_job_seconds = int(data.get("avg_job_seconds", "0") or "0")
            queue_pos = r.zrank("queue", job_id)  # requiere que mantengas ZADD/ZREM en tu worker
            if avg_job_seconds and queue_pos is not None:
                eta_seconds = int(avg_job_seconds * (queue_pos + 1))
    except Exception:
        eta_seconds = None

    return jsonify({
        "job_id": job_id,
        "pct": pct,
        "state": state,
        "msg": msg,
        "eta_seconds": eta_seconds
    })

@app.get("/progress/<job_id>")
def progress_page(job_id):
    token = request.args.get("t","")
    return render_template_string(HTML, job_id=job_id, token=token)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
