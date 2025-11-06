import os, redis, time
from flask import Flask, jsonify, render_template_string, request
import statistics

app = Flask(__name__)

REDIS_URL = os.getenv("REDIS_URL", os.getenv("REDIS_PUBLIC_URL", "redis://localhost:6379/0"))
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
def _key(job_id): return f"job:{job_id}"

def _sector_samples_key(sector): 
    return f"stats:sector:{sector}:samples"

MIN_SAMPLES_FOR_MEDIAN = 1  # mismo umbral que en progress.py

# ==== NUEVO: claves y parámetros para ETA multi-sector ====
def _job_pending_key(job_id): 
    return f"job:{job_id}:pending_by_sector"   # HASH: sector -> pendientes
def _job_eta_key(job_id):
    return f"job:{job_id}:eta"                 # HASH: prev_eta, prev_ts, eta_display

DEFAULT_RATE_SEC_PER_UNIT = int(os.getenv("DEFAULT_RATE_SEC_PER_UNIT", "45"))  # fallback conservador
SAMPLES_CAP = int(os.getenv("SAMPLES_CAP", "200"))  # por si quieres alinear con progress.py
ALPHA = float(os.getenv("ETA_SMOOTHING_ALPHA", "0.30"))  # suavizado exponencial

def eta_sum_by_sector_from_redis(job_id: str):
    """
    Suma por sector: Σ (pendientes_sector * mediana_histórica_sector),
    usando DEFAULT_RATE_SEC_PER_UNIT si falta histórico.
    Devuelve (raw_eta_seconds, dict_by_sector)
    """
    hk = _job_pending_key(job_id)
    items = r.hgetall(hk) or {}
    total = 0.0
    for sector, n_left in items.items():
        try:
            n = max(int(n_left), 0)
        except Exception:
            n = 0
        if n == 0:
            continue
        med, _n = get_sector_median(sector)
        rate = float(med) if med is not None else float(DEFAULT_RATE_SEC_PER_UNIT)
        total += n * max(rate, 0.001)
    return float(total), items

def monotonic_smoothed_eta(job_id: str, raw_eta_seconds: float):
    """
    Aplica clamp NO-creciente + suavizado exponencial al ETA mostrado.
    Permite subidas solo si realmente crece raw_eta (p.ej., aumentan pendientes).
    """
    now = int(time.time())
    ek = _job_eta_key(job_id)
    prev = r.hgetall(ek) or {}
    prev_eta = float(prev.get("prev_eta", raw_eta_seconds))
    prev_ts  = float(prev.get("prev_ts", now))
    disp_prev = float(prev.get("eta_display", raw_eta_seconds))

    elapsed = max(now - prev_ts, 0.0)
    monotonic = max(float(raw_eta_seconds), prev_eta - elapsed)  # no crece "gratis"
    eta_display = ALPHA * monotonic + (1.0 - ALPHA) * disp_prev

    r.hset(ek, mapping={
        "prev_eta": monotonic,
        "prev_ts": now,
        "eta_display": eta_display
    })
    return float(eta_display)
# ==== FIN NUEVO ====

def get_sector_median(sector):
    """
    Lee la LIST stats:sector:<sector>:samples y devuelve (mediana, n_muestras),
    o (None, 0) si aún no hay suficientes datos.
    """
    vals = r.lrange(_sector_samples_key(sector), 0, -1)
    if not vals or len(vals) < MIN_SAMPLES_FOR_MEDIAN:
        return None, len(vals or [])
    nums = list(map(int, vals))
    return int(statistics.median(nums)), len(nums)

def get_units(job_id):
    """Lee total y hechas del job en Redis."""
    d = r.hgetall(_key(job_id)) or {}
    tot  = int(d.get("total_units", "0") or 0)
    done = int(d.get("done_units", "0") or 0)
    return tot, done

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

    # ---------- NUEVO: ETA multi-sector con clamp + suavizado ----------
    eta_seconds = None
    try:
        if state in ("running", "queued"):
            # 1) Intentar suma por sector (requiere que el worker haya escrito job:{job}:pending_by_sector)
            raw_eta, by_sector = eta_sum_by_sector_from_redis(job_id)
    
            if by_sector:  # tenemos desglose por sector -> cálculo robusto
                eta_seconds = monotonic_smoothed_eta(job_id, raw_eta)
            else:
                # 2) Fallback (legacy): si aún no existe pending_by_sector, usar unidades restantes × prior
                try:
                    tot, done = get_units(job_id)
                except Exception:
                    d = r.hgetall(_key(job_id)) or {}
                    tot = int(d.get("total_units", "0") or 0)
                    done = int(d.get("done_units", "0") or 0)
    
                remaining = max(0, tot - done)
    
                # Si existe sector actual y su mediana, úsala; si no, prior global
                data_for_job = r.hgetall(_key(job_id)) or {}
                cur_sector = data_for_job.get("current_sector")
                sector_med = None
                if cur_sector:
                    try:
                        sector_med, _n = get_sector_median(cur_sector)
                    except Exception:
                        sector_med = None
    
                per_unit = (sector_med or DEFAULT_RATE_SEC_PER_UNIT)
                raw_eta = remaining * per_unit
    
                # Si no hay unidades pero hay progreso, usa regla por pct (clamp)
                now_sec = int(time.time())
                started_at = int(data_for_job.get("started_at", "0") or 0)
                pct_val = int(float(data_for_job.get("pct", 0)))
                if tot == 0 and done == 0 and started_at > 0 and pct_val > 0:
                    elapsed = max(1, now_sec - started_at)
                    raw_eta = min(3*3600, int(elapsed * (100 - pct_val) / max(1, pct_val)))
    
                eta_seconds = monotonic_smoothed_eta(job_id, float(raw_eta))
    except Exception:
        eta_seconds = None
    # ---------- FIN NUEVO ----------

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

@app.get("/api/sector-times")
def api_sector_times():
    """
    Devuelve todos los sectores y sus tiempos registrados (samples en segundos).
    Es completamente público (sin token).
    Ejemplo:
      https://progress.tessextractor.app/api/sector-times
    """
    # Buscar todos los sectores con samples guardados
    sectors = set()
    for key in r.scan_iter("stats:sector:*:samples"):
        parts = key.split(":")
        if len(parts) >= 3:
            sectors.add(parts[2])
    sectors = sorted(sectors, key=lambda s: (not s.isdigit(), int(s) if s.isdigit() else s))

    out = []
    for s in sectors:
        vals = r.lrange(f"stats:sector:{s}:samples", 0, -1)
        times = [int(v) for v in vals]
        median = None
        try:
            import statistics
            if times:
                median = int(statistics.median(times))
        except Exception:
            pass
        out.append({
            "sector": s,
            "n_samples": len(times),
            "median_sec_per_unit": median,
            "samples_sec": times
        })

    return jsonify({"sectors": out})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
