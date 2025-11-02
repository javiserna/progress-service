import os, redis
from flask import Flask, jsonify, render_template_string, request

app = Flask(__name__)

# Usa la URL pública de tu Redis de Railway
REDIS_URL = os.getenv("REDIS_URL", os.getenv("REDIS_PUBLIC_URL", "redis://localhost:6379/0"))
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

def _key(job_id): return f"job:{job_id}"

HTML = """
<!doctype html><meta charset="utf-8">
<title>Progreso {{job_id}}</title>
<h2 style="font-family:system-ui;margin:0 0 8px;">Job {{job_id}}</h2>
<div id="pct" style="font-size:2.2rem;font-family:system-ui;">0%</div>
<div id="msg" style="margin-top:6px;color:#555;font-family:system-ui;"></div>
<script>
async function tick(){
  const res = await fetch("/status/{{job_id}}?t={{token}}");
  if (!res.ok) return;
  const j = await res.json();
  document.getElementById('pct').textContent = (j.pct ?? 0) + "%";
  document.getElementById('msg').textContent = j.estado + (j.msg ? (" — " + j.msg) : "");
  if (j.estado === "done" || j.estado === "error"){ clearInterval(iv); }
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
    return jsonify({
        "job_id": job_id,
        "pct": int(float(data.get("pct", 0))),
        "estado": data.get("estado","unknown"),
        "msg": data.get("msg","")
    })

@app.get("/progress/<job_id>")
def progress_page(job_id):
    token = request.args.get("t","")
    return render_template_string(HTML, job_id=job_id, token=token)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
