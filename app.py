#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
import json
import uuid
import time
import threading
import queue as queue_module
import logging
from logging.handlers import RotatingFileHandler
from datetime import datetime
from pathlib import Path

# Try to import optional dependencies gracefully
try:
    import yt_dlp
except ImportError:
    print("ERROR: yt-dlp is not installed. Run: pip install yt-dlp")
    sys.exit(1)

from flask import Flask, request, jsonify, render_template_string, send_from_directory
from flask_socketio import SocketIO, emit

# Optional imports with fallbacks
try:
    import paramiko
    PARAMIKO_AVAILABLE = True
except ImportError:
    PARAMIKO_AVAILABLE = False
    print("WARNING: paramiko not installed. SSH upload will be disabled.")

try:
    import psutil
    PSUTIL_AVAILABLE = True
except ImportError:
    PSUTIL_AVAILABLE = False
    print("WARNING: psutil not installed. System stats will show N/A.")

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    print("WARNING: python-dotenv not installed. Using environment variables directly.")

# ============================================================================
#                               CONFIGURATION
# ============================================================================

BASE_DIR = Path(__file__).parent.absolute()
STATIC_DIR = BASE_DIR / "static"
DOWNLOADS_DIR = BASE_DIR / "downloads"
LOGS_DIR = BASE_DIR / "logs"

for d in [STATIC_DIR, DOWNLOADS_DIR, LOGS_DIR]:
    d.mkdir(exist_ok=True)

# SSH Configuration (from environment)
SSH_HOST = os.getenv("SSH_HOST", "185.208.175.180")
SSH_USER = os.getenv("SSH_USER", "root")
SSH_PASSWORD = os.getenv("SSH_PASSWORD", "Amir.1388")
SSH_PORT = int(os.getenv("SSH_PORT", 22))
REMOTE_UPLOAD_PATH = os.getenv("REMOTE_UPLOAD_PATH", "/root/videos/").rstrip('/') + '/'

# Flask & SocketIO
app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "vplayer-super-secret-key")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ============================================================================
#                               LOGGING
# ============================================================================

def setup_logging():
    log_format = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler = RotatingFileHandler(LOGS_DIR / "vplayer.log", maxBytes=10_485_760, backupCount=5)
    handler.setFormatter(log_format)
    console = logging.StreamHandler()
    console.setFormatter(log_format)
    logger = logging.getLogger("vplayer")
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.addHandler(console)
    return logger

logger = setup_logging()

# ============================================================================
#                               QUEUE & STATE
# ============================================================================

queue_items = []          # list of dicts
queue_lock = threading.Lock()
active_download = None
stop_current = threading.Event()
queue_paused = False
queue_worker_thread = None
should_exit = False
QUEUE_STATE_FILE = BASE_DIR / "queue_state.json"

def save_queue_state():
    with queue_lock:
        to_save = []
        for item in queue_items:
            item_copy = {k: v for k, v in item.items() if k not in ["progress_hook", "thread"]}
            to_save.append(item_copy)
        data = {"items": to_save, "paused": queue_paused}
    try:
        with open(QUEUE_STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        logger.error(f"Failed to save queue: {e}")

def load_queue_state():
    global queue_paused
    if QUEUE_STATE_FILE.exists():
        try:
            with open(QUEUE_STATE_FILE, "r") as f:
                data = json.load(f)
            with queue_lock:
                queue_items.clear()
                for it in data.get("items", []):
                    it["progress"] = 0
                    it["speed"] = 0
                    it["eta"] = ""
                    it["upload_status"] = None
                    it["error"] = None
                    it["thread"] = None
                    if it["status"] in ("downloading", "uploading"):
                        it["status"] = "waiting"
                    queue_items.append(it)
                queue_paused = data.get("paused", False)
            logger.info(f"Loaded {len(queue_items)} items from queue state.")
        except Exception as e:
            logger.error(f"Failed to load queue state: {e}")

# ============================================================================
#                               SSH UPLOADER (with fallback)
# ============================================================================

class SSHUploader:
    @staticmethod
    def upload_file(local_path, remote_filename, progress_callback=None):
        if not PARAMIKO_AVAILABLE:
            return False, None, "paramiko not installed"
        remote_path = REMOTE_UPLOAD_PATH + remote_filename
        transport = None
        sftp = None
        try:
            transport = paramiko.Transport((SSH_HOST, SSH_PORT))
            transport.connect(username=SSH_USER, password=SSH_PASSWORD)
            sftp = paramiko.SFTPClient.from_transport(transport)
            try:
                sftp.stat(REMOTE_UPLOAD_PATH)
            except FileNotFoundError:
                sftp.mkdir(REMOTE_UPLOAD_PATH)
            file_size = os.path.getsize(local_path)
            uploaded = 0
            def callback_sent(bytes_sent):
                nonlocal uploaded
                uploaded += bytes_sent
                if progress_callback and file_size:
                    progress_callback((uploaded / file_size) * 100)
            sftp.put(local_path, remote_path, callback=callback_sent)
            return True, remote_path, None
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return False, None, str(e)
        finally:
            if sftp: sftp.close()
            if transport: transport.close()

# ============================================================================
#                               DOWNLOAD MANAGER
# ============================================================================

def progress_hook(item, d):
    if d["status"] == "downloading":
        total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
        downloaded = d.get("downloaded_bytes", 0)
        if total > 0:
            percent = (downloaded / total) * 100
            item["progress"] = percent
            speed = d.get("speed", 0)
            item["speed"] = speed if speed else 0
            eta = d.get("eta", 0)
            item["eta"] = f"{eta//60}:{eta%60:02d}" if eta else "N/A"
            socketio.emit("queue_update", {"queue": get_queue_summary()})
            socketio.emit("progress", {"item_id": item["id"], "percent": percent})
    elif d["status"] == "finished":
        item["progress"] = 100
        item["status"] = "completed"
        item["filename"] = os.path.basename(d["filename"])
        item["local_path"] = d["filename"]
        socketio.emit("queue_update", {"queue": get_queue_summary()})
        start_upload_for_item(item)

def download_video(item):
    global active_download
    item["status"] = "downloading"
    item["start_time"] = time.time()
    item["error"] = None
    ydl_opts = {
        "outtmpl": str(DOWNLOADS_DIR / "%(title)s.%(ext)s"),
        "format": "best[height<=720]/best",
        "noplaylist": True,
        "progress_hooks": [lambda d: progress_hook(item, d)],
        "quiet": True,
        "no_warnings": True,
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(item["url"], download=False)
            item["title"] = info.get("title", "Unknown")
            ydl.download([item["url"]])
    except Exception as e:
        logger.error(f"Download error: {e}")
        item["status"] = "failed"
        item["error"] = str(e)
        socketio.emit("queue_update", {"queue": get_queue_summary()})
    finally:
        with queue_lock:
            active_download = None
        save_queue_state()

def start_upload_for_item(item):
    if item["status"] != "completed" or not item.get("local_path"):
        return
    if not PARAMIKO_AVAILABLE:
        logger.warning("SSH upload skipped - paramiko missing")
        item["status"] = "uploaded"
        item["upload_status"] = "skipped (no paramiko)"
        socketio.emit("queue_update", {"queue": get_queue_summary()})
        return
    def upload_thread():
        item["status"] = "uploading"
        socketio.emit("queue_update", {"queue": get_queue_summary()})
        def progress_cb(percent):
            item["upload_progress"] = percent
            socketio.emit("upload_progress", {"item_id": item["id"], "percent": percent})
        remote_name = os.path.basename(item["local_path"])
        success, remote_path, error = SSHUploader.upload_file(item["local_path"], remote_name, progress_cb)
        if success:
            item["status"] = "uploaded"
            item["remote_path"] = remote_path
            item["upload_status"] = "success"
        else:
            item["status"] = "failed"
            item["error"] = f"Upload failed: {error}"
        item["completed_time"] = time.time()
        socketio.emit("queue_update", {"queue": get_queue_summary()})
        save_queue_state()
    threading.Thread(target=upload_thread, daemon=True).start()

def queue_worker():
    global active_download
    while not should_exit:
        if queue_paused:
            time.sleep(1)
            continue
        with queue_lock:
            next_item = None
            for item in queue_items:
                if item["status"] == "waiting":
                    next_item = item
                    break
        if next_item and active_download is None:
            active_download = next_item
            threading.Thread(target=download_video, args=(next_item,), daemon=True).start()
        time.sleep(1)

# ============================================================================
#                               HELPERS
# ============================================================================

def get_queue_summary():
    with queue_lock:
        return [{
            "id": i["id"], "url": i["url"], "status": i["status"],
            "progress": i.get("progress", 0), "speed": i.get("speed", 0),
            "eta": i.get("eta", ""), "title": i.get("title", ""),
            "filename": i.get("filename", ""), "error": i.get("error"),
            "upload_status": i.get("upload_status"), "upload_progress": i.get("upload_progress", 0)
        } for i in queue_items]

def get_system_status():
    stats = {"waiting": 0, "downloading": 0, "uploading": 0, "total": len(queue_items)}
    with queue_lock:
        for i in queue_items:
            if i["status"] == "waiting": stats["waiting"] += 1
            elif i["status"] == "downloading": stats["downloading"] += 1
            elif i["status"] == "uploading": stats["uploading"] += 1
    if PSUTIL_AVAILABLE:
        stats.update({
            "cpu": psutil.cpu_percent(interval=0.3),
            "ram": psutil.virtual_memory().percent,
            "disk": psutil.disk_usage(str(BASE_DIR)).percent
        })
    else:
        stats.update({"cpu": "N/A", "ram": "N/A", "disk": "N/A"})
    stats["uptime"] = time.time() - start_time
    return stats

def get_video_files():
    videos = []
    for f in DOWNLOADS_DIR.iterdir():
        if f.is_file() and f.suffix.lower() in (".mp4", ".webm", ".mkv", ".avi", ".mov"):
            videos.append({
                "name": f.name, "path": f"/downloads/{f.name}",
                "size": f.stat().st_size, "modified": f.stat().st_mtime
            })
    videos.sort(key=lambda x: x["modified"], reverse=True)
    return videos

# ============================================================================
#                               FLASK ROUTES
# ============================================================================

@app.route("/")
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route("/api/queue", methods=["GET"])
def api_queue():
    return jsonify(get_queue_summary())

@app.route("/api/queue/add", methods=["POST"])
def api_add_to_queue():
    data = request.json
    urls = data.get("urls", [])
    if not urls:
        return jsonify({"error": "No URLs"}), 400
    with queue_lock:
        for url in urls:
            if url.startswith(("http://", "https://")):
                queue_items.append({
                    "id": str(uuid.uuid4()), "url": url, "status": "waiting",
                    "progress": 0, "speed": 0, "eta": "", "title": "", "filename": "",
                    "error": None, "upload_status": None, "created_time": time.time()
                })
    save_queue_state()
    socketio.emit("queue_update", {"queue": get_queue_summary()})
    return jsonify({"status": "added", "count": len(urls)})

@app.route("/api/queue/remove/<item_id>", methods=["DELETE"])
def api_remove_item(item_id):
    with queue_lock:
        global active_download
        for i, item in enumerate(queue_items):
            if item["id"] == item_id:
                if item == active_download:
                    stop_current.set()
                del queue_items[i]
                break
    save_queue_state()
    socketio.emit("queue_update", {"queue": get_queue_summary()})
    return jsonify({"status": "removed"})

@app.route("/api/queue/retry/<item_id>", methods=["POST"])
def api_retry_item(item_id):
    with queue_lock:
        for item in queue_items:
            if item["id"] == item_id and item["status"] in ("failed", "completed", "uploaded"):
                item["status"] = "waiting"
                item["error"] = None
                item["progress"] = 0
                break
    save_queue_state()
    socketio.emit("queue_update", {"queue": get_queue_summary()})
    return jsonify({"status": "retry scheduled"})

@app.route("/api/queue/pause", methods=["POST"])
def api_pause_queue():
    global queue_paused
    queue_paused = True
    if active_download:
        stop_current.set()
    save_queue_state()
    return jsonify({"status": "paused"})

@app.route("/api/queue/resume", methods=["POST"])
def api_resume_queue():
    global queue_paused
    queue_paused = False
    save_queue_state()
    return jsonify({"status": "resumed"})

@app.route("/api/files", methods=["GET"])
def api_files():
    return jsonify(get_video_files())

@app.route("/api/files/delete", methods=["POST"])
def api_delete_file():
    filename = request.json.get("filename")
    if not filename:
        return jsonify({"error": "No filename"}), 400
    filepath = DOWNLOADS_DIR / filename
    if filepath.exists():
        filepath.unlink()
        return jsonify({"status": "deleted"})
    return jsonify({"error": "Not found"}), 404

@app.route("/api/status", methods=["GET"])
def api_status():
    return jsonify(get_system_status())

@app.route("/downloads/<path:filename>")
def serve_video(filename):
    return send_from_directory(DOWNLOADS_DIR, filename)

# ============================================================================
#                               SOCKETIO
# ============================================================================

@socketio.on("connect")
def handle_connect():
    emit("queue_update", {"queue": get_queue_summary()})

# ============================================================================
#                               MAIN
# ============================================================================

start_time = time.time()

if __name__ == "__main__":
    load_queue_state()
    queue_worker_thread = threading.Thread(target=queue_worker, daemon=True)
    queue_worker_thread.start()
    try:
        socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
    except KeyboardInterrupt:
        should_exit = True
        save_queue_state()
        sys.exit(0)

# ============================================================================
#                               EMBEDDED HTML/CSS/JS
# ============================================================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>VPlayer | Enterprise Media Platform</title>
    <style>
        * { margin:0; padding:0; box-sizing:border-box; }
        :root {
            --bg-dark: #0b0c0e;
            --bg-card: rgba(20,22,27,0.9);
            --accent: #00e5c0;
            --text-main: #f0f3f8;
            --text-dim: #8b93a7;
            --danger: #ff4d6d;
            --radius: 16px;
        }
        body {
            background: radial-gradient(circle at 20% 30%, #14161c, #0b0c0e);
            font-family: 'Inter', system-ui, -apple-system, sans-serif;
            color: var(--text-main);
            padding: 0;
            margin: 0;
        }
        .glass-panel {
            background: var(--bg-card);
            backdrop-filter: blur(16px);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: var(--radius);
            padding: 1.5rem;
            margin: 1rem;
        }
        .navbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1rem 2rem;
            background: rgba(8,10,14,0.8);
            border-bottom: 1px solid rgba(0,229,192,0.2);
        }
        .logo {
            font-size: 1.6rem;
            font-weight: 700;
            background: linear-gradient(135deg, #fff, var(--accent));
            -webkit-background-clip: text;
            background-clip: text;
            color: transparent;
        }
        .tabs {
            display: flex;
            gap: 0.5rem;
            padding: 1rem 2rem 0 2rem;
            flex-wrap: wrap;
        }
        .tab-btn {
            background: transparent;
            border: none;
            padding: 0.6rem 1.4rem;
            color: var(--text-dim);
            cursor: pointer;
            border-radius: 40px;
        }
        .tab-btn.active {
            background: rgba(0,229,192,0.12);
            color: var(--accent);
        }
        .tab-content { display: none; padding: 1rem; }
        .tab-content.active { display: block; }
        input, textarea, button {
            background: #1a1c23;
            border: 1px solid #2a2d36;
            border-radius: 14px;
            padding: 0.8rem 1rem;
            color: white;
        }
        button {
            background: var(--accent);
            color: #0b0c0e;
            font-weight: bold;
            cursor: pointer;
            border: none;
        }
        button.secondary { background: #2a2d36; color: white; }
        button.danger { background: var(--danger); color: white; }
        .queue-item {
            background: #1a1c23;
            border-radius: 20px;
            padding: 1rem;
            margin-bottom: 0.8rem;
            border-left: 4px solid var(--accent);
        }
        .progress-bar {
            background: #2a2d36;
            border-radius: 10px;
            height: 8px;
            overflow: hidden;
            margin: 8px 0;
        }
        .progress-fill {
            background: linear-gradient(90deg, var(--accent), #00a88f);
            width: 0%;
            height: 100%;
        }
        .video-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
            gap: 1rem;
        }
        .video-card {
            background: #1a1c23;
            border-radius: 18px;
            padding: 0.8rem;
        }
        video { width: 100%; border-radius: 12px; }
        @media (max-width: 680px) {
            .tabs { padding: 0.5rem; overflow-x: auto; flex-wrap: nowrap; }
            .tab-btn { padding: 0.4rem 1rem; font-size: 0.8rem; }
        }
    </style>
    <script src="/socket.io/socket.io.js"></script>
    <script>
        let socket = io();
        let currentQueue = [];

        async function fetchQueue() {
            const res = await fetch('/api/queue');
            currentQueue = await res.json();
            renderQueue();
            document.getElementById('queueCount').innerText = currentQueue.length;
        }
        function renderQueue() {
            const container = document.getElementById('queueContainer');
            if(!container) return;
            if(!currentQueue.length) { container.innerHTML = '<div class="glass-panel">✨ Queue is empty</div>'; return; }
            container.innerHTML = currentQueue.map(item => `
                <div class="queue-item">
                    <div><strong>${escapeHtml(item.title || item.url.substring(0,60))}</strong><br>
                    <small>${item.status.toUpperCase()} | ${item.speed ? formatBytes(item.speed)+'/s' : ''} ${item.eta ? 'ETA '+item.eta : ''}</small>
                    <div class="progress-bar"><div class="progress-fill" style="width:${item.progress}%"></div></div>
                    ${item.error ? `<div style="color:var(--danger);">⚠️ ${item.error}</div>` : ''}
                    </div>
                    <div style="margin-top:8px;">
                        ${item.status === 'failed' ? `<button class="secondary" onclick="retryItem('${item.id}')">⟳ Retry</button>` : ''}
                        <button class="danger" onclick="removeItem('${item.id}')">Delete</button>
                    </div>
                </div>
            `).join('');
        }
        async function addToQueue() {
            const urls = document.getElementById('bulkUrls').value.split('\\n').filter(u => u.trim().startsWith('http'));
            if(!urls.length) { alert('Enter valid URLs'); return; }
            await fetch('/api/queue/add', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({urls})});
            document.getElementById('bulkUrls').value = '';
            fetchQueue();
        }
        async function removeItem(id) { await fetch(`/api/queue/remove/${id}`, {method:'DELETE'}); fetchQueue(); }
        async function retryItem(id) { await fetch(`/api/queue/retry/${id}`, {method:'POST'}); fetchQueue(); }
        async function pauseQueue() { await fetch('/api/queue/pause', {method:'POST'}); fetchQueue(); }
        async function resumeQueue() { await fetch('/api/queue/resume', {method:'POST'}); fetchQueue(); }
        async function fetchFiles() {
            const res = await fetch('/api/files');
            const files = await res.json();
            const grid = document.getElementById('videoGrid');
            if(!files.length) { grid.innerHTML = '<div class="glass-panel">📁 No videos yet</div>'; return; }
            grid.innerHTML = files.map(v => `
                <div class="video-card">
                    <video controls preload="metadata"><source src="${v.path}" type="video/mp4"></video>
                    <div><strong>${escapeHtml(v.name)}</strong></div>
                    <div style="font-size:0.7rem;">${formatBytes(v.size)}</div>
                    <button onclick="playVideo('${v.path}')">▶ Play</button>
                    <button class="danger" onclick="deleteFile('${v.name}')">Delete</button>
                </div>
            `).join('');
        }
        async function deleteFile(name) { if(confirm('Delete?')) await fetch('/api/files/delete', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({filename:name})}); fetchFiles(); }
        function playVideo(path) { document.getElementById('playerContainer').innerHTML = `<video controls autoplay style="width:100%;"><source src="${path}" type="video/mp4"></video>`; switchTab('player'); }
        async function fetchStatus() {
            const res = await fetch('/api/status');
            const data = await res.json();
            document.getElementById('statCpu').innerText = data.cpu; document.getElementById('statRam').innerText = data.ram;
            document.getElementById('statDisk').innerText = data.disk; document.getElementById('statQueue').innerText = data.waiting + ' waiting';
        }
        function switchTab(tabId) {
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.getElementById(tabId + 'Tab').classList.add('active');
            document.querySelector(`[data-tab="${tabId}"]`).classList.add('active');
            if(tabId === 'files') fetchFiles();
            if(tabId === 'status') fetchStatus();
        }
        function formatBytes(bytes) { if(!bytes) return '0 B'; const k=1024, sizes=['B','KB','MB','GB']; let i=Math.floor(Math.log(bytes)/Math.log(k)); return parseFloat((bytes/Math.pow(k,i)).toFixed(1))+' '+sizes[i]; }
        function escapeHtml(str) { if(!str) return ''; return str.replace(/[&<>]/g, function(m){if(m==='&') return '&amp;'; if(m==='<') return '&lt;'; if(m==='>') return '&gt;'; return m;}); }
        window.onload = () => {
            fetchQueue(); fetchFiles(); fetchStatus();
            setInterval(fetchStatus, 4000);
            document.getElementById('bulkAddBtn').onclick = addToQueue;
            document.getElementById('pauseQueueBtn').onclick = pauseQueue;
            document.getElementById('resumeQueueBtn').onclick = resumeQueue;
            document.getElementById('singleDownloadBtn').onclick = async () => {
                let url = document.getElementById('singleUrl').value.trim();
                if(url) { await addToQueueByUrl(url); document.getElementById('singleUrl').value = ''; }
            };
        };
        async function addToQueueByUrl(url) { await fetch('/api/queue/add', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({urls:[url]})}); fetchQueue(); switchTab('queueView'); }
        window.removeItem = removeItem; window.retryItem = retryItem; window.deleteFile = deleteFile; window.playVideo = playVideo; window.switchTab = switchTab;
    </script>
</head>
<body>
    <nav class="navbar"><div class="logo">🎬 VPLAYER</div><div class="status-badge">📦 Queue: <span id="queueCount">0</span></div></nav>
    <div class="tabs">
        <button class="tab-btn active" data-tab="single" onclick="switchTab('single')">📥 Single</button>
        <button class="tab-btn" data-tab="bulk" onclick="switchTab('bulk')">📋 Queue</button>
        <button class="tab-btn" data-tab="queueView" onclick="switchTab('queueView')">⏯ Manager</button>
        <button class="tab-btn" data-tab="files" onclick="switchTab('files')">🗂 Files</button>
        <button class="tab-btn" data-tab="status" onclick="switchTab('status')">📊 Status</button>
        <button class="tab-btn" data-tab="player" onclick="switchTab('player')">🎮 Player</button>
    </div>
    <div id="singleTab" class="tab-content active"><div class="glass-panel"><h3>⚡ Quick Download</h3><div class="input-group" style="display:flex; gap:8px;"><input type="text" id="singleUrl" placeholder="https://..." style="flex:1"><button id="singleDownloadBtn">Download</button></div></div></div>
    <div id="bulkTab" class="tab-content"><div class="glass-panel"><h3>➕ Multiple URLs</h3><textarea id="bulkUrls" rows="4" placeholder="https://...\nhttps://..."></textarea><div style="display:flex; gap:8px; margin-top:12px;"><button id="bulkAddBtn">Add to Queue</button><button id="pauseQueueBtn" class="secondary">⏸ Pause</button><button id="resumeQueueBtn" class="secondary">▶ Resume</button></div></div></div>
    <div id="queueViewTab" class="tab-content"><div class="glass-panel"><h3>📋 Download Queue</h3><div id="queueContainer"></div></div></div>
    <div id="filesTab" class="tab-content"><div class="glass-panel"><h3>🗂 Media Library</h3><div id="videoGrid" class="video-grid"></div></div></div>
    <div id="statusTab" class="tab-content"><div class="glass-panel"><h3>📈 System Health</h3><div style="display:flex; gap:1rem; flex-wrap:wrap;"><div class="stat-box">🧠 CPU: <span id="statCpu">--</span></div><div class="stat-box">💾 RAM: <span id="statRam">--</span></div><div class="stat-box">💽 Disk: <span id="statDisk">--</span></div><div class="stat-box">⏳ Queue: <span id="statQueue">--</span></div></div></div></div>
    <div id="playerTab" class="tab-content"><div class="glass-panel"><h3>🎬 Media Player</h3><div id="playerContainer"><video controls style="width:100%;"><source src="" type="video/mp4"></video></div><p style="margin-top:12px;">Select a video from File Manager to play</p></div></div>
</body>
</html>
"""