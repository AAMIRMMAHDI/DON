#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
================================================================================
                        VPLAYER - ENTERPRISE MEDIA PLATFORM
================================================================================
A complete offline-first, production-grade video downloader and media manager.
All-in-one single file: Flask backend, queue system, SSH uploader, real-time UI.
================================================================================
"""

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

import yt_dlp
from flask import Flask, request, jsonify, render_template_string, send_from_directory
from flask_socketio import SocketIO, emit
import paramiko
import psutil
from dotenv import load_dotenv

# ============================================================================
#                               CONFIGURATION
# ============================================================================

load_dotenv()

BASE_DIR = Path(__file__).parent.absolute()
STATIC_DIR = BASE_DIR / "static"
DOWNLOADS_DIR = BASE_DIR / "downloads"
LOGS_DIR = BASE_DIR / "logs"

# Create necessary directories
for d in [STATIC_DIR, DOWNLOADS_DIR, LOGS_DIR]:
    d.mkdir(exist_ok=True)

# SSH Configuration (from .env)
SSH_HOST = os.getenv("SSH_HOST", "185.208.175.180")
SSH_USER = os.getenv("SSH_USER", "root")
SSH_PASSWORD = os.getenv("SSH_PASSWORD", "Amir.1388")
SSH_PORT = int(os.getenv("SSH_PORT", 22))
REMOTE_UPLOAD_PATH = os.getenv("REMOTE_UPLOAD_PATH", "/root/videos/")

# Flask & SocketIO
app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "vplayer-super-secret-key")
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

# ============================================================================
#                               LOGGING SYSTEM
# ============================================================================

def setup_logging():
    log_format = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    # Main log file
    main_handler = RotatingFileHandler(
        LOGS_DIR / "vplayer.log", maxBytes=10_485_760, backupCount=5
    )
    main_handler.setFormatter(log_format)
    # Error log file
    error_handler = RotatingFileHandler(
        LOGS_DIR / "errors.log", maxBytes=5_242_880, backupCount=3
    )
    error_handler.setLevel(logging.ERROR)
    error_handler.setFormatter(log_format)
    # Console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_format)

    logger = logging.getLogger("vplayer")
    logger.setLevel(logging.INFO)
    logger.addHandler(main_handler)
    logger.addHandler(error_handler)
    logger.addHandler(console_handler)
    return logger

logger = setup_logging()

# ============================================================================
#                               QUEUE & STATE MANAGEMENT
# ============================================================================

# Queue items structure
queue_items = []          # list of dicts, order matters
queue_lock = threading.Lock()
active_download = None    # current item being processed (dict or None)
stop_current = threading.Event()
queue_paused = False
queue_worker_thread = None
should_exit = False

# Persistence file
QUEUE_STATE_FILE = BASE_DIR / "queue_state.json"

def save_queue_state():
    """Persist queue items to disk for recovery."""
    with queue_lock:
        # Remove non-serializable objects
        to_save = []
        for item in queue_items:
            item_copy = {
                k: v for k, v in item.items()
                if k not in ["progress_hook", "thread"]
            }
            to_save.append(item_copy)
        data = {
            "items": to_save,
            "paused": queue_paused
        }
    try:
        with open(QUEUE_STATE_FILE, "w") as f:
            json.dump(data, f, indent=2)
        logger.info("Queue state saved.")
    except Exception as e:
        logger.error(f"Failed to save queue state: {e}")

def load_queue_state():
    """Restore queue from disk on startup."""
    global queue_paused
    if QUEUE_STATE_FILE.exists():
        try:
            with open(QUEUE_STATE_FILE, "r") as f:
                data = json.load(f)
            with queue_lock:
                queue_items.clear()
                for it in data.get("items", []):
                    # Restore each item with fresh fields
                    it["progress"] = 0
                    it["speed"] = 0
                    it["eta"] = ""
                    it["upload_status"] = None
                    it["remote_path"] = None
                    it["error"] = None
                    it["thread"] = None
                    it["progress_hook"] = None
                    # If it was downloading when crashed, reset to waiting
                    if it["status"] in ("downloading", "uploading"):
                        it["status"] = "waiting"
                    queue_items.append(it)
                queue_paused = data.get("paused", False)
            logger.info(f"Loaded {len(queue_items)} items from queue state.")
        except Exception as e:
            logger.error(f"Failed to load queue state: {e}")

# ============================================================================
#                               SSH UPLOAD SERVICE
# ============================================================================

class SSHUploader:
    """Handles SFTP upload with retries and progress tracking."""

    @staticmethod
    def upload_file(local_path, remote_filename, progress_callback=None):
        """
        Upload a file via SFTP.
        Returns (success, remote_path, error_message)
        """
        remote_path = os.path.join(REMOTE_UPLOAD_PATH, remote_filename).replace("\\", "/")
        transport = None
        sftp = None
        try:
            transport = paramiko.Transport((SSH_HOST, SSH_PORT))
            transport.connect(username=SSH_USER, password=SSH_PASSWORD)
            sftp = paramiko.SFTPClient.from_transport(transport)

            # Ensure remote directory exists
            try:
                sftp.stat(REMOTE_UPLOAD_PATH)
            except FileNotFoundError:
                sftp.mkdir(REMOTE_UPLOAD_PATH)

            file_size = os.path.getsize(local_path)
            uploaded = 0

            def callback_sent(bytes_sent):
                nonlocal uploaded
                uploaded += bytes_sent
                if progress_callback and file_size > 0:
                    percent = (uploaded / file_size) * 100
                    progress_callback(percent)

            sftp.put(local_path, remote_path, callback=callback_sent)
            return True, remote_path, None
        except Exception as e:
            logger.error(f"Upload failed: {e}")
            return False, None, str(e)
        finally:
            if sftp:
                sftp.close()
            if transport:
                transport.close()

# ============================================================================
#                               DOWNLOAD MANAGER
# ============================================================================

def progress_hook(item, d):
    """yt-dlp progress hook."""
    if d["status"] == "downloading":
        total = d.get("total_bytes") or d.get("total_bytes_estimate", 0)
        downloaded = d.get("downloaded_bytes", 0)
        if total > 0:
            percent = (downloaded / total) * 100
            item["progress"] = percent
            # Speed and ETA
            speed = d.get("speed", 0)
            item["speed"] = speed if speed else 0
            eta = d.get("eta", 0)
            item["eta"] = f"{eta // 60}:{eta % 60:02d}" if eta else "N/A"
            # Emit real-time update
            socketio.emit("queue_update", {"queue": get_queue_summary()})
            socketio.emit("progress", {
                "item_id": item["id"],
                "percent": percent,
                "speed": item["speed"],
                "eta": item["eta"]
            })
    elif d["status"] == "finished":
        item["progress"] = 100
        item["status"] = "completed"
        item["filename"] = os.path.basename(d["filename"])
        item["local_path"] = d["filename"]
        socketio.emit("queue_update", {"queue": get_queue_summary()})
        # Start upload automatically
        start_upload_for_item(item)

def download_video(item):
    """Worker for a single download."""
    url = item["url"]
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
            info = ydl.extract_info(url, download=False)
            item["title"] = info.get("title", "Unknown")
            item["duration"] = info.get("duration", 0)
            item["thumbnail"] = info.get("thumbnail", "")
            ydl.download([url])
    except Exception as e:
        logger.error(f"Download error for {url}: {e}")
        item["status"] = "failed"
        item["error"] = str(e)
        socketio.emit("queue_update", {"queue": get_queue_summary()})

def start_upload_for_item(item):
    """Trigger SSH upload after successful download."""
    if item["status"] != "completed":
        return
    if not item.get("local_path") or not os.path.exists(item["local_path"]):
        item["status"] = "failed"
        item["error"] = "Local file missing after download"
        socketio.emit("queue_update", {"queue": get_queue_summary()})
        return

    def upload_thread():
        item["status"] = "uploading"
        item["upload_status"] = "starting"
        socketio.emit("queue_update", {"queue": get_queue_summary()})

        def progress_cb(percent):
            item["upload_progress"] = percent
            socketio.emit("upload_progress", {
                "item_id": item["id"],
                "percent": percent
            })

        remote_filename = os.path.basename(item["local_path"])
        success, remote_path, error = SSHUploader.upload_file(
            item["local_path"], remote_filename, progress_cb
        )
        if success:
            item["status"] = "uploaded"
            item["remote_path"] = remote_path
            item["upload_status"] = "success"
            logger.info(f"Uploaded {remote_filename} to {remote_path}")
        else:
            item["status"] = "failed"
            item["error"] = f"Upload failed: {error}"
            item["upload_status"] = "failed"
            logger.error(f"Upload failed for {item['id']}: {error}")
        item["completed_time"] = time.time()
        socketio.emit("queue_update", {"queue": get_queue_summary()})
        save_queue_state()

    threading.Thread(target=upload_thread, daemon=True).start()

def queue_worker():
    """Background worker that processes queue items sequentially."""
    global active_download
    while not should_exit:
        if queue_paused:
            time.sleep(1)
            continue
        # Pick next waiting item
        with queue_lock:
            next_item = None
            for item in queue_items:
                if item["status"] == "waiting":
                    next_item = item
                    break
        if next_item and active_download is None:
            active_download = next_item
            # Download in a separate thread to keep worker responsive
            def run_download():
                global active_download
                download_video(next_item)
                with queue_lock:
                    active_download = None
                save_queue_state()
                # After download finishes (success or fail), continue loop
            t = threading.Thread(target=run_download, daemon=True)
            t.start()
            next_item["thread"] = t
        time.sleep(1)

# ============================================================================
#                               HELPER FUNCTIONS
# ============================================================================

def get_queue_summary():
    """Return sanitized queue data for frontend."""
    with queue_lock:
        summary = []
        for item in queue_items:
            summary.append({
                "id": item["id"],
                "url": item["url"],
                "status": item["status"],
                "progress": item.get("progress", 0),
                "speed": item.get("speed", 0),
                "eta": item.get("eta", ""),
                "title": item.get("title", ""),
                "filename": item.get("filename", ""),
                "error": item.get("error"),
                "upload_status": item.get("upload_status"),
                "upload_progress": item.get("upload_progress", 0),
                "remote_path": item.get("remote_path"),
            })
        return summary

def get_system_status():
    """CPU, RAM, disk, queue stats."""
    cpu = psutil.cpu_percent(interval=0.5)
    ram = psutil.virtual_memory().percent
    disk = psutil.disk_usage(str(BASE_DIR)).percent
    with queue_lock:
        waiting = sum(1 for i in queue_items if i["status"] == "waiting")
        downloading = 1 if active_download else 0
        uploading = sum(1 for i in queue_items if i["status"] == "uploading")
    return {
        "cpu": cpu,
        "ram": ram,
        "disk": disk,
        "waiting": waiting,
        "downloading": downloading,
        "uploading": uploading,
        "total": len(queue_items),
        "uptime": time.time() - start_time
    }

def get_video_files():
    """List all downloaded videos in DOWNLOADS_DIR."""
    videos = []
    for f in DOWNLOADS_DIR.iterdir():
        if f.is_file() and f.suffix.lower() in (".mp4", ".webm", ".mkv", ".avi", ".mov"):
            videos.append({
                "name": f.name,
                "path": f"/downloads/{f.name}",
                "size": f.stat().st_size,
                "modified": f.stat().st_mtime
            })
    videos.sort(key=lambda x: x["modified"], reverse=True)
    return videos

# ============================================================================
#                               FLASK ROUTES
# ============================================================================

@app.route("/")
def index():
    """Main dashboard with all tabs."""
    return render_template_string(HTML_TEMPLATE)

@app.route("/api/queue", methods=["GET"])
def api_queue():
    return jsonify(get_queue_summary())

@app.route("/api/queue/add", methods=["POST"])
def api_add_to_queue():
    data = request.json
    urls = data.get("urls", [])
    if not urls:
        return jsonify({"error": "No URLs provided"}), 400
    with queue_lock:
        for url in urls:
            if not url.startswith(("http://", "https://")):
                continue
            new_id = str(uuid.uuid4())
            queue_items.append({
                "id": new_id,
                "url": url,
                "status": "waiting",
                "progress": 0,
                "speed": 0,
                "eta": "",
                "title": "",
                "filename": "",
                "error": None,
                "upload_status": None,
                "upload_progress": 0,
                "remote_path": None,
                "created_time": time.time(),
                "completed_time": None,
            })
    save_queue_state()
    socketio.emit("queue_update", {"queue": get_queue_summary()})
    return jsonify({"status": "added", "count": len(urls)})

@app.route("/api/queue/reorder", methods=["POST"])
def api_reorder_queue():
    data = request.json
    new_order_ids = data.get("order", [])
    with queue_lock:
        # Rebuild queue_items according to new_order_ids
        new_queue = []
        for qid in new_order_ids:
            for item in queue_items:
                if item["id"] == qid:
                    new_queue.append(item)
                    break
        # Append any items not in the new order (should not happen)
        for item in queue_items:
            if item not in new_queue:
                new_queue.append(item)
        queue_items[:] = new_queue
    save_queue_state()
    socketio.emit("queue_update", {"queue": get_queue_summary()})
    return jsonify({"status": "reordered"})

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
                item["upload_status"] = None
                break
    save_queue_state()
    socketio.emit("queue_update", {"queue": get_queue_summary()})
    return jsonify({"status": "retry scheduled"})

@app.route("/api/queue/pause", methods=["POST"])
def api_pause_queue():
    global queue_paused
    queue_paused = True
    # Stop active download
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
    data = request.json
    filename = data.get("filename")
    if not filename:
        return jsonify({"error": "No filename"}), 400
    filepath = DOWNLOADS_DIR / filename
    try:
        if filepath.exists():
            filepath.unlink()
            return jsonify({"status": "deleted"})
        else:
            return jsonify({"error": "File not found"}), 404
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/api/status", methods=["GET"])
def api_status():
    return jsonify(get_system_status())

@app.route("/downloads/<path:filename>")
def serve_video(filename):
    """Serve downloaded videos for playback."""
    return send_from_directory(DOWNLOADS_DIR, filename, as_attachment=False)

# ============================================================================
#                               SOCKETIO EVENTS
# ============================================================================

@socketio.on("connect")
def handle_connect():
    logger.info("Client connected")
    emit("connected", {"status": "ok"})
    emit("queue_update", {"queue": get_queue_summary()})

@socketio.on("disconnect")
def handle_disconnect():
    logger.info("Client disconnected")

# ============================================================================
#                               MAIN ENTRY POINT
# ============================================================================

start_time = time.time()

def main():
    global queue_worker_thread, should_exit
    # Load previous queue
    load_queue_state()
    # Start queue worker thread
    queue_worker_thread = threading.Thread(target=queue_worker, daemon=True)
    queue_worker_thread.start()
    # Start Flask-SocketIO
    try:
        socketio.run(app, host="0.0.0.0", port=5000, debug=False, allow_unsafe_werkzeug=True)
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        should_exit = True
        if queue_worker_thread:
            queue_worker_thread.join(timeout=2)
        save_queue_state()
        sys.exit(0)

if __name__ == "__main__":
    main()

# ============================================================================
#                               EMBEDDED HTML/CSS/JS
# ============================================================================

HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, user-scalable=yes">
    <title>VPlayer | Enterprise Media Platform</title>
    <style>
        /* ---------- GLOBAL RESET & VARIABLES ---------- */
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        :root {
            --bg-dark: #0b0c0e;
            --bg-card: rgba(20, 22, 27, 0.85);
            --border-glow: rgba(0, 255, 200, 0.3);
            --accent: #00e5c0;
            --accent-glow: 0 0 8px rgba(0, 229, 192, 0.6);
            --text-main: #f0f3f8;
            --text-dim: #8b93a7;
            --danger: #ff4d6d;
            --success: #00d26a;
            --warning: #ffb347;
            --radius: 16px;
            --transition: all 0.2s ease;
        }

        body {
            background: radial-gradient(circle at 20% 30%, #14161c, #0b0c0e);
            font-family: 'Inter', system-ui, -apple-system, 'Segoe UI', Roboto, sans-serif;
            color: var(--text-main);
            padding: 0;
            margin: 0;
            line-height: 1.5;
        }

        /* Custom scrollbar */
        ::-webkit-scrollbar {
            width: 6px;
            height: 6px;
        }
        ::-webkit-scrollbar-track {
            background: #1e2028;
            border-radius: 10px;
        }
        ::-webkit-scrollbar-thumb {
            background: var(--accent);
            border-radius: 10px;
        }

        /* Typography */
        h1, h2, h3 {
            font-weight: 600;
            letter-spacing: -0.02em;
        }
        a {
            text-decoration: none;
            color: inherit;
        }

        /* ---------- GLASS PANEL ---------- */
        .glass-panel {
            background: var(--bg-card);
            backdrop-filter: blur(16px);
            border: 1px solid rgba(255,255,255,0.08);
            border-radius: var(--radius);
            box-shadow: 0 12px 30px rgba(0,0,0,0.3);
            transition: var(--transition);
        }

        /* ---------- NAVBAR ---------- */
        .navbar {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 1rem 2rem;
            background: rgba(8, 10, 14, 0.8);
            backdrop-filter: blur(12px);
            border-bottom: 1px solid rgba(0,229,192,0.2);
            position: sticky;
            top: 0;
            z-index: 100;
        }
        .logo {
            font-size: 1.6rem;
            font-weight: 700;
            background: linear-gradient(135deg, #fff, var(--accent));
            -webkit-background-clip: text;
            background-clip: text;
            color: transparent;
        }
        .status-badge {
            display: flex;
            gap: 1.2rem;
            background: rgba(0,0,0,0.5);
            padding: 0.4rem 1rem;
            border-radius: 40px;
            font-size: 0.8rem;
        }
        .status-badge span {
            color: var(--text-dim);
        }
        .status-badge i {
            color: var(--accent);
            font-weight: bold;
        }

        /* ---------- MAIN TABS ---------- */
        .tabs {
            display: flex;
            gap: 0.5rem;
            padding: 1rem 2rem 0 2rem;
            border-bottom: 1px solid rgba(255,255,255,0.05);
        }
        .tab-btn {
            background: transparent;
            border: none;
            padding: 0.6rem 1.4rem;
            font-weight: 500;
            font-size: 0.9rem;
            color: var(--text-dim);
            cursor: pointer;
            border-radius: 40px;
            transition: var(--transition);
        }
        .tab-btn.active {
            background: rgba(0,229,192,0.12);
            color: var(--accent);
            backdrop-filter: blur(4px);
        }
        .tab-content {
            display: none;
            padding: 2rem;
            animation: fadeIn 0.3s ease;
        }
        .tab-content.active {
            display: block;
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(8px);}
            to { opacity: 1; transform: translateY(0);}
        }

        /* ---------- FORMS & INPUTS ---------- */
        .input-group {
            display: flex;
            gap: 0.8rem;
            margin-top: 1rem;
            flex-wrap: wrap;
        }
        input, textarea, select {
            background: #1a1c23;
            border: 1px solid #2a2d36;
            border-radius: 14px;
            padding: 0.8rem 1rem;
            color: white;
            font-size: 0.9rem;
            transition: var(--transition);
        }
        input:focus, textarea:focus, select:focus {
            outline: none;
            border-color: var(--accent);
            box-shadow: 0 0 0 2px rgba(0,229,192,0.2);
        }
        textarea {
            width: 100%;
            min-height: 120px;
            font-family: monospace;
        }
        button {
            background: var(--accent);
            border: none;
            border-radius: 40px;
            padding: 0.6rem 1.4rem;
            font-weight: 600;
            color: #0b0c0e;
            cursor: pointer;
            transition: var(--transition);
        }
        button.secondary {
            background: #2a2d36;
            color: white;
        }
        button.danger {
            background: var(--danger);
            color: white;
        }
        button:hover {
            transform: translateY(-2px);
            filter: brightness(1.05);
        }

        /* Queue cards */
        .queue-list {
            margin-top: 1.5rem;
            display: flex;
            flex-direction: column;
            gap: 0.8rem;
        }
        .queue-item {
            background: rgba(26,28,35,0.7);
            border-radius: 20px;
            padding: 1rem;
            border-left: 4px solid var(--accent);
            display: flex;
            justify-content: space-between;
            align-items: center;
            flex-wrap: wrap;
        }
        .queue-progress {
            flex: 2;
            min-width: 150px;
        }
        .progress-bar {
            background: #2a2d36;
            border-radius: 10px;
            height: 8px;
            width: 100%;
            overflow: hidden;
        }
        .progress-fill {
            background: linear-gradient(90deg, var(--accent), #00a88f);
            width: 0%;
            height: 100%;
            border-radius: 10px;
        }
        /* Video grid */
        .video-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(240px, 1fr));
            gap: 1.2rem;
            margin-top: 1.5rem;
        }
        .video-card {
            background: #1a1c23;
            border-radius: 18px;
            padding: 0.8rem;
            transition: var(--transition);
        }
        .video-card:hover {
            transform: translateY(-4px);
            box-shadow: 0 10px 20px rgba(0,0,0,0.4);
        }
        /* Stats */
        .stats-grid {
            display: flex;
            gap: 1.2rem;
            flex-wrap: wrap;
            margin-bottom: 2rem;
        }
        .stat-box {
            background: #1a1c23;
            border-radius: 24px;
            padding: 1rem 1.6rem;
            text-align: center;
            flex: 1;
        }
        @media (max-width: 680px) {
            .tabs { padding: 0.5rem; overflow-x: auto; }
            .tab-btn { padding: 0.4rem 1rem; font-size: 0.8rem; }
            .tab-content { padding: 1rem; }
        }
        video {
            width: 100%;
            border-radius: 16px;
            background: black;
        }
    </style>
    <script src="/socket.io/socket.io.js"></script>
    <script>
        // ---------- GLOBALS ----------
        let socket = io();
        let currentQueue = [];

        // Helper: format bytes
        function formatBytes(bytes) {
            if (!bytes) return "0 B";
            const k = 1024;
            const sizes = ["B", "KB", "MB", "GB"];
            let i = Math.floor(Math.log(bytes) / Math.log(k));
            return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + " " + sizes[i];
        }

        // Refresh queue UI
        function renderQueue() {
            const container = document.getElementById("queueContainer");
            if (!container) return;
            if (!currentQueue.length) {
                container.innerHTML = `<div class="glass-panel" style="padding:2rem; text-align:center;">🎯 No items in queue. Add URLs to start.</div>`;
                return;
            }
            container.innerHTML = currentQueue.map(item => {
                let statusClass = "";
                let statusText = item.status.toUpperCase();
                if (item.status === "waiting") statusClass = "🕒";
                else if (item.status === "downloading") statusClass = "⬇️";
                else if (item.status === "uploading") statusClass = "☁️";
                else if (item.status === "completed") statusClass = "✅";
                else if (item.status === "uploaded") statusClass = "🚀";
                else if (item.status === "failed") statusClass = "❌";
                return `
                    <div class="queue-item" data-id="${item.id}">
                        <div style="flex:2">
                            <strong>${escapeHtml(item.title || item.url.substring(0,60))}</strong>
                            <div class="queue-progress">
                                <div class="progress-bar"><div class="progress-fill" style="width:${item.progress || 0}%"></div></div>
                                <small>${statusClass} ${statusText}  |  ${item.speed ? formatBytes(item.speed)+'/s' : ''}  ${item.eta ? 'ETA: '+item.eta : ''}</small>
                                ${item.error ? `<div style="color:var(--danger); font-size:0.7rem;">⚠️ ${item.error}</div>` : ''}
                            </div>
                        </div>
                        <div style="display:flex; gap:8px;">
                            ${item.status === "failed" ? `<button class="secondary" onclick="retryItem('${item.id}')">⟳ Retry</button>` : ''}
                            <button class="danger" onclick="removeItem('${item.id}')">🗑</button>
                        </div>
                    </div>
                `;
            }).join('');
        }

        function escapeHtml(str) { if(!str) return ''; return str.replace(/[&<>]/g, function(m){if(m==='&') return '&amp;'; if(m==='<') return '&lt;'; if(m==='>') return '&gt;'; return m;}); }

        async function fetchQueue() {
            const res = await fetch('/api/queue');
            currentQueue = await res.json();
            renderQueue();
            document.getElementById("queueCount").innerText = currentQueue.length;
        }

        async function fetchFiles() {
            const res = await fetch('/api/files');
            const files = await res.json();
            const grid = document.getElementById("videoGrid");
            if (!grid) return;
            if (!files.length) {
                grid.innerHTML = `<div class="glass-panel" style="padding:2rem; text-align:center;">📁 No videos downloaded yet.</div>`;
                return;
            }
            grid.innerHTML = files.map(v => `
                <div class="video-card">
                    <video controls preload="metadata" style="max-height:160px; object-fit:cover;"><source src="${v.path}" type="video/mp4"></video>
                    <div style="margin-top:8px;"><strong>${escapeHtml(v.name)}</strong></div>
                    <div style="font-size:0.7rem; color:var(--text-dim);">${formatBytes(v.size)}</div>
                    <div style="display:flex; gap:8px; margin-top:8px;">
                        <button onclick="playVideo('${v.path}')">▶ Play</button>
                        <button class="danger" onclick="deleteFile('${v.name}')">Delete</button>
                    </div>
                </div>
            `).join('');
        }

        async function fetchStatus() {
            const res = await fetch('/api/status');
            const data = await res.json();
            document.getElementById("statCpu").innerText = data.cpu + "%";
            document.getElementById("statRam").innerText = data.ram + "%";
            document.getElementById("statDisk").innerText = data.disk + "%";
            document.getElementById("statQueue").innerText = data.waiting + " waiting";
        }

        // Queue actions
        async function addToQueue() {
            const textarea = document.getElementById("bulkUrls");
            const urls = textarea.value.split('\\n').filter(u => u.trim().startsWith('http'));
            if(!urls.length) { alert("Enter at least one valid URL"); return; }
            const res = await fetch('/api/queue/add', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({urls})});
            if(res.ok) {
                textarea.value = '';
                fetchQueue();
            }
        }

        async function removeItem(id) {
            await fetch(`/api/queue/remove/${id}`, {method:'DELETE'});
            fetchQueue();
        }

        async function retryItem(id) {
            await fetch(`/api/queue/retry/${id}`, {method:'POST'});
            fetchQueue();
        }

        async function pauseQueue() { await fetch('/api/queue/pause', {method:'POST'}); fetchQueue(); }
        async function resumeQueue() { await fetch('/api/queue/resume', {method:'POST'}); fetchQueue(); }

        async function deleteFile(filename) {
            if(!confirm(`Delete ${filename}?`)) return;
            await fetch('/api/files/delete', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({filename})});
            fetchFiles();
        }

        function playVideo(path) {
            const playerContainer = document.getElementById("playerContainer");
            if(playerContainer) {
                playerContainer.innerHTML = `<video controls autoplay style="width:100%; border-radius:16px;"><source src="${path}" type="video/mp4"></video>`;
                document.getElementById("singlePlayerTab").click(); // switch to player tab
            }
        }

        // Socket listeners
        socket.on('queue_update', (data) => { fetchQueue(); fetchStatus(); });
        socket.on('progress', (data) => { fetchQueue(); });
        socket.on('upload_progress', (data) => { fetchQueue(); });

        // Tab switching
        function switchTab(tabId) {
            document.querySelectorAll('.tab-content').forEach(t => t.classList.remove('active'));
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            document.getElementById(tabId + 'Tab').classList.add('active');
            document.querySelector(`[data-tab="${tabId}"]`).classList.add('active');
            if(tabId === 'files') fetchFiles();
            if(tabId === 'status') fetchStatus();
        }

        window.onload = () => {
            fetchQueue();
            fetchFiles();
            fetchStatus();
            setInterval(() => { fetchStatus(); }, 4000);
            // Drag & drop reorder (simplified: will not implement full drag due to complexity, but API ready)
            document.getElementById("bulkAddBtn").onclick = addToQueue;
            document.getElementById("pauseQueueBtn").onclick = pauseQueue;
            document.getElementById("resumeQueueBtn").onclick = resumeQueue;
        };
    </script>
</head>
<body>
    <nav class="navbar">
        <div class="logo">🎬 VPLAYER</div>
        <div class="status-badge">
            <span>📦 Queue: <span id="queueCount">0</span></span>
            <span>⚡ Online</span>
        </div>
    </nav>
    <div class="tabs">
        <button class="tab-btn active" data-tab="single" onclick="switchTab('single')">📥 Single Download</button>
        <button class="tab-btn" data-tab="bulk" onclick="switchTab('bulk')">📋 Queue Download</button>
        <button class="tab-btn" data-tab="queueView" onclick="switchTab('queueView')">⏯ Queue Manager</button>
        <button class="tab-btn" data-tab="files" onclick="switchTab('files')">🗂 File Manager</button>
        <button class="tab-btn" data-tab="status" onclick="switchTab('status')">📊 System Status</button>
        <button class="tab-btn" data-tab="player" onclick="switchTab('player')">🎮 Media Player</button>
    </div>

    <!-- Single Download Tab -->
    <div id="singleTab" class="tab-content active">
        <div class="glass-panel" style="max-width:700px; margin:0 auto; padding:1.8rem;">
            <h3>⚡ Quick Download</h3>
            <div class="input-group">
                <input type="text" id="singleUrl" placeholder="https://youtube.com/watch?v=..." style="flex:1">
                <button id="singleDownloadBtn">Download</button>
            </div>
            <div id="singleStatus"></div>
        </div>
    </div>

    <!-- Bulk Queue Tab -->
    <div id="bulkTab" class="tab-content">
        <div class="glass-panel" style="max-width:800px; margin:0 auto;">
            <h3>➕ Add Multiple URLs</h3>
            <textarea id="bulkUrls" placeholder="https://...&#10;https://...&#10;one per line"></textarea>
            <div class="input-group">
                <button id="bulkAddBtn">Add to Queue</button>
                <button id="pauseQueueBtn" class="secondary">⏸ Pause Queue</button>
                <button id="resumeQueueBtn" class="secondary">▶ Resume</button>
            </div>
        </div>
    </div>

    <!-- Queue Manager Tab -->
    <div id="queueViewTab" class="tab-content">
        <div class="glass-panel">
            <h3>📋 Download Queue (sequential)</h3>
            <div id="queueContainer" class="queue-list"></div>
        </div>
    </div>

    <!-- File Manager Tab -->
    <div id="filesTab" class="tab-content">
        <div class="glass-panel">
            <h3>🗂 Downloaded Media</h3>
            <div id="videoGrid" class="video-grid"></div>
        </div>
    </div>

    <!-- System Status Tab -->
    <div id="statusTab" class="tab-content">
        <div class="glass-panel">
            <h3>📈 Real-time Metrics</h3>
            <div class="stats-grid">
                <div class="stat-box">🧠 CPU <br><span id="statCpu">--</span></div>
                <div class="stat-box">💾 RAM <br><span id="statRam">--</span></div>
                <div class="stat-box">💽 Disk <br><span id="statDisk">--</span></div>
                <div class="stat-box">⏳ Queue <br><span id="statQueue">--</span></div>
            </div>
        </div>
    </div>

    <!-- Media Player Tab -->
    <div id="playerTab" class="tab-content">
        <div class="glass-panel">
            <h3>🎬 Premium Player</h3>
            <div id="playerContainer">
                <video controls style="width:100%; border-radius:16px;">
                    <source src="" type="video/mp4">
                    Your browser does not support the video tag.
                </video>
            </div>
            <p style="margin-top:12px;">Select a video from File Manager to play here.</p>
        </div>
    </div>

    <script>
        // Single download using queue system (adds to queue)
        document.getElementById("singleDownloadBtn").onclick = async () => {
            const url = document.getElementById("singleUrl").value.trim();
            if(!url) return;
            const res = await fetch('/api/queue/add', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify({urls:[url]})});
            if(res.ok) {
                document.getElementById("singleStatus").innerHTML = "✅ Added to queue";
                setTimeout(()=>document.getElementById("singleStatus").innerHTML="", 2000);
                document.getElementById("singleUrl").value = "";
                fetchQueue();
                switchTab('queueView');
            } else alert("Error adding URL");
        };
        // refresh queue after socket events
        window.fetchQueue = fetchQueue;
        window.removeItem = removeItem;
        window.retryItem = retryItem;
        window.deleteFile = deleteFile;
        window.playVideo = playVideo;
        window.switchTab = switchTab;
    </script>
</body>
</html>
"""