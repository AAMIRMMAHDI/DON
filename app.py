#!/usr/bin/env python3
"""
Enterprise Video Manager – Single‑File Production Platform
Flask + SocketIO + yt‑dlp + Paramiko + Queue System
All frontend assets are served locally – fully offline.
"""

import os, sys, json, uuid, time, threading, queue, re, logging
from pathlib import Path
from datetime import datetime
from collections import deque

import yt_dlp, paramiko
from flask import Flask, render_template_string, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
from dotenv import load_dotenv

load_dotenv()

# ─── Configuration ──────────────────────────────────────────────────────────
class Config:
    BASE_DIR = os.path.abspath(os.path.dirname(__file__))
    STATIC_DIR = os.path.join(BASE_DIR, 'static')
    DOWNLOADS_DIR = os.path.join(BASE_DIR, 'downloads')
    LOGS_DIR = os.path.join(BASE_DIR, 'logs')
    QUEUE_FILE = os.path.join(BASE_DIR, 'queue.json')

    SSH_HOST = os.getenv('SSH_HOST', '185.208.175.180')
    SSH_PORT = int(os.getenv('SSH_PORT', 22))
    SSH_USER = os.getenv('SSH_USER', 'root')
    SSH_PASS = os.getenv('SSH_PASSWORD', 'Amir.1388')
    SSH_REMOTE_PATH = os.getenv('SSH_REMOTE_PATH', '/root/videos/')

    MAX_CONCURRENT_DOWNLOADS = 2  # single download at a time in queue

os.makedirs(Config.DOWNLOADS_DIR, exist_ok=True)
os.makedirs(Config.LOGS_DIR, exist_ok=True)

# ─── Logging Setup ──────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(os.path.join(Config.LOGS_DIR, 'app.log')),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ─── App Initialization ─────────────────────────────────────────────────────
app = Flask(__name__, static_folder='static', static_url_path='/static')
app.config['SECRET_KEY'] = os.urandom(24)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')

# ─── Download Queue & Workers ───────────────────────────────────────────────
download_queue = deque()          # items waiting
active_downloads = {}             # sid -> download info
queue_lock = threading.Lock()
stop_flags = {}                   # sid -> threading.Event()

def load_queue():
    if os.path.exists(Config.QUEUE_FILE):
        try:
            with open(Config.QUEUE_FILE, 'r') as f:
                data = json.load(f)
                for item in data:
                    if item.get('status') not in ('completed', 'failed'):
                        item['status'] = 'waiting'
                        download_queue.append(item)
                return data
        except Exception as e:
            logger.error(f"Failed to load queue: {e}")
    return []

def save_queue():
    with queue_lock:
        all_items = list(download_queue) + list(active_downloads.values())
        # also include completed/failed? We'll save full history in file
        # For simplicity, we store all known items from queue + active
        try:
            # gather all history from a global list (we'll maintain a history list)
            pass
        except:
            pass

# Maintain a full history list
queue_history = []
queue_history_lock = threading.Lock()

def add_to_history(item):
    with queue_history_lock:
        queue_history.append(item)
        # persist to disk
        try:
            with open(Config.QUEUE_FILE, 'w') as f:
                json.dump(queue_history, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save queue history: {e}")

def update_history(sid, updates):
    with queue_history_lock:
        for item in queue_history:
            if item['id'] == sid:
                item.update(updates)
                break
        # persist
        try:
            with open(Config.QUEUE_FILE, 'w') as f:
                json.dump(queue_history, f, indent=2)
        except Exception as e:
            logger.error(f"Could not save queue history: {e}")

# ─── SSH Upload Service ─────────────────────────────────────────────────────
def ssh_upload(local_path, filename, sid):
    """Upload file to remote server using SFTP."""
    try:
        transport = paramiko.Transport((Config.SSH_HOST, Config.SSH_PORT))
        transport.connect(username=Config.SSH_USER, password=Config.SSH_PASS)
        sftp = paramiko.SFTPClient.from_transport(transport)

        remote_path = os.path.join(Config.SSH_REMOTE_PATH, filename).replace('\\', '/')
        sftp.put(local_path, remote_path, callback=lambda transferred, total: socketio.emit('upload_progress', {
            'sid': sid,
            'percent': int(transferred / total * 100) if total else 0
        }))

        sftp.close()
        transport.close()
        return True, remote_path
    except Exception as e:
        logger.error(f"SSH upload failed: {e}")
        return False, str(e)

# ─── Download Function (yt‑dlp) ────────────────────────────────────────────
def download_video(url, sid, quality='best', format_id=None):
    """Perform the actual download with progress reporting."""
    stop_event = stop_flags.get(sid, threading.Event())
    if stop_event.is_set():
        return

    outtmpl = os.path.join(Config.DOWNLOADS_DIR, '%(title).100s.%(ext)s')
    ydl_opts = {
        'outtmpl': outtmpl,
        'format': format_id or quality,
        'noplaylist': True,
        'progress_hooks': [lambda d: progress_hook(d, sid)],
        'quiet': False,
        'no_warnings': False,
        'http_chunk_size': 10485760,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            filename = ydl.prepare_filename(info)
            update_history(sid, {
                'filename': os.path.basename(filename),
                'title': info.get('title', ''),
                'filesize': info.get('filesize_approx', 0),
                'status': 'downloading',
                'progress': 0,
                'speed': '',
                'eta': ''
            })

            socketio.emit('progress', {
                'sid': sid,
                'percent': 0,
                'status': 'downloading',
                'filename': os.path.basename(filename),
                'speed': '',
                'eta': ''
            })

            ydl.download([url])
            # After download, get final filename (may have been renamed)
            actual_file = filename
            if not os.path.exists(actual_file):
                # try to find by partial match
                possible_files = [f for f in os.listdir(Config.DOWNLOADS_DIR) if f.startswith(os.path.splitext(os.path.basename(filename))[0])]
                if possible_files:
                    actual_file = os.path.join(Config.DOWNLOADS_DIR, possible_files[0])
                else:
                    raise Exception("Download completed but file not found.")

            # Mark complete
            update_history(sid, {'status': 'completed', 'progress': 100, 'local_path': actual_file})

            socketio.emit('progress', {
                'sid': sid,
                'percent': 100,
                'status': 'completed',
                'filename': os.path.basename(actual_file),
                'speed': '',
                'eta': ''
            })

            # Start SSH upload
            socketio.emit('upload_status', {'sid': sid, 'status': 'uploading'})
            update_history(sid, {'status': 'uploading'})
            success, remote = ssh_upload(actual_file, os.path.basename(actual_file), sid)
            if success:
                update_history(sid, {'status': 'uploaded', 'remote_path': remote})
                socketio.emit('upload_status', {'sid': sid, 'status': 'uploaded'})
            else:
                update_history(sid, {'status': 'upload_failed', 'error': remote})
                socketio.emit('upload_status', {'sid': sid, 'status': 'upload_failed', 'error': remote})

    except Exception as e:
        logger.error(f"Download error: {e}")
        update_history(sid, {'status': 'failed', 'error': str(e)})
        socketio.emit('error', {'sid': sid, 'message': str(e)})

def progress_hook(d, sid):
    """yt‑dlp progress hook."""
    if d['status'] == 'downloading':
        total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
        downloaded = d.get('downloaded_bytes', 0)
        if total:
            percent = (downloaded / total) * 100
            speed = d.get('speed', 0)
            if speed:
                speed_str = f"{speed/1024/1024:.1f} MB/s"
            else:
                speed_str = ''
            eta = d.get('eta', '')
            update_history(sid, {'progress': percent, 'speed': speed_str, 'eta': eta})
            socketio.emit('progress', {
                'sid': sid,
                'percent': percent,
                'status': 'downloading',
                'speed': speed_str,
                'eta': str(eta)
            })
    elif d['status'] == 'finished':
        update_history(sid, {'progress': 100})
        socketio.emit('progress', {'sid': sid, 'percent': 100, 'status': 'processing'})

# ─── Queue Worker (background thread) ───────────────────────────────────────
def queue_worker():
    """Continuously process download queue."""
    while True:
        if download_queue and len(active_downloads) < Config.MAX_CONCURRENT_DOWNLOADS:
            with queue_lock:
                if download_queue:
                    item = download_queue.popleft()
                    sid = item['id']
                    stop_flags[sid] = threading.Event()
                    active_downloads[sid] = item
                    update_history(sid, {'status': 'downloading'})
                    threading.Thread(target=download_video, args=(item['url'], sid, item.get('quality', 'best')), daemon=True).start()
        time.sleep(1)

# Start worker
worker_thread = threading.Thread(target=queue_worker, daemon=True)
worker_thread.start()

# Load previous queue history
queue_history = load_queue()
# Re‑add waiting items to active queue
for item in queue_history:
    if item['status'] in ('waiting', 'paused'):
        download_queue.append(item)

# ─── Routes ──────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/api/queue/add', methods=['POST'])
def add_to_queue():
    urls_text = request.form.get('urls', '').strip()
    if not urls_text:
        return jsonify({'error': 'No URLs provided'}), 400
    urls = [url.strip() for url in urls_text.splitlines() if url.strip()]
    added = []
    for url in urls:
        sid = str(uuid.uuid4())
        item = {
            'id': sid,
            'url': url,
            'status': 'waiting',
            'progress': 0,
            'filename': '',
            'title': '',
            'filesize': 0,
            'speed': '',
            'eta': '',
            'created_at': datetime.now().isoformat(),
            'quality': 'best'
        }
        download_queue.append(item)
        add_to_history(item)
        added.append(sid)
    return jsonify({'status': 'ok', 'added': len(added)})

@app.route('/api/queue/status')
def queue_status():
    with queue_lock:
        waiting = list(download_queue)
    active = list(active_downloads.values())
    all_items = waiting + active + [i for i in queue_history if i['id'] not in [x['id'] for x in waiting+active]]
    return jsonify(all_items)

@app.route('/api/queue/stop/<sid>', methods=['POST'])
def stop_download(sid):
    if sid in stop_flags:
        stop_flags[sid].set()
        if sid in active_downloads:
            item = active_downloads.pop(sid)
            item['status'] = 'stopped'
            update_history(sid, {'status': 'stopped'})
        return jsonify({'status': 'ok'})
    return jsonify({'error': 'Not found'}), 404

@app.route('/api/videos')
def list_videos():
    videos = []
    for f in os.listdir(Config.DOWNLOADS_DIR):
        path = os.path.join(Config.DOWNLOADS_DIR, f)
        if os.path.isfile(path) and f.lower().endswith(('.mp4', '.webm', '.mkv', '.avi', '.mov', '.flv')):
            videos.append({
                'name': f,
                'path': f'/downloads/{f}',
                'size': os.path.getsize(path),
                'modified': os.path.getmtime(path)
            })
    videos.sort(key=lambda x: x['modified'], reverse=True)
    return jsonify(videos)

@app.route('/downloads/<filename>')
def serve_video(filename):
    return send_from_directory(Config.DOWNLOADS_DIR, filename, conditional=True)

@app.route('/api/system')
def system_status():
    import psutil
    return jsonify({
        'cpu': psutil.cpu_percent(),
        'ram': psutil.virtual_memory().percent,
        'disk': psutil.disk_usage('/').percent,
        'queue_size': len(download_queue),
        'active_downloads': len(active_downloads),
        'uptime': time.time() - psutil.boot_time()
    })

# ─── SocketIO Events ────────────────────────────────────────────────────────
@socketio.on('connect')
def handle_connect():
    emit('connected', {'status': 'ok'})

# ─── HTML Template (embedded) ──────────────────────────────────────────────
HTML_TEMPLATE = r'''
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>وی‌پلیر | Video Manager Enterprise</title>
    <link rel="stylesheet" href="/static/vendor/bootstrap/css/bootstrap.min.css">
    <link rel="stylesheet" href="/static/vendor/fontawesome/css/all.min.css">
    <link rel="stylesheet" href="/static/vendor/fonts/Vazirmatn.css" onerror="this.onerror=null;this.href=''">
    <style>
        {{ CSS | safe }}
    </style>
</head>
<body>
    <div id="app">
        <!-- Navbar -->
        <nav class="navbar">
            <div class="nav-brand">وی‌پلیر</div>
            <div class="nav-stats">
                <span><i class="fas fa-download"></i> <span id="active-count">0</span></span>
                <span><i class="fas fa-server"></i> <span id="sys-status">Online</span></span>
            </div>
        </nav>
        <!-- Tabs -->
        <div class="tabs">
            <button class="tab active" data-tab="single">Single Download</button>
            <button class="tab" data-tab="queue">Queue Download</button>
            <button class="tab" data-tab="files">File Manager</button>
            <button class="tab" data-tab="system">System Status</button>
        </div>
        <!-- Tab contents -->
        <div id="tab-single" class="tab-content active">
            <!-- Single download form -->
            <div class="card">
                <h2>Download Video</h2>
                <form id="single-dl-form">
                    <div class="input-group">
                        <input type="url" id="single-url" placeholder="https://www.youtube.com/watch?v=..." required>
                        <button type="submit" class="btn btn-primary"><i class="fas fa-download"></i> Download</button>
                    </div>
                </form>
                <div id="single-progress" style="display:none;">
                    <div class="progress-bar"><div class="progress-fill" id="single-bar"></div></div>
                    <div class="info"><span id="single-status"></span> - <span id="single-speed"></span> - <span id="single-eta"></span></div>
                    <button id="single-stop" class="btn btn-danger">Stop</button>
                </div>
            </div>
        </div>
        <div id="tab-queue" class="tab-content">
            <div class="card">
                <h2>Queue Download</h2>
                <textarea id="queue-urls" rows="5" placeholder="Enter one URL per line..."></textarea>
                <button id="queue-add" class="btn btn-primary"><i class="fas fa-plus"></i> Add to Queue</button>
                <div id="queue-list"></div>
            </div>
        </div>
        <div id="tab-files" class="tab-content">
            <div class="card">
                <h2>Downloaded Files</h2>
                <div id="file-list"></div>
            </div>
        </div>
        <div id="tab-system" class="tab-content">
            <div class="card">
                <h2>System Status</h2>
                <div id="sys-info"></div>
            </div>
        </div>
    </div>
    <script src="/static/vendor/bootstrap/js/bootstrap.bundle.min.js"></script>
    <script src="/static/vendor/socketio/socket.io.min.js"></script>
    <script>
        {{ JS | safe }}
    </script>
</body>
</html>
'''

# ─── CSS Section (embedded) ─────────────────────────────────────────────────
CSS = '''
:root {
    --bg: #0b0f19;
    --card-bg: rgba(18, 22, 33, 0.8);
    --text: #e2e8f0;
    --primary: #6366f1;
    --secondary: #818cf8;
    --accent: #a78bfa;
    --danger: #ef4444;
    --success: #10b981;
    --glass: rgba(255, 255, 255, 0.05);
    --border: rgba(255, 255, 255, 0.1);
}

* { margin:0; padding:0; box-sizing:border-box; }
body { font-family: 'Vazirmatn', 'Segoe UI', sans-serif; background: var(--bg); color: var(--text); min-height:100vh; }
.navbar { display:flex; justify-content:space-between; align-items:center; padding:1rem 2rem; background: rgba(10,14,23,0.9); backdrop-filter:blur(15px); border-bottom:1px solid var(--border); }
.nav-brand { font-size:1.8rem; font-weight:bold; background: linear-gradient(135deg, var(--primary), var(--accent)); -webkit-background-clip:text; -webkit-text-fill-color:transparent; }
.nav-stats span { margin-left:1.5rem; font-size:0.9rem; }
.tabs { display:flex; gap:0.5rem; padding:1.5rem 2rem 0; }
.tab { padding:0.8rem 1.5rem; background: transparent; border:1px solid var(--border); border-radius:8px 8px 0 0; color: var(--text); cursor:pointer; transition: all 0.3s; }
.tab.active { background: var(--primary); border-color: var(--primary); color: white; }
.tab-content { display:none; padding:2rem; }
.tab-content.active { display:block; }
.card { background: var(--card-bg); backdrop-filter:blur(12px); border:1px solid var(--border); border-radius:16px; padding:2rem; margin-bottom:2rem; }
.input-group { display:flex; gap:1rem; margin:1rem 0; }
input, textarea { flex:1; padding:0.8rem; background: rgba(255,255,255,0.05); border:1px solid var(--border); border-radius:8px; color:var(--text); }
.btn { padding:0.8rem 1.5rem; border:none; border-radius:8px; font-weight:600; cursor:pointer; transition:0.3s; }
.btn-primary { background: var(--primary); color:white; }
.btn-primary:hover { background: var(--secondary); }
.btn-danger { background: var(--danger); color:white; }
.progress-bar { height:8px; background: rgba(255,255,255,0.1); border-radius:4px; overflow:hidden; margin:1rem 0; }
.progress-fill { height:100%; background: linear-gradient(90deg, var(--primary), var(--accent)); width:0%; transition: width 0.3s; }
.info { font-size:0.9rem; opacity:0.8; }
#queue-list, #file-list { margin-top:1.5rem; }
.queue-item, .file-item { display:flex; justify-content:space-between; align-items:center; padding:1rem; background: rgba(255,255,255,0.03); border:1px solid var(--border); border-radius:8px; margin-bottom:0.5rem; }
.status-dot { width:10px; height:10px; border-radius:50%; display:inline-block; margin-left:0.5rem; }
'''

# ─── JavaScript Section (embedded) ──────────────────────────────────────────
JS = '''
const socket = io();
let currentTab = 'single';

// Tab switching
document.querySelectorAll('.tab').forEach(tab => {
    tab.addEventListener('click', () => {
        document.querySelectorAll('.tab').forEach(t => t.classList.remove('active'));
        tab.classList.add('active');
        const id = 'tab-' + tab.dataset.tab;
        document.querySelectorAll('.tab-content').forEach(c => c.classList.remove('active'));
        document.getElementById(id).classList.add('active');
        if (tab.dataset.tab === 'files') loadFiles();
        if (tab.dataset.tab === 'system') loadSystem();
        if (tab.dataset.tab === 'queue') loadQueue();
    });
});

// Single download
document.getElementById('single-dl-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const url = document.getElementById('single-url').value;
    const sid = generateUUID();
    // add to queue as well (single downloads also go through queue)
    socket.emit('add_to_queue', {urls: [url], sid: sid});
    // Show progress
    document.getElementById('single-progress').style.display = 'block';
    // Update UI via socket events later
});

socket.on('progress', (data) => {
    if (data.sid === currentDownloadSid) {
        document.getElementById('single-bar').style.width = data.percent + '%';
        document.getElementById('single-status').textContent = data.status;
        document.getElementById('single-speed').textContent = data.speed;
        document.getElementById('single-eta').textContent = data.eta;
    }
});

// Queue management
document.getElementById('queue-add').addEventListener('click', () => {
    const urls = document.getElementById('queue-urls').value;
    fetch('/api/queue/add', {
        method: 'POST',
        headers: {'Content-Type': 'application/x-www-form-urlencoded'},
        body: 'urls=' + encodeURIComponent(urls)
    }).then(r=>r.json()).then(d=>{ if(d.added) loadQueue(); });
});

function loadQueue() {
    fetch('/api/queue/status').then(r=>r.json()).then(items => {
        const html = items.map(item => `
            <div class="queue-item">
                <span><span class="status-dot" style="background:${getStatusColor(item.status)}"></span>${item.url || item.filename}</span>
                <span>${item.status} ${item.progress ? Math.round(item.progress)+'%' : ''}</span>
                <button class="btn btn-sm btn-danger" onclick="stopQueue('${item.id}')">Stop</button>
            </div>
        `).join('');
        document.getElementById('queue-list').innerHTML = html;
    });
}

function stopQueue(sid) {
    fetch('/api/queue/stop/' + sid, {method:'POST'}).then(()=>loadQueue());
}

function getStatusColor(s) {
    switch(s) {
        case 'downloading': return '#f59e0b';
        case 'completed': case 'uploaded': return '#10b981';
        case 'failed': case 'upload_failed': return '#ef4444';
        default: return '#6b7280';
    }
}

function loadFiles() {
    fetch('/api/videos').then(r=>r.json()).then(files => {
        const html = files.map(f => `
            <div class="file-item">
                <span>${f.name} (${(f.size/1024/1024).toFixed(2)} MB)</span>
                <div>
                    <a href="/downloads/${f.name}" target="_blank" class="btn btn-sm btn-primary">Play</a>
                    <a href="/downloads/${f.name}" download class="btn btn-sm btn-secondary">Download</a>
                </div>
            </div>
        `).join('');
        document.getElementById('file-list').innerHTML = html;
    });
}

function loadSystem() {
    fetch('/api/system').then(r=>r.json()).then(data => {
        document.getElementById('sys-info').innerHTML = `
            CPU: ${data.cpu}%<br>
            RAM: ${data.ram}%<br>
            Disk: ${data.disk}%<br>
            Queue: ${data.queue_size}<br>
            Active: ${data.active_downloads}
        `;
    });
}

function generateUUID() { return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => { const r = Math.random()*16|0, v = c=='x'?r:(r&0x3|0x8); return v.toString(16); }); }
'''

# ─── Main Entry Point ───────────────────────────────────────────────────────
if __name__ == '__main__':
    logger.info("Starting Enterprise Video Manager...")
    socketio.run(app, host='0.0.0.0', port=5000, debug=False, allow_unsafe_werkzeug=True)