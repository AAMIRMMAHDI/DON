import os
import re
import uuid
import threading
import urllib.parse
from flask import Flask, render_template_string, request, jsonify, send_from_directory
from flask_socketio import SocketIO, emit
import yt_dlp

# ====================== تنظیم اولیه ======================
app = Flask(__name__)
app.config['SECRET_KEY'] = os.urandom(24)
socketio = SocketIO(app, cors_allowed_origins="*")

BASE_DIR = os.path.dirname(__file__)
STATIC_DIR = os.path.join(BASE_DIR, 'static')
VIDEO_DIR = os.path.join(STATIC_DIR, 'videos')
CSS_DIR = os.path.join(STATIC_DIR, 'css')
JS_DIR = os.path.join(STATIC_DIR, 'js')
FONTS_DIR = os.path.join(STATIC_DIR, 'fonts')
VENDOR_DIR = os.path.join(JS_DIR, 'vendor')

for d in [VIDEO_DIR, CSS_DIR, JS_DIR, FONTS_DIR, VENDOR_DIR]:
    os.makedirs(d, exist_ok=True)

# ====================== وضعیت دانلود تکی ======================
single_downloads = {}  # {sid: {'stop': bool, 'thread': Thread}}

# ====================== مدیریت صف دانلود ======================
class QueueManager:
    def __init__(self):
        self.queues = {}  # queue_id -> QueueState

    def create_queue(self, urls):
        queue_id = str(uuid.uuid4())
        state = {
            'id': queue_id,
            'urls': urls,
            'current_index': 0,
            'items': [{'url': url, 'status': 'waiting', 'title': '', 'error': None} for url in urls],
            'stop_flag': False,
            'thread': None,
            'total_percent': 0
        }
        self.queues[queue_id] = state
        return queue_id

    def start_queue(self, queue_id):
        state = self.queues.get(queue_id)
        if not state or state['thread'] and state['thread'].is_alive():
            return False
        state['stop_flag'] = False
        thread = threading.Thread(target=self._process_queue, args=(queue_id,))
        thread.daemon = True
        state['thread'] = thread
        thread.start()
        return True

    def stop_queue(self, queue_id):
        state = self.queues.get(queue_id)
        if state:
            state['stop_flag'] = True
            return True
        return False

    def _process_queue(self, queue_id):
        state = self.queues[queue_id]
        total = len(state['urls'])
        completed = 0
        failed = 0

        for idx, url in enumerate(state['urls']):
            if state['stop_flag']:
                state['items'][idx]['status'] = 'stopped'
                break
            if idx < state['current_index']:
                continue
            state['current_index'] = idx
            state['items'][idx]['status'] = 'downloading'

            # اطلاع‌رسانی وضعیت آیتم
            socketio.emit('queue_item_update', {
                'queue_id': queue_id,
                'index': idx,
                'status': 'downloading',
                'title': state['items'][idx]['title']
            })

            # دانلود با پشتیبانی از توقف
            def stop_flag():
                return state['stop_flag']

            try:
                filename = download_single(url, stop_flag=stop_flag, sid_prefix=f"queue_{queue_id}_{idx}")
                if filename:
                    state['items'][idx]['status'] = 'done'
                    completed += 1
                    state['items'][idx]['title'] = filename
                else:
                    state['items'][idx]['status'] = 'failed'
                    failed += 1
            except Exception as e:
                state['items'][idx]['status'] = 'failed'
                state['items'][idx]['error'] = str(e)
                failed += 1

            # ارسال پیشرفت کلی
            overall_percent = ((completed + failed) / total) * 100
            state['total_percent'] = overall_percent
            socketio.emit('queue_progress', {
                'queue_id': queue_id,
                'completed': completed,
                'failed': failed,
                'total': total,
                'percent': overall_percent,
                'current_item': idx
            })

        # پایان صف
        final_status = 'finished' if not state['stop_flag'] else 'stopped'
        socketio.emit('queue_finished', {
            'queue_id': queue_id,
            'status': final_status,
            'completed': completed,
            'failed': failed
        })
        # پاک کردن صف از حافظه (اختیاری)
        del self.queues[queue_id]

queue_manager = QueueManager()

# ====================== تابع دانلود مشترک ======================
def download_single(url, stop_flag=None, sid_prefix=None):
    """
    دانلود یک ویدیو با قابلیت توقف.
    اگر stop_flag برگرداند True، دانلود قطع می‌شود.
    """
    def progress_hook(d):
        if stop_flag and stop_flag():
            raise Exception("CANCELLED")
        if d['status'] == 'downloading':
            total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
            downloaded = d.get('downloaded_bytes', 0)
            if total and total > 0:
                percent = (downloaded / total) * 100
                socketio.emit('download_progress', {
                    'sid': sid_prefix or 'single',
                    'percent': percent,
                    'status': f'در حال دانلود: {percent:.1f}%'
                })
        elif d['status'] == 'finished':
            socketio.emit('download_progress', {
                'sid': sid_prefix or 'single',
                'percent': 100,
                'status': 'کامل شد',
                'filename': os.path.basename(d['filename'])
            })

    ydl_opts = {
        'outtmpl': os.path.join(VIDEO_DIR, '%(title).100s.%(ext)s'),
        'format': 'best[height<=720]/best',
        'noplaylist': True,
        'progress_hooks': [progress_hook],
        'quiet': True,
        'no_warnings': True
    }
    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            title = info.get('title', 'unknown')
            socketio.emit('download_progress', {
                'sid': sid_prefix or 'single',
                'percent': 0,
                'status': f'آماده‌سازی: {title}'
            })
            ydl.download([url])
        return True
    except Exception as e:
        if 'CANCELLED' in str(e):
            socketio.emit('download_progress', {
                'sid': sid_prefix or 'single',
                'percent': 0,
                'status': 'متوقف شد'
            })
        else:
            socketio.emit('download_error', {
                'sid': sid_prefix or 'single',
                'message': str(e)
            })
        return False

# ====================== مسیرهای Flask ======================
@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)

@app.route('/static/<path:filename>')
def serve_static(filename):
    return send_from_directory(STATIC_DIR, filename)

@app.route('/videos')
def list_videos():
    videos = []
    for f in os.listdir(VIDEO_DIR):
        if f.lower().endswith(('.mp4', '.webm', '.mkv', '.avi', '.mov')):
            videos.append({
                'name': f,
                'path': f'/videos/{urllib.parse.quote(f)}'
            })
    videos.sort(key=lambda x: os.path.getctime(os.path.join(VIDEO_DIR, x['name'])), reverse=True)
    return jsonify(videos)

@app.route('/videos/<path:filename>')
def serve_video(filename):
    safe_name = os.path.basename(urllib.parse.unquote(filename))
    return send_from_directory(VIDEO_DIR, safe_name)

# ---------- دانلود تکی ----------
@app.route('/download', methods=['POST'])
def start_download():
    url = request.form.get('url', '').strip()
    if not url or not url.startswith(('http://', 'https://')):
        return jsonify({'error': 'لینک نامعتبر است'})
    sid = str(uuid.uuid4())
    stop_flag = threading.Event()
    single_downloads[sid] = {'stop_flag': stop_flag}

    def download_task():
        def is_stopped():
            return stop_flag.is_set()
        success = download_single(url, stop_flag=is_stopped, sid_prefix=sid)
        single_downloads.pop(sid, None)
        if not success and not stop_flag.is_set():
            socketio.emit('download_error', {'sid': sid, 'message': 'دانلود ناموفق'})

    thread = threading.Thread(target=download_task)
    thread.daemon = True
    thread.start()
    return jsonify({'sid': sid})

@app.route('/stop/<sid>', methods=['POST'])
def stop_download(sid):
    if sid in single_downloads:
        single_downloads[sid]['stop_flag'].set()
        return jsonify({'status': 'توقف درخواست شد'})
    return jsonify({'error': 'یافت نشد'})

# ---------- عملیات صف ----------
@app.route('/queue/create', methods=['POST'])
def create_queue():
    data = request.get_json()
    urls = data.get('urls', [])
    urls = [u.strip() for u in urls if u.strip().startswith(('http://', 'https://'))]
    if not urls:
        return jsonify({'error': 'هیچ لینک معتبری وارد نشده'})
    queue_id = queue_manager.create_queue(urls)
    queue_manager.start_queue(queue_id)
    return jsonify({'queue_id': queue_id})

@app.route('/queue/stop/<queue_id>', methods=['POST'])
def stop_queue(queue_id):
    if queue_manager.stop_queue(queue_id):
        return jsonify({'status': 'توقف صف درخواست شد'})
    return jsonify({'error': 'صف یافت نشد'})

# ====================== HTML, CSS, JS یکپارچه ======================
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>وی‌پلیر حرفه‌ای | دانلود و پخش ویدیو</title>
    <style>
        /* ========== CSS یکپارچه (بدون CDN) ========== */
        * {
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }

        @font-face {
            font-family: 'Vazirmatn';
            src: url('/static/fonts/Vazirmatn-Regular.woff2') format('woff2');
            font-weight: normal;
            font-style: normal;
        }

        @font-face {
            font-family: 'Vazirmatn';
            src: url('/static/fonts/Vazirmatn-Bold.woff2') format('woff2');
            font-weight: bold;
        }

        body {
            font-family: 'Vazirmatn', 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: #0f172a;
            color: #e2e8f0;
            line-height: 1.6;
            padding: 20px;
        }

        .container {
            max-width: 1400px;
            margin: 0 auto;
        }

        /* Header */
        .header {
            text-align: center;
            margin-bottom: 2rem;
        }
        .header h1 {
            font-size: 2.5rem;
            background: linear-gradient(135deg, #38bdf8, #818cf8);
            -webkit-background-clip: text;
            background-clip: text;
            color: transparent;
        }
        .header p {
            color: #94a3b8;
        }

        /* Tabs */
        .tabs {
            display: flex;
            gap: 1rem;
            border-bottom: 1px solid #334155;
            margin-bottom: 2rem;
        }
        .tab-btn {
            background: none;
            border: none;
            padding: 0.75rem 1.5rem;
            font-size: 1rem;
            font-family: inherit;
            color: #94a3b8;
            cursor: pointer;
            transition: all 0.2s;
            border-radius: 8px 8px 0 0;
        }
        .tab-btn.active {
            color: #38bdf8;
            border-bottom: 2px solid #38bdf8;
            background: rgba(56, 189, 248, 0.1);
        }
        .tab-pane {
            display: none;
            animation: fadeIn 0.3s ease;
        }
        .tab-pane.active {
            display: block;
        }
        @keyframes fadeIn {
            from { opacity: 0; transform: translateY(5px);}
            to { opacity: 1; transform: translateY(0);}
        }

        /* Cards */
        .card {
            background: #1e293b;
            border-radius: 20px;
            padding: 1.5rem;
            margin-bottom: 1.5rem;
            box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.3);
        }
        .card-title {
            font-size: 1.25rem;
            margin-bottom: 1rem;
            color: #f1f5f9;
        }

        /* Form */
        .input-group {
            display: flex;
            gap: 0.5rem;
            flex-wrap: wrap;
        }
        .input-group input, .input-group textarea {
            flex: 1;
            padding: 0.75rem;
            background: #0f172a;
            border: 1px solid #334155;
            border-radius: 12px;
            color: #f1f5f9;
            font-family: inherit;
        }
        .input-group textarea {
            min-height: 150px;
            resize: vertical;
        }
        button {
            background: #3b82f6;
            border: none;
            padding: 0.75rem 1.5rem;
            border-radius: 12px;
            color: white;
            font-weight: bold;
            cursor: pointer;
            transition: 0.2s;
        }
        button:hover {
            background: #2563eb;
            transform: translateY(-2px);
        }
        button.danger {
            background: #dc2626;
        }
        button.danger:hover {
            background: #b91c1c;
        }

        /* Progress */
        .progress-wrapper {
            margin-top: 1rem;
        }
        .progress-bar-bg {
            background: #334155;
            border-radius: 10px;
            height: 8px;
            overflow: hidden;
        }
        .progress-fill {
            background: linear-gradient(90deg, #3b82f6, #a855f7);
            width: 0%;
            height: 100%;
            transition: width 0.2s;
        }
        .status-text {
            font-size: 0.875rem;
            margin-top: 0.5rem;
            color: #94a3b8;
        }

        /* Queue items */
        .queue-items {
            max-height: 400px;
            overflow-y: auto;
            margin-top: 1rem;
        }
        .queue-item {
            display: flex;
            justify-content: space-between;
            align-items: center;
            padding: 0.5rem;
            border-bottom: 1px solid #334155;
        }
        .queue-status {
            font-size: 0.75rem;
            padding: 0.25rem 0.5rem;
            border-radius: 20px;
        }
        .status-waiting { background: #475569; }
        .status-downloading { background: #3b82f6; }
        .status-done { background: #10b981; }
        .status-failed { background: #ef4444; }
        .status-stopped { background: #f59e0b; }

        /* Video grid */
        .video-grid {
            display: grid;
            grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
            gap: 1rem;
            margin-top: 1rem;
        }
        .video-card {
            background: #1e293b;
            border-radius: 16px;
            padding: 1rem;
            transition: 0.2s;
        }
        .video-card h4 {
            font-size: 1rem;
            margin-bottom: 0.5rem;
            white-space: nowrap;
            overflow: hidden;
            text-overflow: ellipsis;
        }
        .video-actions {
            display: flex;
            gap: 0.5rem;
            margin-top: 0.5rem;
        }
        .video-actions button {
            padding: 0.4rem 0.8rem;
            font-size: 0.8rem;
        }

        /* Player */
        .player-container {
            position: fixed;
            bottom: 20px;
            right: 20px;
            width: 360px;
            background: #0f172a;
            border-radius: 20px;
            box-shadow: 0 10px 25px -5px rgba(0,0,0,0.5);
            z-index: 1000;
            display: none;
        }
        .player-container.active {
            display: block;
        }
        .player-header {
            display: flex;
            justify-content: space-between;
            padding: 0.5rem 1rem;
            background: #1e293b;
            border-radius: 20px 20px 0 0;
        }
        .player-video {
            width: 100%;
            border-radius: 0 0 20px 20px;
        }
        .close-player {
            cursor: pointer;
        }

        @media (max-width: 640px) {
            .player-container {
                width: 95%;
                right: 2.5%;
                left: 2.5%;
            }
        }
    </style>
    <script src="/static/js/vendor/socket.io.min.js"></script>
    <script>
        // ========== تمام کدهای فرانت (بومی، بدون CDN) ==========
        // این بخش شامل توابع زیر است: مدیریت تب‌ها، دانلود تکی، دانلود صفی، پخش ویدیو، لیست ویدیوها
        // برای حفظ خوانایی، کد فرانت در فایل نهایی به صورت فشرده اما کامل ارائه می‌شود.
        // در ادامه کد کامل و قابل اجرا را قرار می‌دهیم.
    </script>
</head>
<body>
<div class="container">
    <div class="header">
        <h1>🎬 وی‌پلیر حرفه‌ای</h1>
        <p>دانلود و پخش ویدیو از یوتیوب و سایر سایت‌ها — بدون نیاز به اینترنت برای ظاهر</p>
    </div>

    <div class="tabs">
        <button class="tab-btn active" data-tab="single">دانلود تکی</button>
        <button class="tab-btn" data-tab="queue">دانلود صفی</button>
        <button class="tab-btn" data-tab="videos">ویدیوهای من</button>
    </div>

    <!-- دانلود تکی -->
    <div id="single-tab" class="tab-pane active">
        <div class="card">
            <div class="card-title">لینک ویدیو را وارد کنید</div>
            <div class="input-group">
                <input type="text" id="single-url" placeholder="https://www.youtube.com/watch?v=...">
                <button id="single-download-btn">دانلود</button>
            </div>
            <div class="progress-wrapper" id="single-progress" style="display:none;">
                <div class="progress-bar-bg"><div class="progress-fill" id="single-progress-fill"></div></div>
                <div class="status-text" id="single-status"></div>
            </div>
            <button id="single-stop-btn" class="danger" style="display:none; margin-top:1rem;">توقف</button>
        </div>
    </div>

    <!-- دانلود صفی -->
    <div id="queue-tab" class="tab-pane">
        <div class="card">
            <div class="card-title">لیست لینک‌ها (هر لینک در یک خط)</div>
            <textarea id="queue-urls" rows="6" placeholder="https://...&#10;https://..."></textarea>
            <div style="margin-top:1rem; display:flex; gap:1rem;">
                <button id="queue-start-btn">شروع صف</button>
                <button id="queue-stop-btn" class="danger">توقف صف</button>
            </div>
            <div class="progress-wrapper">
                <div class="progress-bar-bg"><div class="progress-fill" id="queue-progress-fill"></div></div>
                <div class="status-text" id="queue-status"></div>
            </div>
            <div id="queue-items-container" class="queue-items"></div>
        </div>
    </div>

    <!-- ویدیوهای ذخیره شده -->
    <div id="videos-tab" class="tab-pane">
        <div class="card">
            <div class="card-title">کتابخانه ویدیوها</div>
            <div id="videos-grid" class="video-grid"></div>
            <button id="refresh-videos" style="margin-top:1rem;">🔄 بروزرسانی</button>
        </div>
    </div>
</div>

<!-- پخش‌کننده شناور -->
<div id="floating-player" class="player-container">
    <div class="player-header">
        <span>در حال پخش</span>
        <span class="close-player" id="close-player">✖</span>
    </div>
    <video id="player-video" class="player-video" controls></video>
</div>

<script>
    // ==================== کد کامل JavaScript (بدون وابستگی خارجی) ====================
    const socket = io();
    let currentSingleSid = null;
    let currentQueueId = null;

    // ---- تابع کمکی برای نمایش نوتیفیکیشن ساده ----
    function showMessage(msg, isError = false) {
        const div = document.createElement('div');
        div.textContent = msg;
        div.style.position = 'fixed';
        div.style.bottom = '20px';
        div.style.left = '20px';
        div.style.backgroundColor = isError ? '#dc2626' : '#10b981';
        div.style.color = 'white';
        div.style.padding = '8px 16px';
        div.style.borderRadius = '20px';
        div.style.zIndex = '9999';
        document.body.appendChild(div);
        setTimeout(() => div.remove(), 3000);
    }

    // ---- مدیریت تب‌ها ----
    document.querySelectorAll('.tab-btn').forEach(btn => {
        btn.addEventListener('click', () => {
            document.querySelectorAll('.tab-btn').forEach(b => b.classList.remove('active'));
            btn.classList.add('active');
            document.querySelectorAll('.tab-pane').forEach(pane => pane.classList.remove('active'));
            document.getElementById(`${btn.dataset.tab}-tab`).classList.add('active');
            if (btn.dataset.tab === 'videos') loadVideos();
        });
    });

    // ---- دانلود تکی ----
    const singleUrl = document.getElementById('single-url');
    const singleDownloadBtn = document.getElementById('single-download-btn');
    const singleStopBtn = document.getElementById('single-stop-btn');
    const singleProgressDiv = document.getElementById('single-progress');
    const singleProgressFill = document.getElementById('single-progress-fill');
    const singleStatus = document.getElementById('single-status');

    singleDownloadBtn.addEventListener('click', async () => {
        const url = singleUrl.value.trim();
        if (!url) return showMessage('لطفاً لینک را وارد کنید', true);
        singleDownloadBtn.disabled = true;
        singleProgressDiv.style.display = 'block';
        singleStopBtn.style.display = 'inline-block';
        singleProgressFill.style.width = '0%';
        singleStatus.textContent = 'در حال شروع...';

        const res = await fetch('/download', {
            method: 'POST',
            headers: {'Content-Type': 'application/x-www-form-urlencoded'},
            body: `url=${encodeURIComponent(url)}`
        });
        const data = await res.json();
        if (data.error) {
            showMessage(data.error, true);
            resetSingleUI();
        } else {
            currentSingleSid = data.sid;
        }
    });

    singleStopBtn.addEventListener('click', async () => {
        if (currentSingleSid) {
            await fetch(`/stop/${currentSingleSid}`, {method: 'POST'});
            showMessage('توقف درخواست شد');
            resetSingleUI();
        }
    });

    function resetSingleUI() {
        singleDownloadBtn.disabled = false;
        singleProgressDiv.style.display = 'none';
        singleStopBtn.style.display = 'none';
        currentSingleSid = null;
    }

    // ---- دانلود صفی ----
    const queueUrlsText = document.getElementById('queue-urls');
    const queueStartBtn = document.getElementById('queue-start-btn');
    const queueStopBtn = document.getElementById('queue-stop-btn');
    const queueProgressFill = document.getElementById('queue-progress-fill');
    const queueStatusSpan = document.getElementById('queue-status');
    const queueItemsContainer = document.getElementById('queue-items-container');

    queueStartBtn.addEventListener('click', async () => {
        const raw = queueUrlsText.value;
        const urls = raw.split('\\n').map(l => l.trim()).filter(l => l.startsWith('http'));
        if (urls.length === 0) return showMessage('حداقل یک لینک معتبر وارد کنید', true);
        const res = await fetch('/queue/create', {
            method: 'POST',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({urls})
        });
        const data = await res.json();
        if (data.error) showMessage(data.error, true);
        else {
            currentQueueId = data.queue_id;
            showMessage(`صف با ${urls.length} آیتم شروع شد`);
            queueStartBtn.disabled = true;
            queueStopBtn.disabled = false;
        }
    });

    queueStopBtn.addEventListener('click', async () => {
        if (currentQueueId) {
            await fetch(`/queue/stop/${currentQueueId}`, {method: 'POST'});
            showMessage('توقف صف درخواست شد');
            queueStartBtn.disabled = false;
            queueStopBtn.disabled = true;
        }
    });

    // ---- رویدادهای Socket.IO ----
    socket.on('download_progress', (data) => {
        if (data.sid === currentSingleSid) {
            singleProgressFill.style.width = `${data.percent}%`;
            singleStatus.textContent = data.status;
            if (data.percent === 100) {
                resetSingleUI();
                loadVideos();
                showMessage('دانلود کامل شد!');
            }
        }
    });

    socket.on('download_error', (data) => {
        if (data.sid === currentSingleSid) {
            showMessage('خطا: ' + data.message, true);
            resetSingleUI();
        }
    });

    socket.on('queue_item_update', (data) => {
        if (data.queue_id !== currentQueueId) return;
        updateQueueUI();
    });

    socket.on('queue_progress', (data) => {
        if (data.queue_id !== currentQueueId) return;
        queueProgressFill.style.width = `${data.percent}%`;
        queueStatusSpan.textContent = `${data.completed} از ${data.total} تکمیل شد (${data.failed} خطا)`;
        updateQueueUI();
    });

    socket.on('queue_finished', (data) => {
        if (data.queue_id !== currentQueueId) return;
        showMessage(`صف به پایان رسید. موفق: ${data.completed}, ناموفق: ${data.failed}`);
        queueStartBtn.disabled = false;
        queueStopBtn.disabled = true;
        currentQueueId = null;
        loadVideos();
    });

    function updateQueueUI() {
        if (!currentQueueId) return;
        fetch(`/queue/status/${currentQueueId}`)
            .then(res => res.json())
            .then(data => {
                if (data.items) {
                    queueItemsContainer.innerHTML = data.items.map((item, idx) => `
                        <div class="queue-item">
                            <span>${item.title || item.url.substring(0, 40)}</span>
                            <span class="queue-status status-${item.status}">${item.status}</span>
                        </div>
                    `).join('');
                }
            })
            .catch(console.error);
    }

    // ---- ویدیوهای ذخیره شده ----
    async function loadVideos() {
        const res = await fetch('/videos');
        const videos = await res.json();
        const grid = document.getElementById('videos-grid');
        if (videos.length === 0) {
            grid.innerHTML = '<div>هیچ ویدیویی دانلود نشده است.</div>';
            return;
        }
        grid.innerHTML = videos.map(v => `
            <div class="video-card">
                <h4>${v.name.replace(/\.[^/.]+$/, '')}</h4>
                <div class="video-actions">
                    <button onclick="playVideo('${v.path}')">▶ پخش</button>
                    <button onclick="downloadFile('${v.path}', '${v.name}')">⬇ دانلود</button>
                </div>
            </div>
        `).join('');
    }

    window.playVideo = (path) => {
        const playerDiv = document.getElementById('floating-player');
        const videoEl = document.getElementById('player-video');
        videoEl.src = path;
        playerDiv.classList.add('active');
        videoEl.play();
    };
    window.downloadFile = (path, name) => {
        const a = document.createElement('a');
        a.href = path;
        a.download = name;
        a.click();
    };
    document.getElementById('close-player').addEventListener('click', () => {
        document.getElementById('floating-player').classList.remove('active');
        document.getElementById('player-video').pause();
    });
    document.getElementById('refresh-videos').addEventListener('click', loadVideos);

    // بارگذاری اولیه ویدیوها
    loadVideos();

    // اضافه کردن endpoint موقتی برای گرفتن وضعیت صف (برای UI)
    // در مسیرهای Flask باید اضافه شود
</script>
</body>
</html>
"""

# اضافه کردن مسیر وضعیت صف (که در JS استفاده شده)
@app.route('/queue/status/<queue_id>')
def queue_status(queue_id):
    state = queue_manager.queues.get(queue_id)
    if not state:
        return jsonify({'error': 'not found'}), 404
    return jsonify({
        'items': [{'url': item['url'], 'status': item['status'], 'title': item.get('title', '')} for item in state['items']],
        'current_index': state['current_index'],
        'total_percent': state['total_percent']
    })

# ====================== اجرا ======================
if __name__ == '__main__':
    socketio.run(
        app,
        host='0.0.0.0',
        port=5000,
        debug=False,
        allow_unsafe_werkzeug=True
    )
