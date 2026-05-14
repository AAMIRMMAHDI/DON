import os
import threading
import uuid
import time
import logging
from flask import Flask, render_template_string, request, jsonify, send_from_directory
import yt_dlp
from flask_socketio import SocketIO
import urllib.parse
import paramiko

# تنظیم Flask و SocketIO
app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# تنظیم لاگینگ
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ساخت پوشه‌های مورد نیاز
BASE_DIR = os.path.dirname(__file__)
STATIC_DIR = os.path.join(BASE_DIR, 'static')
VIDEO_DIR = os.path.join(STATIC_DIR, 'videos')
CSS_DIR = os.path.join(STATIC_DIR, 'css')
JS_DIR = os.path.join(STATIC_DIR, 'js')
FONTS_DIR = os.path.join(STATIC_DIR, 'fonts')
os.makedirs(VIDEO_DIR, exist_ok=True)
os.makedirs(CSS_DIR, exist_ok=True)
os.makedirs(JS_DIR, exist_ok=True)
os.makedirs(FONTS_DIR, exist_ok=True)

# اطلاعات سرور مقصد
REMOTE_HOST = "185.208.175.180"
REMOTE_USER = "root"
REMOTE_PASS = "Amir.1388"
REMOTE_DIR = "/root/videos/"
PUBLIC_BASE_URL = "http://185.208.175.180/videos/"
UPLOAD_RETRIES = 3
SSH_TIMEOUT = 15

# محتوای فایل‌های استاتیک (بدون تغییر، فقط اضافه شدن event listener ها)
INDEX_HTML = """
<!DOCTYPE html>
<html lang="fa" dir="rtl">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>🎬 وی‌پلیر | سرویس پخش و دانلود ویدیو</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <link rel="stylesheet" href="/static/css/style.css">
    <link href="https://fonts.googleapis.com/css2?family=Vazirmatn:wght@300;400;500;600;700&display=swap" rel="stylesheet">
</head>
<body>
    <!-- Navigation -->
    <nav class="navbar navbar-expand-lg navbar-dark neon-navbar">
        <div class="container-fluid">
            <a class="navbar-brand glow-text" href="#">
                <i class="fas fa-play-circle me-2"></i>
                وی‌پلیر
            </a>
            <div class="navbar-nav ms-auto">
                <span class="nav-text">سرویس حرفه‌ای پخش و دانلود ویدیو</span>
            </div>
        </div>
    </nav>

    <!-- Main Container -->
    <div class="container-fluid main-container px-0">
        <!-- Download Section -->
        <div class="download-section container">
            <div class="section-header">
                <h2 class="glow-text"><i class="fas fa-cloud-download-alt me-2"></i>دانلود ویدیو</h2>
                <div class="header-decoration"></div>
            </div>
            <form id="download-form" class="download-form">
                <div class="neon-input-group">
                    <input type="text" id="url" class="form-control neon-input"
                           placeholder="https://www.youtube.com/watch?v=...">
                    <button type="submit" class="btn btn-primary neon-btn">
                        <i class="fas fa-download me-2"></i>
                        دانلود
                    </button>
                </div>
            </form>

            <!-- Progress Section -->
            <div class="progress-section">
                <div class="progress-info">
                    <span id="status" class="status-text">آماده برای دانلود</span>
                    <span id="progress-text" class="progress-text">0%</span>
                </div>
                <div class="progress neon-progress">
                    <div class="progress-bar" id="progress-bar" role="progressbar" style="width: 0%" aria-valuenow="0" aria-valuemin="0" aria-valuemax="100"></div>
                </div>
            </div>

            <div class="controls">
                <button id="stop-btn" class="btn btn-danger neon-btn-danger" disabled>
                    <i class="fas fa-stop me-2"></i>
                    توقف دانلود
                </button>
            </div>
        </div>

        <!-- Stats Section -->
        <div class="stats-section container">
            <div class="row justify-content-center">
                <div class="col-md-3 col-sm-6">
                    <div class="stat-card">
                        <div class="stat-icon">
                            <i class="fas fa-hdd"></i>
                        </div>
                        <div class="stat-info">
                            <h5 id="total-videos">0</h5>
                            <span>تعداد ویدیوها</span>
                        </div>
                    </div>
                </div>
                <div class="col-md-3 col-sm-6">
                    <div class="stat-card">
                        <div class="stat-icon">
                            <i class="fas fa-bolt"></i>
                        </div>
                        <div class="stat-info">
                            <h5 id="active-downloads">0</h5>
                            <span>دانلود فعال</span>
                        </div>
                    </div>
                </div>
                <div class="col-md-3 col-sm-6">
                    <div class="stat-card">
                        <div class="stat-icon">
                            <i class="fas fa-check-circle"></i>
                        </div>
                        <div class="stat-info">
                            <h5>آنلاین</h5>
                            <span>وضعیت سرویس</span>
                        </div>
                    </div>
                </div>
            </div>
        </div>

        <!-- Player Section -->
        <div class="player-section container">
            <div class="section-header">
                <h2 class="glow-text"><i class="fas fa-play-circle me-2"></i>پخش ویدیو</h2>
                <div class="header-decoration"></div>
            </div>

            <div class="video-player-container">
                <div class="custom-player">
                    <video id="player" class="video-element" controls></video>
                    <div class="custom-controls">
                        <button class="control-btn" id="play-pause">
                            <i class="fas fa-play"></i>
                        </button>
                        <div class="progress-container">
                            <div class="progress-bar-container">
                                <div id="progress-bar-video" class="progress-bar-video"></div>
                            </div>
                        </div>
                        <div class="time-display">
                            <span id="current-time">00:00</span> /
                            <span id="duration">00:00</span>
                        </div>
                        <button class="control-btn" id="volume-btn">
                            <i class="fas fa-volume-up"></i>
                        </button>
                        <input type="range" class="volume-slider" id="volume-slider" min="0" max="100" value="100">
                        <button class="control-btn" id="fullscreen-btn">
                            <i class="fas fa-expand"></i>
                        </button>
                    </div>
                </div>

                <div class="player-actions">
                    <button class="btn btn-outline-primary neon-btn-outline" id="close-player">
                        <i class="fas fa-times me-2"></i>
                        بستن پخش کننده
                    </button>
                </div>
            </div>
        </div>

        <!-- Video List Section -->
        <div class="video-list-section container">
            <div class="sidebar-header">
                <h4 class="glow-text"><i class="fas fa-download me-2"></i>ویدیوهای دانلود شده</h4>
                <button class="btn btn-sm btn-outline-primary refresh-btn" id="refresh-btn">
                    <i class="fas fa-sync-alt"></i>
                </button>
            </div>
            <div class="video-strip-container">
                <div id="video-strip" class="video-strip"></div>
            </div>
        </div>
    </div>

    <!-- Footer -->
    <footer class="footer">
        <div class="container-fluid">
            <p>&copy; 2024 وی‌پلیر - سرویس حرفه‌ای پخش و دانلود ویدیو</p>
        </div>
    </footer>

    <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0/dist/js/bootstrap.bundle.min.js"></script>
    <script src="https://cdnjs.cloudflare.com/ajax/libs/socket.io/4.5.0/socket.io.js"></script>
    <script src="/static/js/script.js"></script>
</body>
</html>
"""

STYLE_CSS = """
:root {
    --neon-blue: #00f3ff;
    --neon-purple: #9d00ff;
    --neon-pink: #ff00ff;
    --neon-green: #00ff88;
    --dark-bg: #0a0a0f;
    --darker-bg: #050508;
    --card-bg: rgba(255, 255, 255, 0.05);
    --glass-bg: rgba(255, 255, 255, 0.1);
    --text-primary: #ffffff;
    --text-secondary: #b0b0b0;
}

* {
    margin: 0;
    padding: 0;
    box-sizing: border-box;
}

body {
    font-family: 'Vazirmatn', sans-serif;
    background: linear-gradient(135deg, var(--dark-bg) 0%, var(--darker-bg) 100%);
    color: var(--text-primary);
    min-height: 100vh;
    overflow-x: hidden;
}

/* Navigation */
.neon-navbar {
    background: rgba(10, 10, 15, 0.95) !important;
    backdrop-filter: blur(20px);
    border-bottom: 1px solid rgba(0, 243, 255, 0.3);
    box-shadow: 0 0 20px rgba(0, 243, 255, 0.2);
    padding: 1rem 0;
}

.navbar-brand {
    font-size: 1.8rem;
    font-weight: 700;
    background: linear-gradient(45deg, var(--neon-blue), var(--neon-purple));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
}

.nav-text {
    color: var(--text-secondary);
    font-size: 0.9rem;
}

/* Main Layout */
.main-container {
    min-height: calc(100vh - 120px);
}

/* Sections General */
.download-section, .stats-section, .player-section, .video-list-section {
    background: var(--card-bg);
    backdrop-filter: blur(20px);
    border-radius: 20px;
    border: 1px solid rgba(255, 255, 255, 0.1);
    box-shadow: 0 8px 32px rgba(0, 0, 0, 0.3);
    margin: 2rem auto;
    padding: 2rem;
    max-width: 1400px;
}

.section-header {
    margin-bottom: 2rem;
    text-align: center;
}

.glow-text {
    background: linear-gradient(45deg, var(--neon-blue), var(--neon-purple));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    font-weight: 600;
}

.header-decoration {
    width: 100px;
    height: 3px;
    background: linear-gradient(90deg, var(--neon-blue), var(--neon-purple));
    margin: 0.5rem auto;
    border-radius: 2px;
}

/* Download Section */
.download-form {
    display: flex;
    justify-content: center;
}

.neon-input-group {
    display: flex;
    max-width: 800px;
    width: 100%;
    margin: 0 auto;
}

.neon-input {
    background: rgba(255, 255, 255, 0.05) !important;
    border: 1px solid rgba(0, 243, 255, 0.3) !important;
    color: var(--text-primary) !important;
    border-radius: 12px 0 0 12px !important;
    padding: 1rem;
    transition: all 0.3s ease;
}

.neon-input:focus {
    border-color: var(--neon-blue) !important;
    box-shadow: 0 0 20px rgba(0, 243, 255, 0.3) !important;
    background: rgba(255, 255, 255, 0.08) !important;
}

.neon-btn {
    background: linear-gradient(45deg, var(--neon-blue), var(--neon-purple));
    border: none;
    border-radius: 0 12px 12px 0;
    padding: 1rem 2rem;
    font-weight: 600;
    transition: all 0.3s ease;
    white-space: nowrap;
}

.neon-btn:hover {
    transform: translateY(-2px);
    box-shadow: 0 5px 15px rgba(0, 243, 255, 0.4);
}

.progress-section {
    margin: 2rem 0;
    max-width: 800px;
    margin: 2rem auto;
}

.progress-info {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 0.5rem;
}

.status-text {
    color: var(--neon-green);
    font-weight: 500;
}

.progress-text {
    color: var(--neon-blue);
    font-weight: 600;
}

.neon-progress {
    background: rgba(255, 255, 255, 0.1);
    border-radius: 10px;
    height: 20px;
    overflow: hidden;
    border: 1px solid rgba(0, 243, 255, 0.3);
}

.neon-progress .progress-bar {
    background: linear-gradient(90deg, var(--neon-green), var(--neon-blue));
    transition: width 0.3s ease;
}

.controls {
    text-align: center;
}

.neon-btn-danger {
    background: linear-gradient(45deg, #ff0080, #ff0000);
    border: none;
    border-radius: 12px;
    padding: 1rem 2rem;
    font-weight: 600;
    transition: all 0.3s ease;
}

.neon-btn-danger:hover {
    transform: translateY(-2px);
    box-shadow: 0 5px 15px rgba(255, 0, 128, 0.4);
}

/* Stats Section */
.stats-section .row {
    gap: 1rem;
}

.stat-card {
    background: rgba(255, 255, 255, 0.05);
    border-radius: 15px;
    padding: 1.5rem;
    text-align: center;
    border: 1px solid rgba(255, 255, 255, 0.1);
    transition: all 0.3s ease;
}

.stat-card:hover {
    transform: translateY(-5px);
    border-color: var(--neon-blue);
    box-shadow: 0 10px 25px rgba(0, 243, 255, 0.2);
}

.stat-icon {
    font-size: 2.5rem;
    background: linear-gradient(45deg, var(--neon-blue), var(--neon-purple));
    -webkit-background-clip: text;
    -webkit-text-fill-color: transparent;
    background-clip: text;
    margin-bottom: 1rem;
}

.stat-info h5 {
    font-size: 1.8rem;
    font-weight: 700;
    color: var(--neon-green);
    margin-bottom: 0.5rem;
}

.stat-info span {
    color: var(--text-secondary);
    font-size: 0.9rem;
}

/* Player Section */
.video-player-container {
    max-width: 1200px;
    margin: 0 auto;
}

.custom-player {
    position: relative;
    background: #000;
    border-radius: 15px;
    overflow: hidden;
    box-shadow: 0 10px 30px rgba(0, 0, 0, 0.5);
}

.video-element {
    width: 100%;
    height: auto;
    display: block;
}

.custom-controls {
    position: absolute;
    bottom: 0;
    left: 0;
    right: 0;
    background: linear-gradient(transparent, rgba(0, 0, 0, 0.8));
    padding: 1rem;
    display: flex;
    align-items: center;
    gap: 1rem;
    opacity: 0;
    transition: opacity 0.3s ease;
}

.custom-player:hover .custom-controls {
    opacity: 1;
}

.control-btn {
    background: rgba(255, 255, 255, 0.2);
    border: none;
    color: white;
    width: 40px;
    height: 40px;
    border-radius: 50%;
    display: flex;
    align-items: center;
    justify-content: center;
    cursor: pointer;
    transition: all 0.3s ease;
}

.control-btn:hover {
    background: var(--neon-blue);
    transform: scale(1.1);
}

.progress-container {
    flex: 1;
}

.progress-bar-container {
    background: rgba(255, 255, 255, 0.3);
    height: 6px;
    border-radius: 3px;
    cursor: pointer;
    position: relative;
}

.progress-bar-video {
    background: linear-gradient(90deg, var(--neon-blue), var(--neon-purple));
    height: 100%;
    border-radius: 3px;
    width: 0%;
    transition: width 0.1s ease;
}

.time-display {
    color: white;
    font-size: 0.9rem;
    min-width: 80px;
}

.volume-slider {
    width: 80px;
    cursor: pointer;
}

.player-actions {
    text-align: center;
    margin-top: 1.5rem;
}

.neon-btn-outline {
    border: 2px solid var(--neon-blue);
    color: var(--neon-blue);
    background: transparent;
    border-radius: 12px;
    padding: 0.8rem 1.5rem;
    transition: all 0.3s ease;
}

.neon-btn-outline:hover {
    background: var(--neon-blue);
    color: var(--dark-bg);
}

/* Video List Section */
.sidebar-header {
    display: flex;
    justify-content: space-between;
    align-items: center;
    margin-bottom: 1.5rem;
    padding-bottom: 1rem;
    border-bottom: 1px solid rgba(255, 255, 255, 0.1);
}

.refresh-btn {
    border: 1px solid var(--neon-blue);
    color: var(--neon-blue);
    transition: all 0.3s ease;
}

.refresh-btn:hover {
    background: var(--neon-blue);
    color: var(--dark-bg);
    transform: rotate(180deg);
}

.video-strip-container {
    overflow-x: auto;
    padding-bottom: 1rem;
}

.video-strip {
    display: flex;
    gap: 1rem;
    min-width: max-content;
}

.video-item {
    background: rgba(255, 255, 255, 0.05);
    border: 1px solid rgba(255, 255, 255, 0.1);
    border-radius: 12px;
    padding: 1rem;
    transition: all 0.3s ease;
    cursor: pointer;
    min-width: 300px;
}

.video-item:hover {
    background: rgba(0, 243, 255, 0.1);
    border-color: var(--neon-blue);
    transform: translateY(-5px);
    box-shadow: 0 5px 15px rgba(0, 243, 255, 0.2);
}

.video-title {
    font-size: 0.9rem;
    color: var(--text-primary);
    margin-bottom: 0.5rem;
    line-height: 1.4;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
}

.video-actions {
    display: flex;
    gap: 0.5rem;
    justify-content: center;
}

/* Footer */
.footer {
    background: rgba(10, 10, 15, 0.95);
    backdrop-filter: blur(20px);
    border-top: 1px solid rgba(0, 243, 255, 0.3);
    padding: 1.5rem 0;
    text-align: center;
    color: var(--text-secondary);
}

/* Scrollbar */
::-webkit-scrollbar {
    height: 8px;
    width: 8px;
}

::-webkit-scrollbar-track {
    background: rgba(255, 255, 255, 0.1);
    border-radius: 4px;
}

::-webkit-scrollbar-thumb {
    background: linear-gradient(var(--neon-blue), var(--neon-purple));
    border-radius: 4px;
}

::-webkit-scrollbar-thumb:hover {
    background: linear-gradient(var(--neon-purple), var(--neon-blue));
}

/* Responsive */
@media (max-width: 768px) {
    .download-section, .stats-section, .player-section, .video-list-section {
        margin: 1rem;
        padding: 1.5rem;
    }

    .neon-input-group {
        flex-direction: column;
    }

    .neon-input {
        border-radius: 12px !important;
        margin-bottom: 1rem;
    }

    .neon-btn {
        border-radius: 12px;
    }

    .stat-card {
        margin-bottom: 1rem;
    }

    .video-item {
        min-width: 250px;
    }
}
"""

SCRIPT_JS = """
const socket = io();
let currentSid = null;
let isPlaying = false;
let currentVideoElement = null;

// تنظیم base URL برای VPS
function getBaseUrl() {
    return window.location.origin;
}

// Initialize when page loads
document.addEventListener('DOMContentLoaded', function() {
    updateVideoList();
    initCustomPlayer();

    // Listen for upload events
    socket.on('upload_started', (data) => {
        if (data.sid === currentSid) {
            updateStatus(`آپلود شروع شد: ${data.filename}`, 'info');
            document.getElementById('progress-text').textContent = '0%';
            document.getElementById('progress-bar').style.width = '0%';
        }
    });

    socket.on('upload_progress', (data) => {
        if (data.sid === currentSid) {
            const percent = Math.round(data.percent);
            document.getElementById('progress-bar').style.width = `${percent}%`;
            document.getElementById('progress-text').textContent = `آپلود: ${percent}%`;
            updateStatus(`آپلود: ${data.transferred}/${data.total} بایت`, 'info');
        }
    });

    socket.on('upload_finished', (data) => {
        if (data.sid === currentSid) {
            updateStatus('آپلود با موفقیت کامل شد!', 'success');
            document.getElementById('progress-text').textContent = '100%';
            showNotification(`آپلود کامل شد. لینک: ${data.public_url}`, 'success');
            // Optionally update video list after upload
            updateVideoList();
        }
    });

    socket.on('upload_error', (data) => {
        if (data.sid === currentSid) {
            updateStatus('خطا در آپلود: ' + data.message, 'danger');
            showNotification('خطا در آپلود: ' + data.message, 'error');
        }
    });
});

// ... (بقیه توابع بدون تغییر باقی می‌مانند)
// Initialize custom video player
function initCustomPlayer() {
    currentVideoElement = document.getElementById('player');
    const playPauseBtn = document.getElementById('play-pause');
    const progressBarVideo = document.getElementById('progress-bar-video');
    const volumeBtn = document.getElementById('volume-btn');
    const volumeSlider = document.getElementById('volume-slider');
    const fullscreenBtn = document.getElementById('fullscreen-btn');
    const currentTimeEl = document.getElementById('current-time');
    const durationEl = document.getElementById('duration');

    // Play/Pause
    playPauseBtn.addEventListener('click', togglePlayPause);

    // Progress bar
    currentVideoElement.addEventListener('timeupdate', updateProgressBar);

    // Volume control
    volumeBtn.addEventListener('click', toggleMute);
    volumeSlider.addEventListener('input', setVolume);

    // Fullscreen
    fullscreenBtn.addEventListener('click', toggleFullscreen);

    // Video events
    currentVideoElement.addEventListener('loadedmetadata', function() {
        durationEl.textContent = formatTime(currentVideoElement.duration);
    });

    // Click on progress bar
    document.querySelector('.progress-bar-container').addEventListener('click', function(e) {
        const rect = this.getBoundingClientRect();
        const percent = (e.clientX - rect.left) / rect.width;
        currentVideoElement.currentTime = percent * currentVideoElement.duration;
    });
}

function togglePlayPause() {
    const icon = document.querySelector('#play-pause i');
    if (currentVideoElement.paused) {
        currentVideoElement.play();
        icon.className = 'fas fa-pause';
        isPlaying = true;
    } else {
        currentVideoElement.pause();
        icon.className = 'fas fa-play';
        isPlaying = false;
    }
}

function updateProgressBar() {
    const progress = (currentVideoElement.currentTime / currentVideoElement.duration) * 100;
    document.getElementById('progress-bar-video').style.width = progress + '%';
    document.getElementById('current-time').textContent = formatTime(currentVideoElement.currentTime);
}

function toggleMute() {
    const icon = document.querySelector('#volume-btn i');
    currentVideoElement.muted = !currentVideoElement.muted;
    icon.className = currentVideoElement.muted ? 'fas fa-volume-mute' : 'fas fa-volume-up';
}

function setVolume() {
    currentVideoElement.volume = volumeSlider.value / 100;
    const icon = document.querySelector('#volume-btn i');
    if (currentVideoElement.volume === 0) {
        icon.className = 'fas fa-volume-mute';
    } else if (currentVideoElement.volume < 0.5) {
        icon.className = 'fas fa-volume-down';
    } else {
        icon.className = 'fas fa-volume-up';
    }
}

function toggleFullscreen() {
    const player = document.querySelector('.custom-player');
    if (!document.fullscreenElement) {
        player.requestFullscreen().catch(err => {
            console.log(`Error attempting to enable fullscreen: ${err.message}`);
        });
    } else {
        document.exitFullscreen();
    }
}

function formatTime(seconds) {
    const minutes = Math.floor(seconds / 60);
    seconds = Math.floor(seconds % 60);
    return `${minutes.toString().padStart(2, '0')}:${seconds.toString().padStart(2, '0')}`;
}

// Download form handler
document.getElementById('download-form').addEventListener('submit', async (e) => {
    e.preventDefault();
    const url = document.getElementById('url').value.trim();

    if (!url) {
        showNotification('لطفا لینک ویدیو را وارد کنید', 'warning');
        return;
    }

    console.log('Starting download for URL:', url);
    updateStatus('در حال شروع دانلود...', 'info');
    document.getElementById('active-downloads').textContent = '1';

    try {
        const response = await fetch('/download', {
            method: 'POST',
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
            },
            body: `url=${encodeURIComponent(url)}`
        });

        const data = await response.json();
        if (data.error) {
            showNotification('خطا: ' + data.error, 'error');
            updateStatus('خطا در دانلود', 'danger');
            console.error('Download error:', data.error);
            return;
        }

        currentSid = data.sid;
        document.getElementById('stop-btn').disabled = false;
        updateStatus('در حال دانلود...', 'warning');

    } catch (error) {
        console.error('Network error:', error);
        showNotification('خطای شبکه: ' + error.message, 'error');
        updateStatus('خطای شبکه', 'danger');
    }
});

// Stop download handler
document.getElementById('stop-btn').addEventListener('click', async () => {
    if (currentSid) {
        console.log('Stopping download for SID:', currentSid);
        updateStatus('در حال توقف دانلود...', 'warning');

        try {
            const response = await fetch(`/stop/${currentSid}`, { method: 'POST' });
            const data = await response.json();

            if (data.status) {
                showNotification(data.status, 'success');
                updateStatus('دانلود متوقف شد', 'info');
            } else {
                showNotification(data.error, 'error');
                updateStatus('خطا در توقف', 'danger');
            }

            document.getElementById('stop-btn').disabled = true;
            currentSid = null;
            document.getElementById('active-downloads').textContent = '0';

        } catch (error) {
            console.error('Stop error:', error);
            showNotification('خطا در توقف: ' + error.message, 'error');
        }
    }
});

// Refresh video list
document.getElementById('refresh-btn').addEventListener('click', () => {
    updateVideoList();
    showNotification('لیست ویدیوها بروزرسانی شد', 'success');
});

// Close player
document.getElementById('close-player').addEventListener('click', () => {
    if (currentVideoElement) {
        currentVideoElement.pause();
        currentVideoElement.src = '';
    }
    document.querySelector('#play-pause i').className = 'fas fa-play';
    isPlaying = false;
});

// Socket events
socket.on('progress', (data) => {
    console.log('Progress update:', data);

    if (data.sid === currentSid) {
        const progressBar = document.getElementById('progress-bar');
        const progressText = document.getElementById('progress-text');

        progressBar.style.width = `${data.percent}%`;
        progressText.textContent = `${Math.round(data.percent)}%`;
        progressBar.setAttribute('aria-valuenow', data.percent);

        updateStatus(data.status, 'info');

        if (data.percent === 100) {
            document.getElementById('stop-btn').disabled = true;
            currentSid = null;
            updateStatus('دانلود کامل شد!', 'success');
            document.getElementById('active-downloads').textContent = '0';
            setTimeout(updateVideoList, 2000);
            showNotification('دانلود با موفقیت کامل شد!', 'success');
        }

        if (data.filename) {
            setTimeout(updateVideoList, 1000);
        }
    }
});

socket.on('error', (data) => {
    if (data.sid === currentSid) {
        updateStatus('خطا: ' + data.message, 'danger');
        document.getElementById('stop-btn').disabled = true;
        currentSid = null;
        document.getElementById('active-downloads').textContent = '0';
        showNotification('خطا در دانلود: ' + data.message, 'error');
    }
});

// Update video list
async function updateVideoList() {
    console.log('Updating video list');
    try {
        const response = await fetch('/videos');
        const videos = await response.json();
        console.log('Videos fetched:', videos);

        const videoStrip = document.getElementById('video-strip');
        const totalVideos = document.getElementById('total-videos');

        videoStrip.innerHTML = '';
        totalVideos.textContent = videos.length;

        if (videos.length === 0) {
            videoStrip.innerHTML = `
                <div class="video-item text-center">
                    <div class="text-muted">
                        <i class="fas fa-video-slash fa-2x mb-2"></i>
                        <p>ویدیویی یافت نشد</p>
                    </div>
                </div>
            `;
            return;
        }

        videos.forEach(video => {
            const div = document.createElement('div');
            div.className = 'video-item';

            const videoUrl = `${getBaseUrl()}${video.path}`;
            const displayName = video.name.length > 30 ? video.name.substring(0, 30) + '...' : video.name;

            div.innerHTML = `
                <div class="video-title">${displayName}</div>
                <div class="video-actions">
                    <button class="btn btn-success btn-sm" onclick="playVideo('${videoUrl}')">
                        <i class="fas fa-play me-1"></i>
                        پخش
                    </button>
                    <button class="btn btn-info btn-sm" onclick="downloadVideo('${videoUrl}', '${video.name}')">
                        <i class="fas fa-download me-1"></i>
                        دانلود
                    </button>
                </div>
            `;
            videoStrip.appendChild(div);
        });
    } catch (error) {
        console.error('Error updating video list:', error);
        showNotification('خطا در بارگذاری لیست ویدیوها', 'error');
    }
}

// Play video function
function playVideo(fullUrl) {
    console.log('Playing video from URL:', fullUrl);
    const timestamp = new Date().getTime();
    const videoUrl = `${fullUrl}?t=${timestamp}`;

    // Reset player state
    if (currentVideoElement) {
        currentVideoElement.pause();
        currentVideoElement.src = '';
    }

    // Set new video source
    currentVideoElement.src = videoUrl;

    // Setup event listeners for this video
    currentVideoElement.onloadstart = () => {
        updateStatus('در حال بارگذاری ویدیو...', 'info');
    };

    currentVideoElement.onloadeddata = () => {
        updateStatus('ویدیو بارگذاری شد', 'success');
        document.querySelector('#play-pause i').className = 'fas fa-play';
    };

    currentVideoElement.oncanplay = () => {
        console.log('Video can play');
        updateStatus('آماده پخش', 'success');
    };

    currentVideoElement.onerror = (e) => {
        console.error('Video error details:', {
            error: currentVideoElement.error,
            networkState: currentVideoElement.networkState,
            readyState: currentVideoElement.readyState
        });

        let errorMessage = 'خطا در بارگذاری ویدیو';
        if (currentVideoElement.error) {
            switch(currentVideoElement.error.code) {
                case currentVideoElement.error.MEDIA_ERR_NETWORK:
                    errorMessage = 'خطای شبکه - ویدیو قابل دسترسی نیست';
                    break;
                case currentVideoElement.error.MEDIA_ERR_DECODE:
                    errorMessage = 'خطای decode - فرمت ویدیو پشتیبانی نمی‌شود';
                    break;
                case currentVideoElement.error.MEDIA_ERR_SRC_NOT_SUPPORTED:
                    errorMessage = 'فرمت ویدیو پشتیبانی نمی‌شود';
                    break;
            }
        }

        updateStatus(errorMessage, 'danger');
        showNotification(errorMessage, 'error');
    };

    // Load video
    currentVideoElement.load();

    // Scroll to player
    document.querySelector('.player-section').scrollIntoView({ behavior: 'smooth' });
}

// Download video function
function downloadVideo(url, filename) {
    console.log('Downloading video:', filename);
    showNotification('در حال دانلود ویدیو...', 'info');

    const a = document.createElement('a');
    a.href = url;
    a.download = filename;
    a.style.display = 'none';
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
}

// Update status function
function updateStatus(message, type = 'info') {
    const statusElement = document.getElementById('status');
    statusElement.textContent = message;

    statusElement.className = 'status-text';

    switch(type) {
        case 'success':
            statusElement.style.color = 'var(--neon-green)';
            break;
        case 'warning':
            statusElement.style.color = 'var(--neon-pink)';
            break;
        case 'danger':
            statusElement.style.color = '#ff4444';
            break;
        default:
            statusElement.style.color = 'var(--neon-blue)';
    }
}

// Notification system
function showNotification(message, type = 'info') {
    const existingNotification = document.querySelector('.custom-notification');
    if (existingNotification) {
        existingNotification.remove();
    }

    const notification = document.createElement('div');
    notification.className = `custom-notification alert alert-${type === 'error' ? 'danger' : type}`;
    notification.style.cssText = `
        position: fixed;
        top: 20px;
        left: 50%;
        transform: translateX(-50%);
        z-index: 9999;
        min-width: 300px;
        text-align: center;
        border-radius: 10px;
        backdrop-filter: blur(20px);
        border: 1px solid rgba(255,255,255,0.2);
    `;

    notification.innerHTML = `
        <div class="d-flex align-items-center justify-content-center">
            <i class="fas fa-${getNotificationIcon(type)} me-2"></i>
            <span>${message}</span>
        </div>
    `;

    document.body.appendChild(notification);

    setTimeout(() => {
        if (notification.parentNode) {
            notification.parentNode.removeChild(notification);
        }
    }, 5000);
}

function getNotificationIcon(type) {
    switch(type) {
        case 'success': return 'check-circle';
        case 'warning': return 'exclamation-triangle';
        case 'error': return 'exclamation-circle';
        default: return 'info-circle';
    }
}
"""

# ذخیره فایل‌های استاتیک
with open(os.path.join(CSS_DIR, 'style.css'), 'w', encoding='utf-8') as f:
    f.write(STYLE_CSS)
with open(os.path.join(JS_DIR, 'script.js'), 'w', encoding='utf-8') as f:
    f.write(SCRIPT_JS)

# وضعیت دانلودها
download_status = {}

def sanitize_filename(filename):
    """فقط کاراکترهای مجاز در نام فایل باقی بمانند."""
    import re
    name, ext = os.path.splitext(filename)
    name = re.sub(r'[^\w\-_]', '_', name)
    return name + ext

def upload_to_server(file_path, sid):
    """
    آپلود فایل روی سرور ریموت با استفاده از SSH/SFTP.
    پیشرفت و نتیجه از طریق SocketIO به کلاینت ارسال می‌شود.
    """
    if not os.path.exists(file_path):
        logger.error(f"File not found for upload: {file_path}")
        socketio.emit('upload_error', {'sid': sid, 'message': 'فایل محلی یافت نشد'})
        return

    filename = os.path.basename(file_path)
    safe_filename = sanitize_filename(filename)
    remote_path = os.path.join(REMOTE_DIR, safe_filename).replace("\\", "/")  # سازگار با لینوکس

    # ایجاد کلاینت SSH
    ssh = paramiko.SSHClient()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    last_exception = None
    for attempt in range(1, UPLOAD_RETRIES + 1):
        try:
            logger.info(f"SSH connection attempt {attempt}/{UPLOAD_RETRIES} to {REMOTE_HOST}")
            socketio.emit('upload_started', {'sid': sid, 'filename': safe_filename})

            ssh.connect(
                REMOTE_HOST,
                username=REMOTE_USER,
                password=REMOTE_PASS,
                timeout=SSH_TIMEOUT
            )

            # اطمینان از وجود دایرکتوری ریموت
            logger.info("Ensuring remote directory exists: %s", REMOTE_DIR)
            ssh.exec_command(f'mkdir -p "{REMOTE_DIR}"')
            time.sleep(1)  # Give it a moment

            # باز کردن SFTP
            sftp = ssh.open_sftp()

            # گرفتن حجم فایل برای محاسبه درصد
            file_size = os.path.getsize(file_path)
            transferred = [0]  # استفاده از لیست برای تغییرپذیری در callback

            def progress_callback(transferred_bytes, total_bytes):
                transferred[0] = transferred_bytes
                percent = (transferred_bytes / total_bytes) * 100 if total_bytes else 0
                logger.debug(f"Upload progress: {transferred_bytes}/{total_bytes} ({percent:.1f}%)")
                socketio.emit('upload_progress', {
                    'sid': sid,
                    'percent': percent,
                    'transferred': transferred_bytes,
                    'total': total_bytes
                })

            logger.info(f"Starting SFTP upload: {file_path} -> {remote_path}")
            sftp.put(file_path, remote_path, callback=progress_callback, confirm=True)

            # بستن اتصالات
            sftp.close()
            ssh.close()

            # موفقیت
            public_url = f"{PUBLIC_BASE_URL}{urllib.parse.quote(safe_filename)}"
            logger.info(f"Upload successful. Public URL: {public_url}")
            socketio.emit('upload_finished', {'sid': sid, 'public_url': public_url})
            return  # خروج از تابع بعد از موفقیت

        except Exception as e:
            last_exception = e
            logger.error(f"Upload attempt {attempt} failed: {str(e)}")
            if attempt < UPLOAD_RETRIES:
                wait = 5 * attempt
                logger.info(f"Retrying in {wait} seconds...")
                time.sleep(wait)
            else:
                logger.error("All upload retries exhausted.")
        finally:
            try:
                ssh.close()
            except:
                pass

    # اگر حلقه تمام شد و موفق نشد
    socketio.emit('upload_error', {
        'sid': sid,
        'message': f'آپلود پس از {UPLOAD_RETRIES} بار تلاش ناموفق ماند: {str(last_exception)}'
    })

def progress_hook(d):
    sid = d.get('sid')
    if not sid:
        return

    if d['status'] == 'downloading':
        total = d.get('total_bytes') or d.get('total_bytes_estimate', 0)
        downloaded = d.get('downloaded_bytes', 0)
        if total and total > 0:
            percent = (downloaded / total) * 100
            socketio.emit('progress', {
                'sid': sid,
                'percent': percent,
                'status': f'در حال دانلود: {percent:.1f}%'
            })

    elif d['status'] == 'finished':
        filename = d.get('filename')
        if filename:
            download_status[sid]['filename'] = filename
            download_status[sid]['download_complete'] = True
            logger.info(f"Download finished for SID {sid}: {filename}")
            socketio.emit('progress', {
                'sid': sid,
                'percent': 100,
                'status': 'دانلود کامل شد!',
                'filename': os.path.basename(filename)
            })

    elif d['status'] == 'error':
        socketio.emit('error', {
            'sid': sid,
            'message': d.get('error', 'خطای ناشناخته')
        })

def download_video(url, sid):
    # شروع دانلود
    if download_status.get(sid, {}).get('stop', False):
        socketio.emit('progress', {'sid': sid, 'status': 'دانلود متوقف شد'})
        return

    ydl_opts = {
        'outtmpl': os.path.join(VIDEO_DIR, '%(title).100s.%(ext)s'),
        'format': 'best[height<=720]/best',
        'noplaylist': True,
        'progress_hooks': [lambda d: progress_hook({**d, 'sid': sid})],
        'quiet': False,
        'no_warnings': False,
        'http_chunk_size': 10485760,
    }

    try:
        with yt_dlp.YoutubeDL(ydl_opts) as ydl:
            info = ydl.extract_info(url, download=False)
            socketio.emit('progress', {
                'sid': sid,
                'percent': 0,
                'status': f'آماده سازی: {info.get("title", "Unknown")}'
            })
            ydl.download([url])

        # بعد از پایان دانلود (بدون خطا)، اگر کامل شده باشد و توقف نشده باشد
        if download_status.get(sid, {}).get('download_complete') and not download_status.get(sid, {}).get('stop'):
            local_file = download_status[sid].get('filename')
            if local_file and os.path.exists(local_file):
                logger.info(f"Starting upload thread for: {local_file}")
                threading.Thread(target=upload_to_server, args=(local_file, sid), daemon=True).start()
            else:
                logger.error(f"Download marked complete but file missing: {local_file}")
                socketio.emit('upload_error', {'sid': sid, 'message': 'فایل دانلود شده پیدا نشد'})

    except Exception as e:
        error_msg = str(e)
        logger.error(f"Download error SID {sid}: {error_msg}")
        socketio.emit('error', {
            'sid': sid,
            'message': error_msg
        })

@app.route('/')
def index():
    return render_template_string(INDEX_HTML)

@app.route('/download', methods=['POST'])
def start_download():
    url = request.form.get('url', '').strip()
    if not url:
        return jsonify({'error': 'لینک نمی‌تواند خالی باشد!'})

    if not url.startswith(('http://', 'https://')):
        return jsonify({'error': 'لینک نامعتبر است! باید با http:// یا https:// شروع شود.'})

    sid = str(uuid.uuid4())
    download_status[sid] = {'url': url, 'stop': False, 'download_complete': False}

    thread = threading.Thread(target=download_video, args=(url, sid))
    thread.daemon = True
    thread.start()

    return jsonify({'sid': sid, 'status': 'دانلود شروع شد'})

@app.route('/stop/<sid>', methods=['POST'])
def stop_download(sid):
    if sid in download_status:
        download_status[sid]['stop'] = True
        return jsonify({'status': 'درخواست توقف ثبت شد'})
    return jsonify({'error': 'دانلود یافت نشد'})

@app.route('/videos')
def list_videos():
    try:
        videos = []
        for f in os.listdir(VIDEO_DIR):
            file_path = os.path.join(VIDEO_DIR, f)
            if os.path.isfile(file_path) and f.lower().endswith(('.mp4', '.webm', '.mkv', '.avi', '.mov', '.flv')):
                encoded_name = urllib.parse.quote(f)
                videos.append({
                    'name': f,
                    'path': f'/videos/{encoded_name}'
                })
        videos.sort(key=lambda x: os.path.getctime(os.path.join(VIDEO_DIR, x['name'])), reverse=True)
        return jsonify(videos)
    except Exception as e:
        logger.error(f"Error listing videos: {e}")
        return jsonify([])

@app.route('/videos/<filename>')
def serve_video(filename):
    try:
        decoded_filename = urllib.parse.unquote(filename)
        file_path = os.path.join(VIDEO_DIR, decoded_filename)

        if not os.path.isfile(file_path):
            return "ویدیو یافت نشد", 404

        response = send_from_directory(VIDEO_DIR, decoded_filename)
        response.headers.add('Access-Control-Allow-Origin', '*')
        response.headers.add('Accept-Ranges', 'bytes')

        return response

    except Exception as e:
        logger.error(f"Error serving video {filename}: {e}")
        return "خطا در سرویس دهی ویدیو", 500

if __name__ == '__main__':
    print("🎬 وی‌پلیر در حال اجرا است...")
    print("🌐 آدرس دسترسی: http://0.0.0.0:5000")
    print("✨ طراحی مدرن با افکت‌های نئونی")
    print("⏹️ برای توقف برنامه Ctrl+C را بفشارید")

    socketio.run(
        app,
        host='0.0.0.0',
        port=5000,
        debug=False,
        allow_unsafe_werkzeug=True
    )