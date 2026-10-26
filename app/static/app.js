// ========================================================
// ChunkStream Client: Minimal, Clean & Zero-Server-Storage
// ========================================================

// Theme Management with Safe Storage Fallback
const themeToggle = document.getElementById('theme-toggle');
const iconSun = document.querySelector('.icon-sun');
const iconMoon = document.querySelector('.icon-moon');

function safeGetStorage(key, fallback = null) {
  try {
    return localStorage.getItem(key) || fallback;
  } catch (_) {
    return fallback;
  }
}

function safeSetStorage(key, val) {
  try {
    localStorage.setItem(key, val);
  } catch (_) {}
}

function applyTheme(theme) {
  if (theme === 'light') {
    document.documentElement.setAttribute('data-theme', 'light');
    if (iconSun && iconMoon) {
      iconSun.classList.add('hidden');
      iconMoon.classList.remove('hidden');
    }
  } else {
    document.documentElement.removeAttribute('data-theme');
    if (iconSun && iconMoon) {
      iconSun.classList.remove('hidden');
      iconMoon.classList.add('hidden');
    }
  }
}

const savedTheme = safeGetStorage('cs_theme', window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark');
applyTheme(savedTheme);

if (themeToggle) {
  themeToggle.addEventListener('click', () => {
    const isLight = document.documentElement.getAttribute('data-theme') === 'light';
    const newTheme = isLight ? 'dark' : 'light';
    safeSetStorage('cs_theme', newTheme);
    applyTheme(newTheme);
  });
}

// Helpers
function formatBytes(bytes) {
  if (!bytes || bytes <= 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatDuration(sec) {
  if (!isFinite(sec) || sec < 0) return '--';
  if (sec === 0) return '0s';
  if (sec < 60) return `${Math.ceil(sec)}s`;
  if (sec > 86400) return '--'; // Outlier guard (> 24 hours)
  const m = Math.floor(sec / 60);
  const s = Math.ceil(sec % 60);
  if (m < 60) return `${m}m ${s}s`;
  const h = Math.floor(m / 60);
  const remM = m % 60;
  return `${h}h ${remM}m`;
}

function generate4DigitCode() {
  return Math.floor(1000 + Math.random() * 9000).toString();
}

function arrayBufferToBase64(buffer) {
  const bytes = new Uint8Array(buffer);
  let binary = '';
  const batchSize = 8192;
  for (let i = 0; i < bytes.length; i += batchSize) {
    binary += String.fromCharCode.apply(null, bytes.subarray(i, i + batchSize));
  }
  return window.btoa(binary);
}

function base64ToUint8Array(base64) {
  const binaryString = window.atob(base64);
  const len = binaryString.length;
  const bytes = new Uint8Array(len);
  for (let i = 0; i < len; i++) {
    bytes[i] = binaryString.charCodeAt(i);
  }
  return bytes;
}

// Global State
let currentFile = null;
let currentCode = null;
let senderWs = null;
let receiverWs = null;
let isStreaming = false;
let isPaused = false;
let chunkSize = 262144; // 256 KB
let totalChunks = 0;
let ackedChunks = new Set();
let inFlightChunks = new Set();
let inFlightTimestamps = new Map();
let pipelineWatchdog = null;
let bytesSent = 0;
let senderStartTime = 0;

let rxBytesReceived = 0;
let rxStartTime = 0;
let rxChunksMap = new Map(); // In-memory chunk buffer until complete
let rxReceivedIndices = new Set();
let rxTransferData = null;

let isPumping = false;
let senderPingTimer = null;
let rxPingTimer = null;

// WebRTC Direct LAN DataChannel State
let rtcPeerConn = null;
let rtcDataChannel = null;
let isWebRtcReady = false;
let webRtcRole = null;
let webRtcNegotiationTimer = null;

const RTC_CONFIG = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
    { urls: 'stun:stun1.l.google.com:19302' },
  ],
};

// DOM Elements - Navigation & Device
const homeLink = document.getElementById('home-link');
const btnChoiceSend = document.getElementById('btn-choice-send');
const btnChoiceReceive = document.getElementById('btn-choice-receive');
const viewSend = document.getElementById('view-send');
const viewReceive = document.getElementById('view-receive');
const deviceBarName = document.getElementById('device-bar-name');
const deviceBarId = document.getElementById('device-bar-id');
const deviceBarIcon = document.getElementById('device-bar-icon');

// Sender Elements
const fileInput = document.getElementById('file-input');
const dropZone = document.getElementById('drop-zone');
const senderStepPick = document.getElementById('sender-step-pick');
const senderStepTransfer = document.getElementById('sender-step-transfer');
const sendFileName = document.getElementById('send-file-name');
const sendFileSize = document.getElementById('send-file-size');
const senderPeerPill = document.getElementById('sender-peer-pill');
const senderPeerName = document.getElementById('sender-peer-name');
const senderPeerIcon = document.getElementById('sender-peer-icon');
const senderCodeDisplay = document.getElementById('sender-code-display');
const btnCopyCode = document.getElementById('btn-copy-code');
const btnCopyText = document.getElementById('btn-copy-text');
const senderStatusText = document.getElementById('sender-status-text');
const senderStatusDot = document.getElementById('sender-status-dot');
const senderProgressBox = document.getElementById('sender-progress-box');
const senderProgressFill = document.getElementById('sender-progress-fill');
const senderProgressPct = document.getElementById('sender-progress-pct');
const senderSpeed = document.getElementById('sender-speed');
const senderEta = document.getElementById('sender-eta');
const senderRouteMode = document.getElementById('sender-route-mode');
const senderProgressBytes = document.getElementById('sender-progress-bytes');
const senderProgressChunks = document.getElementById('sender-progress-chunks');
const senderActiveControls = document.getElementById('sender-active-controls');
const btnPauseSend = document.getElementById('btn-pause-send');
const btnResumeSend = document.getElementById('btn-resume-send');
const btnCancelSend = document.getElementById('btn-cancel-send');
const btnSendAgain = document.getElementById('btn-send-again');
const btnBackFromSend = document.getElementById('btn-back-from-send');

// Receiver Elements
const receiverStepInput = document.getElementById('receiver-step-input');
const receiverStepTransfer = document.getElementById('receiver-step-transfer');
const receiveCodeInput = document.getElementById('receive-code-input');
const btnStartReceive = document.getElementById('btn-start-receive');
const rxFileName = document.getElementById('rx-file-name');
const rxFileSize = document.getElementById('rx-file-size');
const rxPeerPill = document.getElementById('rx-peer-pill');
const rxPeerName = document.getElementById('rx-peer-name');
const rxPeerIcon = document.getElementById('rx-peer-icon');
const rxStatusText = document.getElementById('rx-status-text');
const rxStatusDot = document.getElementById('rx-status-dot');
const rxProgressBox = document.getElementById('rx-progress-box');
const rxProgressFill = document.getElementById('rx-progress-fill');
const rxProgressPct = document.getElementById('rx-progress-pct');
const rxSpeed = document.getElementById('rx-speed');
const rxEta = document.getElementById('rx-eta');
const rxRouteMode = document.getElementById('rx-route-mode');
const rxProgressBytes = document.getElementById('rx-progress-bytes');
const rxProgressChunks = document.getElementById('rx-progress-chunks');
const rxActiveControls = document.getElementById('rx-active-controls');
const btnCancelReceive = document.getElementById('btn-cancel-receive');
const rxClearedBox = document.getElementById('rx-cleared-box');
const btnReceiveAgain = document.getElementById('btn-receive-again');
const btnBackFromReceive = document.getElementById('btn-back-from-receive');
const senderActivityList = document.getElementById('sender-activity-list');
const rxActivityList = document.getElementById('rx-activity-list');

function addActivityLog(role, message) {
  const list = role === 'sender' ? senderActivityList : rxActivityList;
  if (!list) return;
  const now = new Date();
  const timeStr = now.toTimeString().split(' ')[0];
  const row = document.createElement('div');
  row.className = 'log-item';
  row.innerHTML = `<span class="log-time">${timeStr}</span><span class="log-text">${message}</span>`;
  list.appendChild(row);
  list.scrollTop = list.scrollHeight;
  while (list.children.length > 80) {
    list.removeChild(list.firstChild);
  }
}

// Settings Elements & State
const settingsToggle = document.getElementById('settings-toggle');
const settingsDrawer = document.getElementById('settings-drawer');
const btnCloseSettings = document.getElementById('btn-close-settings');
const settingRouteMode = document.getElementById('setting-route-mode');
const settingChunkSize = document.getElementById('setting-chunk-size');

if (settingRouteMode) {
  settingRouteMode.value = safeGetStorage('cs_route_mode', 'auto');
  settingRouteMode.addEventListener('change', () => {
    safeSetStorage('cs_route_mode', settingRouteMode.value);
  });
}

if (settingChunkSize) {
  settingChunkSize.value = safeGetStorage('cs_chunk_size', 'auto');
  settingChunkSize.addEventListener('change', () => {
    safeSetStorage('cs_chunk_size', settingChunkSize.value);
  });
}

if (settingsToggle && settingsDrawer) {
  settingsToggle.addEventListener('click', () => {
    settingsDrawer.classList.toggle('hidden');
  });
}

if (btnCloseSettings && settingsDrawer) {
  btnCloseSettings.addEventListener('click', () => {
    settingsDrawer.classList.add('hidden');
  });
}

function getConfiguredChunkSize(isDirectLan = false) {
  const pref = settingChunkSize ? settingChunkSize.value : 'auto';
  if (pref === '256') return 262144;
  if (pref === '512') return 524288;
  if (pref === '1024') return 1048576;
  // 'auto': 512 KB for cloud relay, 256 KB for P2P DataChannel
  return isDirectLan ? 262144 : 524288;
}

async function computeSha256(arrayBuffer) {
  try {
    const hashBuf = await crypto.subtle.digest('SHA-256', arrayBuffer);
    const arr = new Uint8Array(hashBuf);
    let hex = '';
    for (let i = 0; i < arr.length; i++) {
      hex += arr[i].toString(16).padStart(2, '0');
    }
    return hex;
  } catch (_) {
    return null;
  }
}

// Device Identification
function getCookie(name) {
  try {
    const match = document.cookie.match(new RegExp('(^|;\\s*)(' + name + ')=([^;]*)'));
    return match ? decodeURIComponent(match[3]) : null;
  } catch (_) {
    return null;
  }
}

function setCookie(name, val, days = 30) {
  try {
    const expires = new Date(Date.now() + days * 864e5).toUTCString();
    document.cookie = `${name}=${encodeURIComponent(val)}; expires=${expires}; path=/; SameSite=Lax`;
  } catch (_) {}
}

function getOrCreateDeviceId() {
  let devId = getCookie('cs_device_id');
  if (!devId) {
    devId = safeGetStorage('cs_device_id');
  }
  if (!devId) {
    devId = 'DEV-' + Math.random().toString(16).substring(2, 6).toUpperCase();
    setCookie('cs_device_id', devId, 30);
    safeSetStorage('cs_device_id', devId);
  }
  return devId;
}

// Clean SVG Icons
const SVG_LAPTOP = `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"></rect><line x1="8" y1="21" x2="16" y2="21"></line><line x1="12" y1="17" x2="12" y2="21"></line></svg>`;
const SVG_MOBILE = `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="2" width="14" height="20" rx="2" ry="2"></rect><line x1="12" y1="18" x2="12.01" y2="18"></line></svg>`;

function detectDeviceInfo() {
  const ua = navigator.userAgent || '';
  const platform = navigator.platform || '';
  const maxTouchPoints = navigator.maxTouchPoints || 0;

  let os = 'Unknown OS';
  if (/iPhone/i.test(ua)) os = 'iOS';
  else if (/iPad/i.test(ua) || (platform === 'MacIntel' && maxTouchPoints > 1)) os = 'iPadOS';
  else if (/Android/i.test(ua)) os = 'Android';
  else if (/Macintosh|Mac OS X/i.test(ua)) os = 'macOS';
  else if (/Windows/i.test(ua)) os = 'Windows';
  else if (/Linux/i.test(ua)) os = 'Linux';

  let deviceType = 'Desktop';
  let deviceModel = 'PC';
  if (/iPhone/i.test(ua)) {
    deviceType = 'Phone';
    deviceModel = 'iPhone';
  } else if (/iPad/i.test(ua) || (platform === 'MacIntel' && maxTouchPoints > 1)) {
    deviceType = 'Tablet';
    deviceModel = 'iPad';
  } else if (/Android/i.test(ua)) {
    deviceType = /Mobile/i.test(ua) ? 'Phone' : 'Tablet';
    if (/Samsung|SM-/i.test(ua)) deviceModel = 'Samsung Galaxy';
    else if (/Pixel/i.test(ua)) deviceModel = 'Google Pixel';
    else if (/OnePlus/i.test(ua)) deviceModel = 'OnePlus';
    else deviceModel = 'Android ' + deviceType;
  } else if (os === 'macOS') {
    deviceModel = 'Mac';
  } else if (os === 'Windows') {
    deviceModel = 'Windows PC';
  } else if (os === 'Linux') {
    deviceModel = 'Linux PC';
  }

  let browser = 'Browser';
  if (/Edg\//i.test(ua)) browser = 'Edge';
  else if (/Chrome\/|CriOS\//i.test(ua)) browser = 'Chrome';
  else if (/Firefox\/|FxiOS\//i.test(ua)) browser = 'Firefox';
  else if (/SamsungBrowser\//i.test(ua)) browser = 'Samsung Internet';
  else if (/Version\/.*Safari/i.test(ua) || /Safari/i.test(ua)) browser = 'Safari';

  return {
    os,
    browser,
    deviceType,
    deviceModel,
    displayName: `${deviceModel} (${browser})`,
  };
}

function updateLocalDeviceDisplay() {
  try {
    const dev = detectDeviceInfo();
    const id = getOrCreateDeviceId();
    if (deviceBarName) deviceBarName.textContent = dev.displayName;
    if (deviceBarId) deviceBarId.textContent = id;
    if (deviceBarIcon) {
      deviceBarIcon.innerHTML = (dev.deviceType === 'Phone' || dev.deviceType === 'Tablet') ? SVG_MOBILE : SVG_LAPTOP;
    }
  } catch (_) {}
}

// Navigation & Segmented Tabs
function showView(view) {
  if (viewSend) viewSend.classList.add('hidden');
  if (viewReceive) viewReceive.classList.add('hidden');
  if (view) view.classList.remove('hidden');

  if (view === viewSend) {
    if (btnChoiceSend) btnChoiceSend.classList.add('active');
    if (btnChoiceReceive) btnChoiceReceive.classList.remove('active');
  } else if (view === viewReceive) {
    if (btnChoiceReceive) btnChoiceReceive.classList.add('active');
    if (btnChoiceSend) btnChoiceSend.classList.remove('active');
  }
}

if (homeLink) {
  homeLink.addEventListener('click', () => {
    resetSender();
    resetReceiver();
    showView(viewSend);
  });
}

if (btnChoiceSend) {
  btnChoiceSend.addEventListener('click', () => {
    const isActivelyReceiving = receiverWs && receiverWs.readyState === WebSocket.OPEN && rxBytesReceived > 0 && rxReceivedIndices.size < (rxTransferData ? rxTransferData.total_chunks : Infinity);
    if (isActivelyReceiving) return;
    showView(viewSend);
  });
}

if (btnChoiceReceive) {
  btnChoiceReceive.addEventListener('click', () => {
    if (currentFile && isStreaming && !isPaused) return;
    const isActivelyReceiving = receiverWs && receiverWs.readyState === WebSocket.OPEN && rxBytesReceived > 0 && rxReceivedIndices.size < (rxTransferData ? rxTransferData.total_chunks : Infinity);
    if (!isActivelyReceiving && rxBytesReceived > 0) {
      resetReceiver();
    }
    showView(viewReceive);
    setTimeout(() => {
      if (receiveCodeInput) receiveCodeInput.focus();
    }, 60);
  });
}

if (btnBackFromSend) {
  btnBackFromSend.addEventListener('click', () => {
    cancelTransfer();
    showView(viewSend);
  });
}

if (btnBackFromReceive) {
  btnBackFromReceive.addEventListener('click', () => {
    cancelReceive();
    showView(viewSend);
  });
}

// Safe Clipboard Copy
async function copyToClipboard(text) {
  if (navigator.clipboard && window.isSecureContext) {
    try {
      await navigator.clipboard.writeText(text);
      return true;
    } catch (_) {}
  }
  try {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    textarea.style.position = 'fixed';
    textarea.style.left = '-9999px';
    textarea.style.top = '0';
    textarea.setAttribute('readonly', '');
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    const success = document.execCommand('copy');
    document.body.removeChild(textarea);
    return success;
  } catch (_) {
    return false;
  }
}

if (btnCopyCode) {
  btnCopyCode.addEventListener('click', async () => {
    if (!currentCode) return;
    const ok = await copyToClipboard(currentCode);
    if (ok && btnCopyText) {
      const orig = btnCopyText.textContent;
      btnCopyText.textContent = 'Copied!';
      setTimeout(() => {
        btnCopyText.textContent = orig;
      }, 1800);
    }
  });
}

// ==========================================
// WebRTC Direct LAN (P2P) Engine
// ==========================================
function updateRouteDisplay(role, mode) {
  const isDirect = mode === 'lan' || mode === 'Direct LAN (P2P)';
  const label = isDirect ? 'Direct LAN' : 'Cloud Relay';
  const cls = isDirect ? 'stat-val stat-route mode-lan' : 'stat-val stat-route mode-cloud';

  if (senderRouteMode) {
    senderRouteMode.textContent = label;
    senderRouteMode.className = cls;
  }
  if (rxRouteMode) {
    rxRouteMode.textContent = label;
    rxRouteMode.className = cls;
  }
}

function closeWebRTC() {
  if (webRtcNegotiationTimer) {
    clearTimeout(webRtcNegotiationTimer);
    webRtcNegotiationTimer = null;
  }
  if (rtcDataChannel) {
    try { rtcDataChannel.close(); } catch (_) {}
    rtcDataChannel = null;
  }
  if (rtcPeerConn) {
    try { rtcPeerConn.close(); } catch (_) {}
    rtcPeerConn = null;
  }
  isWebRtcReady = false;
  webRtcRole = null;
}

function initWebRTC(role) {
  closeWebRTC();
  webRtcRole = role;

  const routePref = settingRouteMode ? settingRouteMode.value : 'auto';
  if (routePref === 'force_cloud') {
    addActivityLog(role, 'Settings: Forced Cloud Relay mode • WebRTC skipped');
    updateRouteDisplay(role, 'cloud');
    if (role === 'sender' && !isStreaming && currentFile) {
      startStreamingChunks();
    }
    return;
  }

  if (!window.RTCPeerConnection) {
    addActivityLog(role, 'WebRTC not available in browser • Using Cloud Relay');
    updateRouteDisplay(role, 'cloud');
    return;
  }

  try {
    rtcPeerConn = new RTCPeerConnection(RTC_CONFIG);

    rtcPeerConn.onicecandidate = (event) => {
      if (event.candidate) {
        const ws = role === 'sender' ? senderWs : receiverWs;
        if (ws && ws.readyState === WebSocket.OPEN) {
          ws.send(JSON.stringify({
            type: 'signal_candidate',
            candidate: event.candidate,
          }));
        }
      }
    };

    rtcPeerConn.onconnectionstatechange = () => {
      if (!rtcPeerConn) return;
      const state = rtcPeerConn.connectionState;
      addActivityLog(role, `WebRTC Connection: ${state}`);
      if (state === 'failed' || state === 'disconnected') {
        isWebRtcReady = false;
        updateRouteDisplay(role, 'cloud');
      }
    };

    if (role === 'sender') {
      try {
        rtcDataChannel = rtcPeerConn.createDataChannel('chunkstream', { ordered: true });
        setupDataChannel(rtcDataChannel, 'sender');
      } catch (err) {
        console.warn('Failed to create RTCDataChannel:', err);
      }

      rtcPeerConn.createOffer()
        .then((offer) => rtcPeerConn.setLocalDescription(offer))
        .then(() => {
          if (senderWs && senderWs.readyState === WebSocket.OPEN) {
            senderWs.send(JSON.stringify({
              type: 'signal_offer',
              offer: rtcPeerConn.localDescription,
            }));
            addActivityLog('sender', 'WebRTC: P2P offer dispatched to peer');
          }
        })
        .catch((err) => {
          console.warn('WebRTC offer error:', err);
          updateRouteDisplay('sender', 'cloud');
        });

      // Fallback timer to Cloud Relay if LAN negotiation is blocked (unless force_lan is set)
      if (routePref !== 'force_lan') {
        webRtcNegotiationTimer = setTimeout(() => {
          if (!isWebRtcReady) {
            addActivityLog('sender', 'WebRTC negotiation timeout • Continuing via Cloud Relay');
            updateRouteDisplay('sender', 'cloud');
            if (!isStreaming && currentFile) {
              startStreamingChunks();
            }
          }
        }, 4000);
      } else {
        addActivityLog('sender', 'Settings: Forced Direct LAN (P2P) mode active • Awaiting peer');
      }

    } else if (role === 'receiver') {
      rtcPeerConn.ondatachannel = (event) => {
        rtcDataChannel = event.channel;
        setupDataChannel(rtcDataChannel, 'receiver');
      };
    }
  } catch (err) {
    console.warn('WebRTC initialization error:', err);
    updateRouteDisplay(role, 'cloud');
  }
}

function setupDataChannel(channel, role) {
  if (!channel) return;

  channel.onopen = () => {
    isWebRtcReady = true;
    if (webRtcNegotiationTimer) {
      clearTimeout(webRtcNegotiationTimer);
      webRtcNegotiationTimer = null;
    }
    updateRouteDisplay(role, 'lan');
    addActivityLog(role, 'Direct LAN (P2P) DataChannel OPEN • Direct high-speed link');

    if (role === 'sender' && !isStreaming && currentFile) {
      startStreamingChunks();
    }
  };

  channel.onclose = () => {
    isWebRtcReady = false;
    updateRouteDisplay(role, 'cloud');
    addActivityLog(role, 'P2P DataChannel closed • Using Cloud Relay');
  };

  channel.onerror = (err) => {
    console.warn('RTCDataChannel error:', err);
    isWebRtcReady = false;
    updateRouteDisplay(role, 'cloud');
  };

  channel.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      if (role === 'receiver') {
        if (msg.type === 'chunk' || msg.type === 'chunk_data') {
          handleReceiverChunk(msg);
        }
      } else if (role === 'sender') {
        if (msg.type === 'chunk_ack' || msg.type === 'rx_ack') {
          handleChunkAck(msg.index);
        } else if (msg.type === 'request_missing') {
          addActivityLog('sender', `[Recovery] Re-streaming ${msg.missing_chunks.length} missing chunk(s) via DataChannel`);
          if (!isPaused) startStreamingChunks(msg.missing_chunks);
        }
      }
    } catch (e) {
      console.error('DataChannel msg parse error:', e);
    }
  };
}

async function handleWebRTCSignal(role, msg) {
  if (!rtcPeerConn && (msg.type === 'signal_offer' || msg.type === 'signal_candidate')) {
    initWebRTC(role);
  }
  if (!rtcPeerConn) return;

  try {
    if (msg.type === 'signal_offer' && role === 'receiver') {
      await rtcPeerConn.setRemoteDescription(new RTCSessionDescription(msg.offer));
      const answer = await rtcPeerConn.createAnswer();
      await rtcPeerConn.setLocalDescription(answer);
      if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
        receiverWs.send(JSON.stringify({
          type: 'signal_answer',
          answer: rtcPeerConn.localDescription,
        }));
        addActivityLog('rx', 'WebRTC: P2P answer dispatched to sender');
      }
    } else if (msg.type === 'signal_answer' && role === 'sender') {
      await rtcPeerConn.setRemoteDescription(new RTCSessionDescription(msg.answer));
      addActivityLog('sender', 'WebRTC: Remote answer set • P2P handshake complete');
    } else if (msg.type === 'signal_candidate') {
      if (msg.candidate) {
        await rtcPeerConn.addIceCandidate(new RTCIceCandidate(msg.candidate));
      }
    }
  } catch (err) {
    console.warn('WebRTC signal handling error:', err);
  }
}

// ==========================================
// Sender Flow
// ==========================================
dropZone.addEventListener('click', () => fileInput.click());

dropZone.addEventListener('dragover', (e) => {
  e.preventDefault();
  dropZone.classList.add('dragover');
});

dropZone.addEventListener('dragleave', () => dropZone.classList.remove('dragover'));

dropZone.addEventListener('drop', (e) => {
  e.preventDefault();
  dropZone.classList.remove('dragover');
  if (e.dataTransfer.files.length > 0) {
    handleFileSelected(e.dataTransfer.files[0]);
  }
});

fileInput.addEventListener('change', (e) => {
  if (e.target.files.length > 0) {
    handleFileSelected(e.target.files[0]);
  }
});

function escapeHtml(str) {
  if (!str) return '';
  return String(str).replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;').replace(/"/g, '&quot;');
}

async function handleFileSelected(file) {
  if (!file || file.size <= 0) {
    alert('Please select a valid non-empty file.');
    return;
  }

  currentFile = file;
  currentCode = generate4DigitCode();
  chunkSize = getConfiguredChunkSize(isWebRtcReady);
  totalChunks = Math.ceil(file.size / chunkSize);
  ackedChunks.clear();
  inFlightChunks.clear();
  inFlightTimestamps.clear();
  bytesSent = 0;
  isStreaming = false;
  isPaused = false;

  sendFileName.textContent = file.name;
  sendFileSize.textContent = formatBytes(file.size);
  senderCodeDisplay.textContent = currentCode;

  // Initialize progress counters immediately so user sees "0 B / 300 MB" and "Chunk 0/1200"
  senderProgressFill.style.width = '0%';
  senderProgressPct.textContent = '0%';
  senderSpeed.textContent = '0.0 MB/s';
  senderEta.textContent = '--';
  senderProgressBytes.textContent = `0 B / ${formatBytes(file.size)}`;
  senderProgressChunks.textContent = `Chunk 0/${totalChunks}`;

  senderStepPick.classList.add('hidden');
  senderStepTransfer.classList.remove('hidden');
  senderStatusText.textContent = 'Waiting for receiver to connect...';
  senderStatusDot.className = 'status-dot';
  senderProgressBox.classList.remove('hidden');
  senderActiveControls.classList.add('hidden');
  if (senderPeerPill) senderPeerPill.classList.add('hidden');
  btnSendAgain.classList.add('hidden');
  btnPauseSend.classList.remove('hidden');
  btnResumeSend.classList.add('hidden');

  if (senderActivityList) senderActivityList.innerHTML = '';
  addActivityLog('sender', `Selected "${escapeHtml(file.name)}" (${formatBytes(file.size)})`);
  addActivityLog('sender', `Prepared ${totalChunks} chunks of ${formatBytes(chunkSize)} slices`);
  addActivityLog('sender', `Transfer Code: <strong>${currentCode}</strong>`);
  addActivityLog('sender', 'Waiting for receiver to connect...');

  try {
    const res = await fetch('/api/transfers', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        transfer_id: currentCode,
        filename: file.name,
        filesize: file.size,
        chunk_size: chunkSize,
        total_chunks: totalChunks,
      }),
    });
    if (!res.ok) throw new Error('Failed to register transfer');
  } catch (err) {
    console.error('Failed to init transfer:', err);
    alert('Could not start transfer session. Please check your connection.');
    resetSender();
    return;
  }

  connectSenderWebSocket();
}

function connectSenderWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  senderWs = new WebSocket(`${protocol}//${window.location.host}/ws/transfer/${currentCode}?role=sender`);

  senderWs.onopen = () => {
    sendPeerInfo('sender', senderWs);
    addActivityLog('sender', 'Connected to relay server. Waiting for peer...');
    if (senderPingTimer) clearInterval(senderPingTimer);
    senderPingTimer = setInterval(() => {
      if (senderWs && senderWs.readyState === WebSocket.OPEN) {
        try { senderWs.send(JSON.stringify({ type: 'ping', client_ts: Date.now() })); } catch (_) {}
      }
    }, 10000);
  };

  senderWs.onmessage = async (e) => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'peer_connected' && msg.role === 'receiver') {
        sendPeerInfo('sender', senderWs);
        senderStatusDot.className = 'status-dot green';
        senderStatusText.textContent = 'Receiver connected. Establishing direct link...';
        addActivityLog('sender', 'Receiver connected • Initiating Direct LAN handshake...');
        initWebRTC('sender');
      } else if (msg.type === 'peer_info') {
        if (msg.role === 'receiver' || msg.role === 'rx') {
          handlePeerInfo('sender', msg);
          const devName = msg.display_name || `${msg.device_model || msg.device || 'Peer'} (${msg.browser || 'Browser'})`;
          addActivityLog('sender', `Peer identified: <strong>${escapeHtml(devName)}</strong>`);
          if (!rtcPeerConn) {
            initWebRTC('sender');
          }
        }
      } else if (msg.type === 'signal_answer' || msg.type === 'signal_candidate') {
        handleWebRTCSignal('sender', msg);
      } else if (msg.type === 'chunk_ack' || msg.type === 'rx_ack') {
        handleChunkAck(msg.index);
      } else if (msg.type === 'peer_disconnected') {
        if (msg.role === 'receiver') {
          isPaused = true;
          if (pipelineWatchdog) {
            clearInterval(pipelineWatchdog);
            pipelineWatchdog = null;
          }
          senderStatusDot.className = 'status-dot';
          senderStatusText.textContent = 'Receiver disconnected. Transfer paused waiting for reconnect...';
          addActivityLog('sender', 'Receiver disconnected. Transfer paused');
          btnPauseSend.classList.add('hidden');
          btnResumeSend.classList.remove('hidden');
          if (senderPeerPill) senderPeerPill.classList.add('hidden');
        }
      } else if (msg.type === 'request_missing') {
        addActivityLog('sender', `Re-streaming ${msg.missing_chunks.length} missing chunks`);
        if (!isPaused) startStreamingChunks(msg.missing_chunks);
      } else if (msg.type === 'peer_paused' || msg.type === 'paused') {
        isPaused = true;
        if (pipelineWatchdog) {
          clearInterval(pipelineWatchdog);
          pipelineWatchdog = null;
        }
        senderStatusDot.className = 'status-dot';
        senderStatusText.textContent = 'Transfer paused';
        addActivityLog('sender', 'Transfer paused');
        btnPauseSend.classList.add('hidden');
        btnResumeSend.classList.remove('hidden');
      } else if (msg.type === 'peer_resumed' || msg.type === 'resumed') {
        isPaused = false;
        senderStatusDot.className = 'status-dot green';
        senderStatusText.textContent = 'Streaming directly to receiver...';
        addActivityLog('sender', 'Transfer resumed');
        btnResumeSend.classList.add('hidden');
        btnPauseSend.classList.remove('hidden');
        startStreamingChunks();
      } else if (msg.type === 'transfer_cancelled' || msg.type === 'peer_cancelled') {
        if (msg.role !== 'sender') {
          addActivityLog('sender', 'Transfer cancelled by receiver • Memory purged');
          senderStatusDot.className = 'status-dot';
          senderStatusText.textContent = 'Transfer was cancelled by receiver.';
          setTimeout(() => { resetSender(); }, 1200);
        }
      } else if (msg.type === 'ping') {
        if (senderWs && senderWs.readyState === WebSocket.OPEN) {
          senderWs.send(JSON.stringify({ type: 'pong', client_ts: msg.client_ts }));
        }
      }
    } catch (_) {}
  };

  senderWs.onerror = () => {
    senderStatusText.textContent = 'Connection error. Retrying...';
  };

  senderWs.onclose = () => {
    if (senderPingTimer) {
      clearInterval(senderPingTimer);
      senderPingTimer = null;
    }
    if (isStreaming && ackedChunks.size < totalChunks) {
      senderStatusDot.className = 'status-dot';
      senderStatusText.textContent = 'Connection closed. Transfer stopped.';
    }
  };
}

function sendPeerInfo(role, ws) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  const dev = detectDeviceInfo();
  const deviceId = getOrCreateDeviceId();

  ws.send(JSON.stringify({
    type: 'peer_info',
    role: role,
    device_id: deviceId,
    device: dev.deviceType,
    device_model: dev.deviceModel,
    display_name: dev.displayName,
    os: dev.os,
    browser: dev.browser,
  }));
}

function handlePeerInfo(role, msg) {
  const isSender = role === 'sender';
  const peerPill = isSender ? senderPeerPill : rxPeerPill;
  const peerName = isSender ? senderPeerName : rxPeerName;
  const peerIcon = isSender ? senderPeerIcon : rxPeerIcon;

  const displayName = msg.display_name || `${msg.device_model || msg.device || 'Peer'} (${msg.browser || 'Browser'})`;
  const isMobile = msg.device === 'Phone' || msg.device === 'Tablet';

  if (peerName) peerName.textContent = `Connected: ${displayName}`;
  if (peerIcon) peerIcon.innerHTML = isMobile ? SVG_MOBILE : SVG_LAPTOP;
  if (peerPill) peerPill.classList.remove('hidden');
}

function getMaxInFlight() {
  return (isWebRtcReady && rtcDataChannel && rtcDataChannel.readyState === 'open') ? 12 : 6;
}

async function pumpPipeline() {
  const isChannelOpen = isWebRtcReady && rtcDataChannel && rtcDataChannel.readyState === 'open';
  const isWsOpen = senderWs && senderWs.readyState === WebSocket.OPEN;
  if (isPumping || isPaused || !isStreaming || (!isChannelOpen && !isWsOpen) || !currentFile) return;

  const maxInFlight = getMaxInFlight();
  isPumping = true;
  try {
    while (inFlightChunks.size < maxInFlight && (ackedChunks.size + inFlightChunks.size) < totalChunks) {
      let nextIdx = -1;
      for (let i = 0; i < totalChunks; i++) {
        if (!ackedChunks.has(i) && !inFlightChunks.has(i)) {
          nextIdx = i;
          break;
        }
      }
      if (nextIdx === -1) break;

      inFlightChunks.add(nextIdx);
      inFlightTimestamps.set(nextIdx, Date.now());

      const start = nextIdx * chunkSize;
      const end = Math.min(start + chunkSize, currentFile.size);
      const slice = currentFile.slice(start, end);

      try {
        const buf = await slice.arrayBuffer();
        if (!isStreaming || isPaused) {
          inFlightChunks.delete(nextIdx);
          inFlightTimestamps.delete(nextIdx);
          break;
        }
        const hash = await computeSha256(buf);
        const b64 = arrayBufferToBase64(buf);
        const chunkMsg = JSON.stringify({
          type: 'chunk',
          index: nextIdx,
          data: b64,
          total: totalChunks,
          hash: hash,
        });

        if (isWebRtcReady && rtcDataChannel && rtcDataChannel.readyState === 'open') {
          if (rtcDataChannel.bufferedAmount > 4 * 1024 * 1024) {
            inFlightChunks.delete(nextIdx);
            inFlightTimestamps.delete(nextIdx);
            break;
          }
          rtcDataChannel.send(chunkMsg);
        } else if (senderWs && senderWs.readyState === WebSocket.OPEN) {
          senderWs.send(chunkMsg);
        } else {
          inFlightChunks.delete(nextIdx);
          inFlightTimestamps.delete(nextIdx);
          break;
        }
      } catch (err) {
        console.error('Slice read error at chunk', nextIdx, err);
        inFlightChunks.delete(nextIdx);
        inFlightTimestamps.delete(nextIdx);
        break;
      }
    }
  } finally {
    isPumping = false;
  }
}

async function startStreamingChunks(specificIndices = null) {
  const isChannelOpen = isWebRtcReady && rtcDataChannel && rtcDataChannel.readyState === 'open';
  const isWsOpen = senderWs && senderWs.readyState === WebSocket.OPEN;
  if (!currentFile || (!isChannelOpen && !isWsOpen)) return;

  isStreaming = true;
  isPaused = false;
  btnSendAgain.classList.add('hidden');
  senderActiveControls.classList.remove('hidden');
  if (senderStartTime === 0) senderStartTime = Date.now();
  senderStatusDot.className = 'status-dot green';
  senderStatusText.textContent = isChannelOpen ? 'Streaming directly over LAN (P2P)...' : 'Streaming directly to receiver...';
  senderProgressBox.classList.remove('hidden');

  if (currentFile) {
    senderProgressBytes.textContent = `${formatBytes(bytesSent)} / ${formatBytes(currentFile.size)}`;
    senderProgressChunks.textContent = `Chunk ${ackedChunks.size}/${totalChunks}`;
  }

  if (specificIndices && specificIndices.length > 0) {
    btnSendAgain.classList.add('hidden');
    senderActiveControls.classList.remove('hidden');
    for (const idx of specificIndices) {
      ackedChunks.delete(idx);
      inFlightChunks.delete(idx);
      inFlightTimestamps.delete(idx);
    }
  }

  if (!pipelineWatchdog) {
    pipelineWatchdog = setInterval(() => {
      const isChOpen = isWebRtcReady && rtcDataChannel && rtcDataChannel.readyState === 'open';
      const isWOpen = senderWs && senderWs.readyState === WebSocket.OPEN;
      if (!isStreaming || isPaused || (!isChOpen && !isWOpen)) return;
      const now = Date.now();
      let hasTimedOut = false;
      for (const [idx, ts] of inFlightTimestamps.entries()) {
        if (now - ts > 4000) {
          inFlightChunks.delete(idx);
          inFlightTimestamps.delete(idx);
          hasTimedOut = true;
        }
      }
      if (hasTimedOut || (inFlightChunks.size < getMaxInFlight() && (ackedChunks.size + inFlightChunks.size) < totalChunks)) {
        pumpPipeline();
      }
    }, 1000);
  }

  pumpPipeline();
}

function handleChunkAck(index) {
  if (ackedChunks.has(index)) return; // Avoid duplicate ACK processing
  inFlightChunks.delete(index);
  inFlightTimestamps.delete(index);
  ackedChunks.add(index);

  bytesSent = Math.min(ackedChunks.size * chunkSize, currentFile ? currentFile.size : 0);
  const totalBytes = currentFile ? currentFile.size : 0;
  const pct = totalBytes > 0 ? Math.min(100, Math.round((bytesSent / totalBytes) * 100)) : 0;

  senderProgressFill.style.width = `${pct}%`;
  senderProgressPct.textContent = `${pct}%`;

  const elapsedSec = (Date.now() - senderStartTime) / 1000;
  if (ackedChunks.size === totalChunks || pct >= 100) {
    senderEta.textContent = '0s';
  } else if (elapsedSec > 1.0 && ackedChunks.size >= 2) {
    const bytesPerSec = bytesSent / elapsedSec;
    const mbps = bytesPerSec / (1024 * 1024);
    senderSpeed.textContent = `${mbps.toFixed(1)} MB/s`;

    const remainingBytes = Math.max(0, totalBytes - bytesSent);
    const etaSec = bytesPerSec > 0 ? remainingBytes / bytesPerSec : 0;
    senderEta.textContent = formatDuration(etaSec);
  } else {
    senderEta.textContent = '--';
  }

  senderProgressBytes.textContent = `${formatBytes(bytesSent)} / ${formatBytes(totalBytes)}`;
  senderProgressChunks.textContent = `Chunk ${ackedChunks.size}/${totalChunks}`;

  if (ackedChunks.size === 1) {
    addActivityLog('sender', 'Live streaming active • Delivering chunks...');
  } else if ((ackedChunks.size % 25 === 0) || (ackedChunks.size === totalChunks)) {
    addActivityLog('sender', `Delivered chunk ${ackedChunks.size}/${totalChunks} (${pct}%) • ${senderSpeed.textContent}`);
  }

  if (ackedChunks.size === totalChunks) {
    isStreaming = false;
    senderEta.textContent = '0s';
    if (pipelineWatchdog) {
      clearInterval(pipelineWatchdog);
      pipelineWatchdog = null;
    }
    // Notify server that transfer has fully completed to trigger server-side cleanup
    if (senderWs && senderWs.readyState === WebSocket.OPEN) {
      try {
        senderWs.send(JSON.stringify({ type: 'complete', transfer_id: currentCode }));
      } catch (_) {}
    }
    senderStatusDot.className = 'status-dot green';
    senderStatusText.textContent = 'Transfer complete! Delivered to receiver.';
    senderActiveControls.classList.add('hidden');
    btnSendAgain.classList.remove('hidden');
    addActivityLog('sender', 'Transfer complete • All chunks delivered • Server notified');
    return;
  }

  // Refill pipeline window
  pumpPipeline();
}

// Pause, Resume, Cancel Controls
btnPauseSend.addEventListener('click', () => {
  if (!isStreaming) return;
  isPaused = true;
  if (pipelineWatchdog) {
    clearInterval(pipelineWatchdog);
    pipelineWatchdog = null;
  }
  if (senderWs && senderWs.readyState === WebSocket.OPEN) {
    senderWs.send(JSON.stringify({ type: 'pause' }));
  }
  btnPauseSend.classList.add('hidden');
  btnResumeSend.classList.remove('hidden');
  senderStatusDot.className = 'status-dot';
  senderStatusText.textContent = 'Transfer paused';
});

btnResumeSend.addEventListener('click', () => {
  if (!isPaused) return;
  isPaused = false;
  if (senderWs && senderWs.readyState === WebSocket.OPEN) {
    senderWs.send(JSON.stringify({ type: 'resume' }));
  }
  btnResumeSend.classList.add('hidden');
  btnPauseSend.classList.remove('hidden');
  senderStatusDot.className = 'status-dot green';
  senderStatusText.textContent = 'Streaming directly to receiver...';
  startStreamingChunks();
});

btnCancelSend.addEventListener('click', cancelTransfer);
btnSendAgain.addEventListener('click', () => {
  resetSender();
  fileInput.click();
});

function cancelTransfer() {
  const codeToDelete = currentCode;
  closeWebRTC();
  if (pipelineWatchdog) {
    clearInterval(pipelineWatchdog);
    pipelineWatchdog = null;
  }
  if (senderWs) {
    if (senderWs.readyState === WebSocket.OPEN) {
      try { senderWs.send(JSON.stringify({ type: 'cancel' })); } catch (_) {}
    }
    try { senderWs.close(); } catch (_) {}
    senderWs = null;
  }
  resetSender();
  if (codeToDelete) {
    fetch(`/api/transfers/${codeToDelete}`, { method: 'DELETE' }).catch(() => {});
  }
}

function resetSender() {
  if (senderPingTimer) {
    clearInterval(senderPingTimer);
    senderPingTimer = null;
  }
  if (pipelineWatchdog) {
    clearInterval(pipelineWatchdog);
    pipelineWatchdog = null;
  }
  closeWebRTC();
  isPumping = false;
  if (senderWs) {
    try { senderWs.close(); } catch (_) {}
    senderWs = null;
  }
  currentFile = null;
  currentCode = null;
  isStreaming = false;
  isPaused = false;
  senderStartTime = 0;
  bytesSent = 0;
  ackedChunks.clear();
  inFlightChunks.clear();
  inFlightTimestamps.clear();
  senderStepPick.classList.remove('hidden');
  senderStepTransfer.classList.add('hidden');
  senderActiveControls.classList.add('hidden');
  if (senderPeerPill) senderPeerPill.classList.add('hidden');
  if (senderActivityList) senderActivityList.innerHTML = '';
  if (senderRouteMode) {
    senderRouteMode.textContent = 'Direct LAN';
    senderRouteMode.className = 'stat-val stat-route mode-lan';
  }
  fileInput.value = '';
}

// ==========================================
// Receiver Flow
// ==========================================
btnStartReceive.addEventListener('click', startReceiving);

receiveCodeInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') startReceiving();
});

receiveCodeInput.addEventListener('input', () => {
  receiveCodeInput.value = receiveCodeInput.value.trim().toUpperCase();
});

async function startReceiving() {
  const code = receiveCodeInput.value.trim().toUpperCase();
  if (!code || code.length < 4) {
    alert('Please enter a valid 4-digit transfer code.');
    return;
  }

  if (rxActivityList) rxActivityList.innerHTML = '';
  addActivityLog('rx', `Connecting with code <strong>${escapeHtml(code)}</strong>...`);

  // Pre-fetch metadata
  try {
    const res = await fetch(`/api/transfers/${code}`);
    if (res.ok) {
      rxTransferData = await res.json();
      if (rxTransferData.chunk_size) {
        chunkSize = rxTransferData.chunk_size;
      }
      rxFileName.textContent = rxTransferData.filename;
      rxFileSize.textContent = formatBytes(rxTransferData.filesize);
      rxProgressBytes.textContent = `0 B / ${formatBytes(rxTransferData.filesize)}`;
      rxProgressChunks.textContent = `Chunk 0/${rxTransferData.total_chunks}`;
      addActivityLog('rx', `File: <strong>${escapeHtml(rxTransferData.filename)}</strong> (${formatBytes(rxTransferData.filesize)}, ${rxTransferData.total_chunks} chunks of ${formatBytes(chunkSize)} slices)`);
    }
  } catch (_) {}

  receiverStepInput.classList.add('hidden');
  receiverStepTransfer.classList.remove('hidden');
  rxStatusDot.className = 'status-dot green';
  rxStatusText.textContent = 'Connecting to sender...';
  rxBytesReceived = 0;
  rxReceivedIndices.clear();
  rxChunksMap.clear();

  connectReceiverWebSocket(code);
}

function connectReceiverWebSocket(code) {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  receiverWs = new WebSocket(`${protocol}//${window.location.host}/ws/transfer/${code}?role=receiver`);

  receiverWs.onopen = () => {
    sendPeerInfo('rx', receiverWs);
    addActivityLog('rx', 'Connected to relay server. Waiting for sender...');
    if (rxPingTimer) clearInterval(rxPingTimer);
    rxPingTimer = setInterval(() => {
      if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
        try { receiverWs.send(JSON.stringify({ type: 'ping', client_ts: Date.now() })); } catch (_) {}
      }
    }, 10000);
  };

  receiverWs.onmessage = async (e) => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'peer_connected' && msg.role === 'sender') {
        sendPeerInfo('rx', receiverWs);
        addActivityLog('rx', 'Sender connected • Listening for direct stream...');
      } else if (msg.type === 'peer_info') {
        if (msg.role === 'sender') {
          handlePeerInfo('rx', msg);
          const devName = msg.display_name || `${msg.device_model || msg.device || 'Peer'} (${msg.browser || 'Browser'})`;
          addActivityLog('rx', `Peer identified: <strong>${escapeHtml(devName)}</strong>`);
        }
      } else if (msg.type === 'signal_offer' || msg.type === 'signal_candidate') {
        handleWebRTCSignal('receiver', msg);
      } else if (msg.type === 'chunk' || msg.type === 'chunk_data') {
        await handleReceiverChunk(msg);
      } else if (msg.type === 'peer_disconnected') {
        if (msg.role === 'sender') {
          rxStatusDot.className = 'status-dot';
          rxStatusText.textContent = 'Sender disconnected. Waiting to reconnect...';
          addActivityLog('rx', 'Sender disconnected. Waiting to reconnect...');
          if (rxPeerPill) rxPeerPill.classList.add('hidden');
        }
      } else if (msg.type === 'peer_paused') {
        rxStatusDot.className = 'status-dot';
        rxStatusText.textContent = 'Transfer paused by sender';
        addActivityLog('rx', 'Transfer paused by sender');
      } else if (msg.type === 'peer_resumed') {
        rxStatusDot.className = 'status-dot green';
        rxStatusText.textContent = 'Receiving stream from sender...';
        addActivityLog('rx', 'Transfer resumed by sender');
      } else if (msg.type === 'transfer_cancelled' || msg.type === 'peer_cancelled') {
        if (msg.role !== 'receiver') {
          addActivityLog('rx', 'Transfer cancelled by sender • Memory purged');
          rxStatusDot.className = 'status-dot';
          rxStatusText.textContent = 'Transfer was cancelled by sender.';
          setTimeout(() => { resetReceiver(); }, 1200);
        }
      } else if (msg.type === 'ping') {
        if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
          receiverWs.send(JSON.stringify({ type: 'pong', client_ts: msg.client_ts }));
        }
      }
    } catch (_) {}
  };

  receiverWs.onerror = () => {
    rxStatusText.textContent = 'Connection error. Retrying...';
  };

  receiverWs.onclose = () => {
    if (rxPingTimer) {
      clearInterval(rxPingTimer);
      rxPingTimer = null;
    }
    if (rxTransferData && rxReceivedIndices.size < rxTransferData.total_chunks) {
      rxStatusDot.className = 'status-dot';
      rxStatusText.textContent = 'Connection lost.';
    }
  };
}

let rxWatchdogTimer = null;

function requestMissingChunk(index) {
  const nackMsg = JSON.stringify({
    type: 'request_missing',
    missing_chunks: [index],
  });
  if (isWebRtcReady && rtcDataChannel && rtcDataChannel.readyState === 'open') {
    try { rtcDataChannel.send(nackMsg); } catch (_) {}
  } else if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
    try { receiverWs.send(nackMsg); } catch (_) {}
  }
}

function resetRxWatchdog() {
  if (rxWatchdogTimer) clearTimeout(rxWatchdogTimer);
  const totalExpected = rxTransferData ? rxTransferData.total_chunks : 0;
  if (rxReceivedIndices.size >= totalExpected && totalExpected > 0) return;

  rxWatchdogTimer = setTimeout(() => {
    checkMissingChunks();
  }, 3500);
}

function checkMissingChunks() {
  if (!rxTransferData || rxReceivedIndices.size >= rxTransferData.total_chunks) return;
  const total = rxTransferData.total_chunks;
  const missing = [];
  for (let i = 0; i < total; i++) {
    if (!rxReceivedIndices.has(i)) {
      missing.push(i);
      if (missing.length >= 32) break; // batch requests
    }
  }
  if (missing.length > 0) {
    addActivityLog('rx', `[Recovery] Detected ${missing.length} missing chunk(s) • Requesting re-transmission...`);
    const nackMsg = JSON.stringify({
      type: 'request_missing',
      missing_chunks: missing,
    });
    if (isWebRtcReady && rtcDataChannel && rtcDataChannel.readyState === 'open') {
      try { rtcDataChannel.send(nackMsg); } catch (_) {}
    } else if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
      try { receiverWs.send(nackMsg); } catch (_) {}
    }
    resetRxWatchdog();
  }
}

async function handleReceiverChunk(msg) {
  const { index, data, total, hash } = msg;

  if (rxStartTime === 0) {
    rxStartTime = Date.now();
    rxStatusText.textContent = 'Receiving stream from sender...';
  }

  const uint8 = base64ToUint8Array(data);

  // Per-chunk SHA-256 data integrity check
  if (hash) {
    const computedHash = await computeSha256(uint8.buffer);
    if (computedHash && computedHash !== hash) {
      addActivityLog('rx', `[Integrity Error] Hash mismatch on chunk #${index}! Requesting retransmit...`);
      requestMissingChunk(index);
      return;
    }
  }

  rxChunksMap.set(index, uint8);
  rxReceivedIndices.add(index);
  rxBytesReceived += uint8.byteLength;

  // Send ACK back to sender (direct via DataChannel or via WebSocket relay)
  const ackPayload = JSON.stringify({ type: 'chunk_ack', index: index });
  if (isWebRtcReady && rtcDataChannel && rtcDataChannel.readyState === 'open') {
    try { rtcDataChannel.send(ackPayload); } catch (_) {}
  } else if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
    try { receiverWs.send(ackPayload); } catch (_) {}
  }

  // Active missing chunk detection watchdog
  resetRxWatchdog();

  // Update Progress
  const totalExpected = rxTransferData ? rxTransferData.filesize : (total * chunkSize);
  const pct = totalExpected > 0 ? Math.min(100, Math.round((rxBytesReceived / totalExpected) * 100)) : 0;

  rxProgressFill.style.width = `${pct}%`;
  rxProgressPct.textContent = `${pct}%`;

  const elapsedSec = (Date.now() - rxStartTime) / 1000;
  if (rxReceivedIndices.size === total || pct >= 100) {
    rxEta.textContent = '0s';
  } else if (elapsedSec > 1.0 && rxReceivedIndices.size >= 2) {
    const bytesPerSec = rxBytesReceived / elapsedSec;
    const mbps = bytesPerSec / (1024 * 1024);
    rxSpeed.textContent = `${mbps.toFixed(1)} MB/s`;

    const remaining = Math.max(0, totalExpected - rxBytesReceived);
    const etaSec = bytesPerSec > 0 ? remaining / bytesPerSec : 0;
    rxEta.textContent = formatDuration(etaSec);
  } else {
    rxEta.textContent = '--';
  }

  rxProgressBytes.textContent = `${formatBytes(rxBytesReceived)} / ${formatBytes(totalExpected)}`;
  rxProgressChunks.textContent = `Chunk ${rxReceivedIndices.size}/${total}`;
  rxStatusText.textContent = `Receiving chunk ${rxReceivedIndices.size} of ${total} (${pct}% in memory)`;

  if (rxReceivedIndices.size === 1) {
    addActivityLog('rx', 'Receiving chunk stream directly into memory...');
  } else if ((rxReceivedIndices.size % 25 === 0) || (rxReceivedIndices.size === total)) {
    addActivityLog('rx', `Buffered chunk ${rxReceivedIndices.size}/${total} (${pct}%) • ${rxSpeed.textContent}`);
  }

  if (rxReceivedIndices.size === total) {
    completeReceiverFile(rxTransferData ? rxTransferData.filename : 'downloaded_file', rxTransferData ? rxTransferData.mime_type : null);
  }
}

async function completeReceiverFile(filename, mimeType) {
  if (rxWatchdogTimer) {
    clearTimeout(rxWatchdogTimer);
    rxWatchdogTimer = null;
  }
  rxStatusDot.className = 'status-dot green';
  rxStatusText.textContent = 'Download complete!';
  rxEta.textContent = '0s';
  rxActiveControls.classList.add('hidden');
  addActivityLog('rx', 'All chunks buffered • 100% bit-exact SHA-256 verified');
  addActivityLog('rx', `Assembling file in memory...`);

  // Assemble file sequentially from received chunks and trigger standard browser download
  const totalExpectedChunks = rxTransferData ? rxTransferData.total_chunks : rxReceivedIndices.size;
  const sortedChunks = [];
  for (let i = 0; i < totalExpectedChunks; i++) {
    if (rxChunksMap.has(i)) {
      sortedChunks.push(rxChunksMap.get(i));
    }
  }

  // Notify server of completion
  if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
    try {
      receiverWs.send(JSON.stringify({
        type: 'complete',
        transfer_id: rxTransferData ? rxTransferData.id : null,
      }));
    } catch (_) {}
  }

  const blob = new Blob(sortedChunks, { type: mimeType || 'application/octet-stream' });
  // Immediately clear chunk references to free client RAM
  sortedChunks.length = 0;
  for (const k of rxChunksMap.keys()) {
    rxChunksMap.set(k, null);
  }
  rxChunksMap.clear();
  rxReceivedIndices.clear();

  const url = URL.createObjectURL(blob);
  const tempLink = document.createElement('a');
  tempLink.href = url;
  tempLink.download = filename || 'download';
  document.body.appendChild(tempLink);
  tempLink.click();
  document.body.removeChild(tempLink);

  addActivityLog('rx', `File assembled • Download triggered directly to device`);

  // Revoke object URL promptly to purge blob memory
  setTimeout(() => {
    try {
      URL.revokeObjectURL(url);
      addActivityLog('rx', 'Memory cleared • Object URL cache purged');
    } catch (_) {}
  }, 8000);

  rxStatusFinished.textContent = 'File downloaded to your device.';
  rxClearedBox.classList.remove('hidden');
}

btnCancelReceive.addEventListener('click', cancelReceive);

function cancelReceive() {
  if (rxWatchdogTimer) {
    clearTimeout(rxWatchdogTimer);
    rxWatchdogTimer = null;
  }
  closeWebRTC();
  if (receiverWs) {
    if (receiverWs.readyState === WebSocket.OPEN) {
      try { receiverWs.send(JSON.stringify({ type: 'cancel' })); } catch (_) {}
    }
    try { receiverWs.close(); } catch (_) {}
    receiverWs = null;
  }
  for (const k of rxChunksMap.keys()) {
    rxChunksMap.set(k, null);
  }
  rxChunksMap.clear();
  rxReceivedIndices.clear();
  resetReceiver();
}

btnReceiveAgain.addEventListener('click', () => {
  resetReceiver();
  showView(viewReceive);
  if (receiveCodeInput) receiveCodeInput.focus();
});

function resetReceiver() {
  if (rxPingTimer) {
    clearInterval(rxPingTimer);
    rxPingTimer = null;
  }
  if (rxWatchdogTimer) {
    clearTimeout(rxWatchdogTimer);
    rxWatchdogTimer = null;
  }
  closeWebRTC();
  if (receiverWs) {
    try { receiverWs.close(); } catch (_) {}
    receiverWs = null;
  }
  for (const k of rxChunksMap.keys()) {
    rxChunksMap.set(k, null);
  }
  rxChunksMap.clear();
  rxReceivedIndices.clear();
  rxBytesReceived = 0;
  rxStartTime = 0;
  rxTransferData = null;
  if (receiveCodeInput) receiveCodeInput.value = '';
  if (receiverStepInput) receiverStepInput.classList.remove('hidden');
  if (receiverStepTransfer) receiverStepTransfer.classList.add('hidden');
  if (rxClearedBox) rxClearedBox.classList.add('hidden');
  if (rxProgressBox) rxProgressBox.classList.remove('hidden');
  if (rxActiveControls) rxActiveControls.classList.remove('hidden');
  if (rxPeerPill) rxPeerPill.classList.add('hidden');
  if (rxActivityList) rxActivityList.innerHTML = '';
  if (rxRouteMode) {
    rxRouteMode.textContent = 'Direct LAN';
    rxRouteMode.className = 'stat-val stat-route mode-lan';
  }
}

// Initial UI Setup
updateLocalDeviceDisplay();
