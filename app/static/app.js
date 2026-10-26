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
  if (!bytes || bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function formatDuration(sec) {
  if (!isFinite(sec) || sec <= 0) return '--';
  if (sec < 60) return `${Math.ceil(sec)}s`;
  const m = Math.floor(sec / 60);
  const s = Math.ceil(sec % 60);
  return `${m}m ${s}s`;
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
let bytesSent = 0;
let senderStartTime = 0;

let rxBytesReceived = 0;
let rxStartTime = 0;
let rxWritableStream = null;
let rxChunksMap = new Map(); // Fallback for browsers without FileSystem Access API
let rxReceivedIndices = new Set();

// DOM Elements
const viewHome = document.getElementById('view-home');
const viewSend = document.getElementById('view-send');
const viewReceive = document.getElementById('view-receive');
const homeLink = document.getElementById('home-link');

const btnChoiceSend = document.getElementById('btn-choice-send');
const btnChoiceReceive = document.getElementById('btn-choice-receive');
const btnBackFromSend = document.getElementById('btn-back-from-send');
const btnBackFromReceive = document.getElementById('btn-back-from-receive');

// Device Bar & Peer Recognition Elements
const deviceBar = document.getElementById('device-bar');
const deviceBarName = document.getElementById('device-bar-name');
const deviceBarId = document.getElementById('device-bar-id');
const deviceBarIcon = document.getElementById('device-bar-icon');
const senderPeerPill = document.getElementById('sender-peer-pill');
const senderPeerName = document.getElementById('sender-peer-name');
const senderPeerIcon = document.getElementById('sender-peer-icon');
const rxPeerPill = document.getElementById('rx-peer-pill');
const rxPeerName = document.getElementById('rx-peer-name');
const rxPeerIcon = document.getElementById('rx-peer-icon');

// Sender Elements
const fileInput = document.getElementById('file-input');
const dropZone = document.getElementById('drop-zone');
const senderStepPick = document.getElementById('sender-step-pick');
const senderStepTransfer = document.getElementById('sender-step-transfer');
const sendFileName = document.getElementById('send-file-name');
const sendFileSize = document.getElementById('send-file-size');
const senderCodeDisplay = document.getElementById('sender-code-display');
const btnCopyCode = document.getElementById('btn-copy-code');
const senderStatusText = document.getElementById('sender-status-text');
const senderStatusDot = document.getElementById('sender-status-dot');
const senderProgressBox = document.getElementById('sender-progress-box');
const senderProgressFill = document.getElementById('sender-progress-fill');
const senderProgressPct = document.getElementById('sender-progress-pct');
const senderSpeed = document.getElementById('sender-speed');
const senderEta = document.getElementById('sender-eta');
const senderProgressBytes = document.getElementById('sender-progress-bytes');
const senderProgressChunks = document.getElementById('sender-progress-chunks');
const senderActiveControls = document.getElementById('sender-active-controls');
const btnPauseSend = document.getElementById('btn-pause-send');
const btnResumeSend = document.getElementById('btn-resume-send');
const btnCancelSend = document.getElementById('btn-cancel-send');
const btnSendAgain = document.getElementById('btn-send-again');

// Receiver Elements
const receiverStepInput = document.getElementById('receiver-step-input');
const receiverStepTransfer = document.getElementById('receiver-step-transfer');
const receiveCodeInput = document.getElementById('receive-code-input');
const btnStartReceive = document.getElementById('btn-start-receive');
const rxFileName = document.getElementById('rx-file-name');
const rxFileSize = document.getElementById('rx-file-size');
const rxStatusText = document.getElementById('rx-status-text');
const rxStatusDot = document.getElementById('rx-status-dot');
const rxProgressBox = document.getElementById('rx-progress-box');
const rxProgressFill = document.getElementById('rx-progress-fill');
const rxProgressPct = document.getElementById('rx-progress-pct');
const rxSpeed = document.getElementById('rx-speed');
const rxEta = document.getElementById('rx-eta');
const rxProgressBytes = document.getElementById('rx-progress-bytes');
const rxProgressChunks = document.getElementById('rx-progress-chunks');
const rxActiveControls = document.getElementById('rx-active-controls');
const btnCancelReceive = document.getElementById('btn-cancel-receive');
const rxClearedBox = document.getElementById('rx-cleared-box');
const rxStatusFinished = document.getElementById('rx-status-finished');
const btnReceiveAgain = document.getElementById('btn-receive-again');

// Telemetry Elements - Sender
const senderTelemetryToggle = document.getElementById('sender-telemetry-toggle');
const senderTelemetryDrawer = document.getElementById('sender-telemetry-drawer');
const senderTelemetryChevron = document.getElementById('sender-telemetry-chevron');
const senderRttBadge = document.getElementById('sender-rtt-badge');
const senderLocalIcon = document.getElementById('sender-local-icon');
const senderLocalName = document.getElementById('sender-local-name');
const senderLocalNet = document.getElementById('sender-local-net');
const senderRelayRegion = document.getElementById('sender-relay-region');
const senderPeerNodeIcon = document.getElementById('sender-peer-node-icon');
const senderPeerNodeName = document.getElementById('sender-peer-node-name');
const senderPeerNet = document.getElementById('sender-peer-net');
const senderMyId = document.getElementById('sender-my-id');
const senderMyInfo = document.getElementById('sender-my-info');
const senderMyNetPill = document.getElementById('sender-my-net-pill');
const senderPeerId = document.getElementById('sender-peer-id');
const senderPeerInfo = document.getElementById('sender-peer-info');
const senderPeerNetPill = document.getElementById('sender-peer-net-pill');
const senderChunkMatrix = document.getElementById('sender-chunk-matrix');
const senderMatrixStat = document.getElementById('sender-matrix-stat');
const senderSparklineCanvas = document.getElementById('sender-sparkline-canvas');
const senderSparklineVal = document.getElementById('sender-sparkline-val');

// Telemetry Elements - Receiver
const rxTelemetryToggle = document.getElementById('rx-telemetry-toggle');
const rxTelemetryDrawer = document.getElementById('rx-telemetry-drawer');
const rxTelemetryChevron = document.getElementById('rx-telemetry-chevron');
const rxRttBadge = document.getElementById('rx-rtt-badge');
const rxPeerNodeIcon = document.getElementById('rx-peer-node-icon');
const rxPeerNodeName = document.getElementById('rx-peer-node-name');
const rxPeerNet = document.getElementById('rx-peer-net');
const rxRelayRegion = document.getElementById('rx-relay-region');
const rxLocalIcon = document.getElementById('rx-local-icon');
const rxLocalName = document.getElementById('rx-local-name');
const rxLocalNet = document.getElementById('rx-local-net');
const rxMyId = document.getElementById('rx-my-id');
const rxMyInfo = document.getElementById('rx-my-info');
const rxMyNetPill = document.getElementById('rx-my-net-pill');
const rxPeerId = document.getElementById('rx-peer-id');
const rxPeerInfo = document.getElementById('rx-peer-info');
const rxPeerNetPill = document.getElementById('rx-peer-net-pill');
const rxChunkMatrix = document.getElementById('rx-chunk-matrix');
const rxMatrixStat = document.getElementById('rx-matrix-stat');
const rxSparklineCanvas = document.getElementById('rx-sparkline-canvas');
const rxSparklineVal = document.getElementById('rx-sparkline-val');

// Telemetry State
let senderPingTimer = null;
let rxPingTimer = null;
let senderSpeedHistory = [0];
let rxSpeedHistory = [0];
let senderCellMap = [];
let rxCellMap = [];
let cachedNetInfo = null;

// Dynamic Transfer Mode State (Direct LAN vs Cloud Relay)
let peerConnection = null;
let dataChannel = null;
let transferMode = 'detecting'; // 'detecting' | 'lan' | 'cloud'
let lanDetectionTimer = null;
const rtcConfig = {
  iceServers: [
    { urls: 'stun:stun.l.google.com:19302' },
  ],
};

function updateTransferModeUI(role, mode) {
  transferMode = mode;
  const pill = role === 'sender' ? document.getElementById('sender-mode-pill') : document.getElementById('rx-mode-pill');
  const label = role === 'sender' ? document.getElementById('sender-mode-label') : document.getElementById('rx-mode-label');
  const relayNode = role === 'sender' ? document.getElementById('sender-hop-relay') : document.getElementById('rx-hop-relay');
  const relaySub = role === 'sender' ? document.getElementById('sender-relay-region') : document.getElementById('rx-relay-region');

  if (!pill || !label) return;

  pill.className = `mode-pill mode-${mode}`;
  if (mode === 'lan') {
    label.textContent = 'Direct LAN (P2P)';
    if (relayNode) relayNode.classList.add('bypassed');
    if (relaySub) relaySub.textContent = 'Bypassed (Direct LAN)';
  } else if (mode === 'cloud') {
    label.textContent = 'Cloud Relay (Zero Storage)';
    if (relayNode) relayNode.classList.remove('bypassed');
    if (relaySub && cachedNetInfo && cachedNetInfo.server_region) {
      relaySub.textContent = cachedNetInfo.server_region;
    }
  } else {
    label.textContent = 'Mode: Auto-Detecting...';
    if (relayNode) relayNode.classList.remove('bypassed');
  }
}

// Cookie & Storage Device Identification
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
    devId = 'DEV-' + Math.random().toString(16).substring(2, 8).toUpperCase();
    setCookie('cs_device_id', devId, 30);
    safeSetStorage('cs_device_id', devId);
  }
  return devId;
}

// Professional SVG device icons
const SVG_LAPTOP = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="2" y="3" width="20" height="14" rx="2" ry="2"></rect><line x1="8" y1="21" x2="16" y2="21"></line><line x1="12" y1="17" x2="12" y2="21"></line></svg>`;
const SVG_MOBILE = `<svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="5" y="2" width="14" height="20" rx="2" ry="2"></rect><line x1="12" y1="18" x2="12.01" y2="18"></line></svg>`;

// Client Hardware, OS, Browser & Accurate Device Detection
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
    else if (/Xiaomi|Redmi/i.test(ua)) deviceModel = 'Xiaomi';
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

  let netType = 'Wi-Fi / LAN';
  try {
    const conn = navigator.connection || navigator.mozConnection || navigator.webkitConnection;
    if (conn) {
      if (conn.type === 'cellular') {
        netType = `Cellular (${conn.effectiveType ? conn.effectiveType.toUpperCase() : '5G/4G'})`;
      } else if (conn.type === 'wifi') {
        netType = 'Wi-Fi';
      } else if (conn.effectiveType) {
        netType = conn.effectiveType.toUpperCase();
      }
    }
  } catch (_) {}

  return {
    os,
    browser,
    deviceType,
    deviceModel,
    network: netType,
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

async function getNetworkInfo() {
  if (cachedNetInfo) return cachedNetInfo;
  try {
    const res = await fetch('/api/network-info');
    if (res.ok) {
      cachedNetInfo = await res.json();
    }
  } catch (_) {
    cachedNetInfo = {
      client_ip: '127.0.0.1',
      is_local: true,
      server_region: 'Frankfurt, EU (Cloud Relay)',
    };
  }
  return cachedNetInfo || {
    client_ip: '127.0.0.1',
    is_local: true,
    server_region: 'Frankfurt, EU (Cloud Relay)',
  };
}

async function setupLocalDeviceCard(role) {
  const netInfo = await getNetworkInfo();
  const dev = detectDeviceInfo();
  const deviceId = getOrCreateDeviceId();

  const localIcon = role === 'sender' ? senderLocalIcon : rxLocalIcon;
  const localName = role === 'sender' ? senderLocalName : rxLocalName;
  const localNet = role === 'sender' ? senderLocalNet : rxLocalNet;
  const relayRegion = role === 'sender' ? senderRelayRegion : rxRelayRegion;
  const myId = role === 'sender' ? senderMyId : rxMyId;
  const myInfo = role === 'sender' ? senderMyInfo : rxMyInfo;
  const myNetPill = role === 'sender' ? senderMyNetPill : rxMyNetPill;

  if (localIcon) localIcon.innerHTML = (dev.deviceType === 'Phone' || dev.deviceType === 'Tablet') ? SVG_MOBILE : SVG_LAPTOP;
  if (localName) localName.textContent = `${dev.browser} on ${dev.os}`;
  if (localNet) localNet.textContent = `${dev.network} • ${netInfo.client_ip}`;
  if (relayRegion && netInfo.server_region) relayRegion.textContent = netInfo.server_region;
  if (myId) myId.textContent = `ID: ${deviceId}`;
  if (myInfo) myInfo.textContent = `${dev.browser} • ${dev.os}`;
  if (myNetPill) myNetPill.textContent = `${dev.network} • ${netInfo.is_local ? 'Local LAN' : 'WAN'}`;
}

// RTT Ping/Pong Latency
function startRttPing(role, ws) {
  stopRttPing(role);
  const timer = setInterval(() => {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({
        type: 'ping',
        client_ts: performance.now(),
      }));
    }
  }, 2500);

  if (role === 'sender') senderPingTimer = timer;
  else rxPingTimer = timer;
}

function stopRttPing(role) {
  if (role === 'sender' && senderPingTimer) {
    clearInterval(senderPingTimer);
    senderPingTimer = null;
  } else if (role === 'rx' && rxPingTimer) {
    clearInterval(rxPingTimer);
    rxPingTimer = null;
  }
}

function handlePong(role, msg) {
  if (!msg.client_ts) return;
  const rtt = Math.max(1, Math.round(performance.now() - msg.client_ts));
  const badge = role === 'sender' ? senderRttBadge : rxRttBadge;
  if (badge) {
    badge.textContent = `RTT: ${rtt} ms`;
  }
}

// Peer Info Exchange
async function sendPeerInfo(role, ws) {
  if (!ws || ws.readyState !== WebSocket.OPEN) return;
  const netInfo = await getNetworkInfo();
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
    network: dev.network,
    location: netInfo ? (netInfo.client_ip + (netInfo.is_local ? ' (Local)' : ' (WAN)')) : 'Unknown',
  }));
}

function handlePeerInfo(role, msg) {
  const isSender = role === 'sender';
  const peerPill = isSender ? senderPeerPill : rxPeerPill;
  const peerName = isSender ? senderPeerName : rxPeerName;
  const peerIcon = isSender ? senderPeerIcon : rxPeerIcon;
  const peerNode = isSender ? document.getElementById('sender-hop-peer') : document.getElementById('rx-hop-peer');
  const peerNodeName = isSender ? senderPeerNodeName : rxPeerNodeName;
  const peerNodeIcon = isSender ? senderPeerNodeIcon : rxPeerNodeIcon;

  const displayName = msg.display_name || `${msg.device_model || msg.device || 'Peer'} (${msg.browser || 'Browser'})`;
  const isMobile = msg.device === 'Phone' || msg.device === 'Tablet';

  if (peerName) peerName.textContent = `Connected: ${displayName}`;
  if (peerIcon) peerIcon.innerHTML = isMobile ? SVG_MOBILE : SVG_LAPTOP;
  if (peerPill) peerPill.classList.remove('hidden');

  if (peerNode) peerNode.classList.add('active');
  if (peerNodeName) peerNodeName.textContent = displayName;
  if (peerNodeIcon) peerNodeIcon.innerHTML = isMobile ? SVG_MOBILE : SVG_LAPTOP;

  const peerId = isSender ? senderPeerId : rxPeerId;
  const peerInfo = isSender ? senderPeerInfo : rxPeerInfo;
  const peerNetPill = isSender ? senderPeerNetPill : rxPeerNetPill;

  if (peerId) peerId.textContent = `ID: ${msg.device_id || '----'}`;
  if (peerInfo) peerInfo.textContent = `${msg.browser || 'Browser'} • ${msg.os || 'OS'}`;
  if (peerNetPill) peerNetPill.textContent = `${msg.network || 'Network'} • ${msg.location || 'Remote'}`;
}

// Chunk Transmission Matrix
function initChunkMatrix(role, total) {
  const container = role === 'sender' ? senderChunkMatrix : rxChunkMatrix;
  const footerStat = role === 'sender' ? senderMatrixStat : rxMatrixStat;
  if (!container) return;

  container.innerHTML = '';
  const numCells = total <= 120 ? total : 100;
  const cellMap = [];

  for (let i = 0; i < numCells; i++) {
    const cell = document.createElement('div');
    cell.className = 'chunk-cell pending';
    cell.title = `Chunk block ${i + 1}/${numCells}`;
    container.appendChild(cell);
    cellMap.push(cell);
  }

  if (role === 'sender') {
    senderCellMap = cellMap;
    if (footerStat) footerStat.textContent = `0 / ${total} chunks ACKed • Zero server storage`;
  } else {
    rxCellMap = cellMap;
    if (footerStat) footerStat.textContent = `0 / ${total} chunks saved • Direct stream to disk`;
  }
}

function setChunkState(role, chunkIdx, state, totalCount) {
  const cellMap = role === 'sender' ? senderCellMap : rxCellMap;
  if (!cellMap || cellMap.length === 0) return;

  const total = totalCount || (role === 'sender' ? totalChunks : 1);
  const cellIdx = Math.min(cellMap.length - 1, Math.floor((chunkIdx / Math.max(total, 1)) * cellMap.length));
  const cell = cellMap[cellIdx];
  if (cell) {
    cell.className = `chunk-cell ${state}`;
  }

  const footerStat = role === 'sender' ? senderMatrixStat : rxMatrixStat;
  if (footerStat) {
    if (role === 'sender') {
      footerStat.textContent = `${ackedChunks.size} / ${totalChunks} chunks ACKed • In-flight: ${inFlightChunks.size}`;
    } else {
      footerStat.textContent = `${rxReceivedIndices.size} chunks saved directly to device`;
    }
  }
}

function resetChunkMatrix(role) {
  const container = role === 'sender' ? senderChunkMatrix : rxChunkMatrix;
  if (container) container.innerHTML = '';
  if (role === 'sender') {
    senderCellMap = [];
    if (senderMatrixStat) senderMatrixStat.textContent = '256 KB Chunks • Zero server storage';
    if (senderRttBadge) senderRttBadge.textContent = 'RTT: -- ms';
    if (senderPeerId) senderPeerId.textContent = 'ID: ----';
    if (senderPeerInfo) senderPeerInfo.textContent = 'Waiting for peer...';
    if (senderPeerNetPill) senderPeerNetPill.textContent = '--';
    const peerNode = document.getElementById('sender-hop-peer');
    if (peerNode) peerNode.classList.remove('active');
  } else {
    rxCellMap = [];
    if (rxMatrixStat) rxMatrixStat.textContent = '256 KB Chunks • Direct stream to disk';
    if (rxRttBadge) rxRttBadge.textContent = 'RTT: -- ms';
    if (rxPeerId) rxPeerId.textContent = 'ID: ----';
    if (rxPeerInfo) rxPeerInfo.textContent = 'Waiting for peer...';
    if (rxPeerNetPill) rxPeerNetPill.textContent = '--';
    const peerNode = document.getElementById('rx-hop-peer');
    if (peerNode) peerNode.classList.remove('active');
  }
}

// Throughput Dynamics Sparkline
function recordSpeedSample(role, mbps) {
  const history = role === 'sender' ? senderSpeedHistory : rxSpeedHistory;
  history.push(mbps);
  if (history.length > 30) {
    history.shift();
  }
  const valLabel = role === 'sender' ? senderSparklineVal : rxSparklineVal;
  if (valLabel) {
    valLabel.textContent = `${mbps.toFixed(1)} MB/s`;
  }
  renderSparkline(role);
}

function renderSparkline(role) {
  const canvas = role === 'sender' ? senderSparklineCanvas : rxSparklineCanvas;
  if (!canvas) return;
  const history = role === 'sender' ? senderSpeedHistory : rxSpeedHistory;
  const dpr = window.devicePixelRatio || 1;
  const rect = canvas.getBoundingClientRect();
  const width = rect.width || 380;
  const height = rect.height || 52;

  if (canvas.width !== Math.round(width * dpr) || canvas.height !== Math.round(height * dpr)) {
    canvas.width = Math.round(width * dpr);
    canvas.height = Math.round(height * dpr);
  }

  const ctx = canvas.getContext('2d');
  ctx.save();
  ctx.scale(dpr, dpr);
  ctx.clearRect(0, 0, width, height);

  const samples = history.length > 0 ? history : [0];
  const maxVal = Math.max(1.0, ...samples);
  const strokeColor = role === 'sender' ? '#2563eb' : '#10b981';
  const fillColorStart = role === 'sender' ? 'rgba(37, 99, 235, 0.28)' : 'rgba(16, 185, 129, 0.28)';
  const fillColorEnd = role === 'sender' ? 'rgba(37, 99, 235, 0.0)' : 'rgba(16, 185, 129, 0.0)';

  const step = width / Math.max(samples.length - 1, 1);

  ctx.beginPath();
  for (let i = 0; i < samples.length; i++) {
    const x = i * step;
    const y = height - (samples[i] / maxVal) * (height - 8) - 4;
    if (i === 0) ctx.moveTo(x, y);
    else ctx.lineTo(x, y);
  }

  ctx.strokeStyle = strokeColor;
  ctx.lineWidth = 2;
  ctx.lineJoin = 'round';
  ctx.stroke();

  ctx.lineTo((samples.length - 1) * step, height);
  ctx.lineTo(0, height);
  ctx.closePath();
  const grad = ctx.createLinearGradient(0, 0, 0, height);
  grad.addColorStop(0, fillColorStart);
  grad.addColorStop(1, fillColorEnd);
  ctx.fillStyle = grad;
  ctx.fill();

  ctx.restore();
}

function resetSparkline(role) {
  if (role === 'sender') {
    senderSpeedHistory = [0];
    if (senderSparklineVal) senderSparklineVal.textContent = '0.0 MB/s';
    renderSparkline('sender');
  } else {
    rxSpeedHistory = [0];
    if (rxSparklineVal) rxSparklineVal.textContent = '0.0 MB/s';
    renderSparkline('rx');
  }
}

// Drawer Toggle Logic
function setupTelemetryToggles() {
  if (senderTelemetryToggle && senderTelemetryDrawer) {
    senderTelemetryToggle.addEventListener('click', () => {
      const isHidden = senderTelemetryDrawer.classList.toggle('hidden');
      if (senderTelemetryChevron) senderTelemetryChevron.classList.toggle('open', !isHidden);
      senderTelemetryToggle.setAttribute('aria-expanded', !isHidden);
      if (!isHidden) {
        setTimeout(() => renderSparkline('sender'), 50);
      }
    });
  }

  if (rxTelemetryToggle && rxTelemetryDrawer) {
    rxTelemetryToggle.addEventListener('click', () => {
      const isHidden = rxTelemetryDrawer.classList.toggle('hidden');
      if (rxTelemetryChevron) rxTelemetryChevron.classList.toggle('open', !isHidden);
      rxTelemetryToggle.setAttribute('aria-expanded', !isHidden);
      if (!isHidden) {
        setTimeout(() => renderSparkline('rx'), 50);
      }
    });
  }
}
setupTelemetryToggles();

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
    if (currentFile && isStreaming) return;
    showView(viewSend);
  });
}

if (btnChoiceReceive) {
  btnChoiceReceive.addEventListener('click', () => {
    if (rxBytesReceived > 0) return;
    showView(viewReceive);
    setTimeout(() => receiveCodeInput.focus(), 80);
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

// Sender Drag & Drop
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
    const successful = document.execCommand('copy');
    document.body.removeChild(textarea);
    return successful;
  } catch (err) {
    return false;
  }
}

const defaultCopyHtml = `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg><span>Copy Code</span>`;
const copiedHtml = `<svg width="13" height="13" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"><polyline points="20 6 9 17 4 12"></polyline></svg><span style="color:var(--status-green)">Copied!</span>`;

if (btnCopyCode) {
  btnCopyCode.addEventListener('click', async () => {
    if (!currentCode) return;
    const ok = await copyToClipboard(currentCode);
    if (ok) {
      btnCopyCode.innerHTML = copiedHtml;
      setTimeout(() => { btnCopyCode.innerHTML = defaultCopyHtml; }, 1600);
    }
  });
}

window.addEventListener('resize', () => {
  if (senderTelemetryDrawer && !senderTelemetryDrawer.classList.contains('hidden')) {
    renderSparkline('sender');
  }
  if (rxTelemetryDrawer && !rxTelemetryDrawer.classList.contains('hidden')) {
    renderSparkline('rx');
  }
});

// WebRTC Direct LAN Implementation
async function initSenderWebRTC() {
  if (peerConnection) {
    try { peerConnection.close(); } catch (_) {}
  }

  try {
    peerConnection = new RTCPeerConnection(rtcConfig);

    peerConnection.onicecandidate = (event) => {
      if (event.candidate && senderWs && senderWs.readyState === WebSocket.OPEN) {
        senderWs.send(JSON.stringify({
          type: 'signal_candidate',
          candidate: event.candidate,
        }));
      }
    };

    dataChannel = peerConnection.createDataChannel('chunkstream-lane', {
      ordered: true,
    });
    dataChannel.binaryType = 'arraybuffer';
    dataChannel.bufferedAmountLowThreshold = 512 * 1024;

    dataChannel.onopen = () => {
      if (lanDetectionTimer) {
        clearTimeout(lanDetectionTimer);
        lanDetectionTimer = null;
      }
      updateTransferModeUI('sender', 'lan');
      if (!isPaused && !isStreaming) {
        startStreamingChunks();
      }
    };

    dataChannel.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data);
        if (msg.type === 'rx_ack') {
          handleChunkAck(msg.index);
        }
      } catch (_) {}
    };

    dataChannel.onerror = () => fallbackToCloudRelay('sender');
    dataChannel.onclose = () => {
      if (isStreaming && transferMode === 'lan') {
        fallbackToCloudRelay('sender');
      }
    };

    const offer = await peerConnection.createOffer();
    await peerConnection.setLocalDescription(offer);

    if (senderWs && senderWs.readyState === WebSocket.OPEN) {
      senderWs.send(JSON.stringify({
        type: 'signal_offer',
        sdp: offer.sdp,
      }));
    }
  } catch (err) {
    console.warn('WebRTC init error, falling back to cloud relay:', err);
    fallbackToCloudRelay('sender');
  }
}

function fallbackToCloudRelay(role) {
  if (transferMode === 'lan' && dataChannel && dataChannel.readyState === 'open') return;
  if (lanDetectionTimer) {
    clearTimeout(lanDetectionTimer);
    lanDetectionTimer = null;
  }
  updateTransferModeUI(role, 'cloud');
  if (role === 'sender' && !isPaused && !isStreaming) {
    startStreamingChunks();
  }
}

async function handleReceiverSignalOffer(sdp, transferData) {
  if (peerConnection) {
    try { peerConnection.close(); } catch (_) {}
  }

  try {
    peerConnection = new RTCPeerConnection(rtcConfig);

    peerConnection.onicecandidate = (event) => {
      if (event.candidate && receiverWs && receiverWs.readyState === WebSocket.OPEN) {
        receiverWs.send(JSON.stringify({
          type: 'signal_candidate',
          candidate: event.candidate,
        }));
      }
    };

    peerConnection.ondatachannel = (event) => {
      dataChannel = event.channel;
      dataChannel.binaryType = 'arraybuffer';
      dataChannel.onopen = () => {
        updateTransferModeUI('rx', 'lan');
      };
      dataChannel.onmessage = async (e) => {
        try {
          const msg = JSON.parse(e.data);
          if (msg.type === 'chunk') {
            if (dataChannel && dataChannel.readyState === 'open') {
              dataChannel.send(JSON.stringify({ type: 'rx_ack', index: msg.index }));
            }
            await processReceivedChunk(msg.index, msg.data, transferData);
          }
        } catch (err) {
          console.warn('Error handling LAN chunk:', err);
        }
      };
    };

    await peerConnection.setRemoteDescription(new RTCSessionDescription({ type: 'offer', sdp }));
    const answer = await peerConnection.createAnswer();
    await peerConnection.setLocalDescription(answer);

    if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
      receiverWs.send(JSON.stringify({
        type: 'signal_answer',
        sdp: answer.sdp,
      }));
    }
  } catch (err) {
    console.warn('Error handling signal offer on receiver:', err);
    updateTransferModeUI('rx', 'cloud');
  }
}

async function processReceivedChunk(index, b64Data, transferData) {
  if (rxReceivedIndices.has(index)) return;

  const chunkBytes = base64ToUint8Array(b64Data);

  // Stream chunk directly to disk without storing in memory
  if (rxWritableStream) {
    await rxWritableStream.write({
      type: 'write',
      position: index * transferData.chunk_size,
      data: chunkBytes,
    });
  } else {
    rxChunksMap.set(index, chunkBytes);
  }

  rxReceivedIndices.add(index);
  rxBytesReceived += chunkBytes.length;
  setChunkState('rx', index, 'acked', transferData.total_chunks);

  const total = transferData.total_chunks;
  const pct = Math.round((rxReceivedIndices.size / total) * 100);
  rxProgressFill.style.width = `${pct}%`;
  rxProgressPct.textContent = `${pct}%`;

  const elapsedSec = (Date.now() - rxStartTime) / 1000;
  if (elapsedSec > 0.2) {
    const speedBytesPerSec = rxBytesReceived / elapsedSec;
    const mbps = (speedBytesPerSec / (1024 * 1024)).toFixed(1);
    rxSpeed.textContent = `${mbps} MB/s`;
    recordSpeedSample('rx', parseFloat(mbps));

    const remainingBytes = transferData.filesize - rxBytesReceived;
    const etaSec = speedBytesPerSec > 0 ? remainingBytes / speedBytesPerSec : 0;
    rxEta.textContent = formatDuration(etaSec);
  }

  rxProgressBytes.textContent = `${formatBytes(rxBytesReceived)} / ${formatBytes(transferData.filesize)}`;
  rxProgressChunks.textContent = `Chunk ${rxReceivedIndices.size}/${total}`;

  if (rxReceivedIndices.size === total) {
    completeReceiverFile(transferData.filename, transferData.mime_type);
  }
}

async function handleFileSelected(file) {
  if (!file || file.size <= 0) {
    alert('Please select a valid non-empty file.');
    return;
  }

  currentFile = file;
  currentCode = generate4DigitCode();
  totalChunks = Math.ceil(file.size / chunkSize);
  ackedChunks.clear();
  inFlightChunks.clear();
  inFlightTimestamps.clear();
  bytesSent = 0;
  isStreaming = false;
  isPaused = false;
  transferMode = 'detecting';

  sendFileName.textContent = file.name;
  sendFileSize.textContent = formatBytes(file.size);
  senderCodeDisplay.textContent = currentCode;

  senderStepPick.classList.add('hidden');
  senderStepTransfer.classList.remove('hidden');
  senderStatusText.textContent = 'Waiting for receiver to connect...';
  senderStatusDot.className = 'status-dot';
  senderProgressBox.classList.add('hidden');
  senderActiveControls.classList.add('hidden');
  btnSendAgain.classList.add('hidden');
  btnPauseSend.classList.remove('hidden');
  btnResumeSend.classList.add('hidden');

  // Initialize Telemetry & Transfer Mode
  updateTransferModeUI('sender', 'detecting');
  initChunkMatrix('sender', totalChunks);
  setupLocalDeviceCard('sender');
  resetSparkline('sender');

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
    if (!res.ok) throw new Error('Failed to create transfer session');
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
    startRttPing('sender', senderWs);
    sendPeerInfo('sender', senderWs);
  };

  senderWs.onmessage = async (e) => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'peer_connected' && msg.role === 'receiver') {
        sendPeerInfo('sender', senderWs);
        // Start 3-second LAN connection race!
        if (transferMode === 'detecting' && !lanDetectionTimer) {
          initSenderWebRTC();
          lanDetectionTimer = setTimeout(() => {
            if (transferMode !== 'lan') {
              fallbackToCloudRelay('sender');
            }
          }, 3000);
        }
      } else if (msg.type === 'signal_answer') {
        if (peerConnection) {
          try {
            await peerConnection.setRemoteDescription(new RTCSessionDescription({ type: 'answer', sdp: msg.sdp }));
          } catch (err) {
            console.warn('Error setting remote answer:', err);
            fallbackToCloudRelay('sender');
          }
        }
      } else if (msg.type === 'signal_candidate') {
        if (peerConnection && msg.candidate) {
          try {
            await peerConnection.addIceCandidate(new RTCIceCandidate(msg.candidate));
          } catch (_) {}
        }
      } else if (msg.type === 'peer_info') {
        if (msg.role === 'receiver') {
          handlePeerInfo('sender', msg);
        }
      } else if (msg.type === 'pong') {
        handlePong('sender', msg);
      } else if (msg.type === 'request_missing') {
        if (!isPaused) startStreamingChunks(msg.missing_chunks);
      } else if (msg.type === 'chunk_ack') {
        handleChunkAck(msg.index);
      } else if (msg.type === 'peer_disconnected' && msg.role === 'receiver') {
        isStreaming = false;
        senderStatusDot.className = 'status-dot red';
        senderStatusText.textContent = 'Receiver disconnected. Transfer paused.';
        btnPauseSend.classList.add('hidden');
        btnResumeSend.classList.remove('hidden');
      } else if (msg.type === 'peer_cancelled') {
        isStreaming = false;
        senderStatusDot.className = 'status-dot red';
        senderStatusText.textContent = 'Receiver cancelled transfer.';
        senderActiveControls.classList.add('hidden');
        btnSendAgain.classList.remove('hidden');
      }
    } catch (err) {
      console.warn('Sender WS error:', err);
    }
  };
}

function startStreamingChunks(missingList) {
  if (isPaused) return;

  senderActiveControls.classList.remove('hidden');
  btnPauseSend.classList.remove('hidden');
  btnResumeSend.classList.add('hidden');

  if (isStreaming) {
    if (missingList && missingList.length > 0) {
      streamFileChunks(missingList);
    }
    return;
  }

  isStreaming = true;
  senderStartTime = Date.now();
  senderStatusText.textContent = 'Streaming file live to receiver...';
  senderStatusDot.className = 'status-dot green';
  senderProgressBox.classList.remove('hidden');

  streamFileChunks(missingList);
}

async function streamFileChunks(missingList) {
  const queue = missingList && missingList.length > 0
    ? [...missingList]
    : Array.from({ length: totalChunks }, (_, i) => i);

  while (queue.length > 0) {
    if (!isStreaming || isPaused) break;

    const idx = queue.shift();
    if (ackedChunks.has(idx)) continue;

    // Concurrency limit with ACK timeout watchdog
    while (inFlightChunks.size >= 4) {
      if (!isStreaming || isPaused) break;

      // Check for stalled chunks (> 3500ms without ACK)
      const now = Date.now();
      for (const [inflightIdx, sentTs] of inFlightTimestamps.entries()) {
        if (now - sentTs > 3500) {
          inFlightChunks.delete(inflightIdx);
          inFlightTimestamps.delete(inflightIdx);
          if (!ackedChunks.has(inflightIdx) && !queue.includes(inflightIdx)) {
            queue.unshift(inflightIdx); // Re-queue stalled chunk
          }
        }
      }

      await new Promise(r => setTimeout(r, 15));
    }
    if (!isStreaming || isPaused) break;

    inFlightChunks.add(idx);
    inFlightTimestamps.set(idx, Date.now());
    setChunkState('sender', idx, 'inflight');

    const start = idx * chunkSize;
    const end = Math.min(start + chunkSize, currentFile.size);
    const slice = currentFile.slice(start, end);
    const buffer = await slice.arrayBuffer();
    const b64 = arrayBufferToBase64(buffer);

    const chunkPayload = JSON.stringify({
      type: 'chunk',
      index: idx,
      total: totalChunks,
      size: buffer.byteLength,
      data: b64,
    });

    if (transferMode === 'lan' && dataChannel && dataChannel.readyState === 'open') {
      // Direct LAN stream (zero server bandwidth)
      dataChannel.send(chunkPayload);
      if (dataChannel.bufferedAmount > 1024 * 1024) {
        await new Promise(resolve => {
          dataChannel.onbufferedamountlow = () => {
            dataChannel.onbufferedamountlow = null;
            resolve();
          };
        });
      }
    } else if (senderWs && senderWs.readyState === WebSocket.OPEN) {
      // Cloud Relay stream
      senderWs.send(chunkPayload);
    }

    await new Promise(r => setTimeout(r, 2));
  }
}

function handleChunkAck(index) {
  inFlightChunks.delete(index);
  inFlightTimestamps.delete(index);
  ackedChunks.add(index);
  bytesSent = Math.min(ackedChunks.size * chunkSize, currentFile.size);
  setChunkState('sender', index, 'acked');

  const pct = Math.round((ackedChunks.size / totalChunks) * 100);
  senderProgressFill.style.width = `${pct}%`;
  senderProgressPct.textContent = `${pct}%`;

  const elapsedSec = (Date.now() - senderStartTime) / 1000;
  if (elapsedSec > 0.2) {
    const speedBytesPerSec = bytesSent / elapsedSec;
    const mbps = (speedBytesPerSec / (1024 * 1024)).toFixed(1);
    senderSpeed.textContent = `${mbps} MB/s`;
    recordSpeedSample('sender', parseFloat(mbps));

    const remainingBytes = currentFile.size - bytesSent;
    const etaSec = speedBytesPerSec > 0 ? remainingBytes / speedBytesPerSec : 0;
    senderEta.textContent = formatDuration(etaSec);
  }

  senderProgressBytes.textContent = `${formatBytes(bytesSent)} / ${formatBytes(currentFile.size)}`;
  senderProgressChunks.textContent = `Chunk ${ackedChunks.size}/${totalChunks}`;

  if (ackedChunks.size === totalChunks) {
    senderStatusText.textContent = 'Transfer complete';
    senderStatusDot.className = 'status-dot green';
    senderActiveControls.classList.add('hidden');
    btnSendAgain.classList.remove('hidden');
    if (senderWs && senderWs.readyState === WebSocket.OPEN) {
      senderWs.send(JSON.stringify({ type: 'complete' }));
    }
    if (dataChannel && dataChannel.readyState === 'open') {
      try { dataChannel.send(JSON.stringify({ type: 'complete' })); } catch (_) {}
    }
  }
}

// Pause Transfer
btnPauseSend.addEventListener('click', pauseTransfer);

function pauseTransfer() {
  isStreaming = false;
  isPaused = true;
  inFlightChunks.clear();
  inFlightTimestamps.clear();

  senderStatusText.textContent = 'Transfer paused';
  senderStatusDot.className = 'status-dot';
  senderSpeed.textContent = '0.0 MB/s';
  senderEta.textContent = 'Paused';

  btnPauseSend.classList.add('hidden');
  btnResumeSend.classList.remove('hidden');

  if (senderWs && senderWs.readyState === WebSocket.OPEN) {
    senderWs.send(JSON.stringify({ type: 'pause' }));
  }
}

// Resume Transfer
btnResumeSend.addEventListener('click', resumeTransfer);

async function resumeTransfer() {
  if (!currentCode) return;

  senderStatusText.textContent = 'Resuming transfer...';
  senderStatusDot.className = 'status-dot';
  btnResumeSend.classList.add('hidden');
  btnPauseSend.classList.remove('hidden');

  try {
    const res = await fetch(`/api/transfers/${currentCode}`);
    if (res.ok) {
      isPaused = false;
      if (senderWs && senderWs.readyState === WebSocket.OPEN) {
        senderWs.send(JSON.stringify({ type: 'resume' }));
      }
      // Resume remaining unacknowledged chunks
      const allChunks = Array.from({ length: totalChunks }, (_, i) => i);
      const remaining = allChunks.filter(i => !ackedChunks.has(i));
      startStreamingChunks(remaining);
    }
  } catch (err) {
    senderStatusText.textContent = 'Could not fetch transfer state';
    senderStatusDot.className = 'status-dot red';
    btnResumeSend.classList.remove('hidden');
    btnPauseSend.classList.add('hidden');
  }
}

// Cancel Transfer
btnCancelSend.addEventListener('click', cancelTransfer);

function cancelTransfer() {
  isStreaming = false;
  isPaused = false;

  if (senderWs && senderWs.readyState === WebSocket.OPEN) {
    senderWs.send(JSON.stringify({ type: 'cancel' }));
    senderWs.close();
    senderWs = null;
  }

  if (currentCode) {
    fetch(`/api/transfers/${currentCode}`, { method: 'DELETE' }).catch(() => {});
  }

  resetSender();
}

btnSendAgain.addEventListener('click', () => {
  resetSender();
  fileInput.click();
});

function resetSender() {
  if (senderWs) {
    senderWs.close();
    senderWs = null;
  }
  currentFile = null;
  currentCode = null;
  isStreaming = false;
  isPaused = false;
  ackedChunks.clear();
  inFlightChunks.clear();
  inFlightTimestamps.clear();
  stopRttPing('sender');
  resetChunkMatrix('sender');
  resetSparkline('sender');
  if (lanDetectionTimer) {
    clearTimeout(lanDetectionTimer);
    lanDetectionTimer = null;
  }
  if (dataChannel) {
    try { dataChannel.close(); } catch (_) {}
    dataChannel = null;
  }
  if (peerConnection) {
    try { peerConnection.close(); } catch (_) {}
    peerConnection = null;
  }
  transferMode = 'detecting';
  updateTransferModeUI('sender', 'detecting');
  senderStepPick.classList.remove('hidden');
  senderStepTransfer.classList.add('hidden');
  senderActiveControls.classList.add('hidden');
  if (senderPeerPill) senderPeerPill.classList.add('hidden');
  fileInput.value = '';
}

// Receiver Flow
btnStartReceive.addEventListener('click', startReceiving);

receiveCodeInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') startReceiving();
});

receiveCodeInput.addEventListener('input', () => {
  receiveCodeInput.value = receiveCodeInput.value.trim().toUpperCase();
});

async function startReceiving() {
  const code = receiveCodeInput.value.trim().toUpperCase();
  if (!code) {
    alert('Please enter a 4-digit code');
    return;
  }

  try {
    const res = await fetch(`/api/transfers/${code}`);
    if (!res.ok) throw new Error('Transfer code not found or expired');
    const data = await res.json();

    rxFileName.textContent = data.filename;
    rxFileSize.textContent = formatBytes(data.filesize);

    // Direct-to-Disk Streamer (FileSystem Access API)
    rxWritableStream = null;
    rxChunksMap.clear();
    rxReceivedIndices.clear();

    if ('showSaveFilePicker' in window) {
      try {
        const fileHandle = await window.showSaveFilePicker({
          suggestedName: data.filename,
        });
        rxWritableStream = await fileHandle.createWritable();
      } catch (pickerErr) {
        if (pickerErr.name === 'AbortError') return; // User cancelled picker
        console.warn('File picker unavailable, falling back to memory assembly:', pickerErr);
      }
    }

    receiverStepInput.classList.add('hidden');
    receiverStepTransfer.classList.remove('hidden');
    rxActiveControls.classList.remove('hidden');
    rxProgressBox.classList.remove('hidden');
    rxClearedBox.classList.add('hidden');

    rxStatusText.textContent = rxWritableStream
      ? 'Connected. Streaming directly to disk...'
      : 'Connected. Receiving stream...';
    rxStatusDot.className = 'status-dot green';
    rxProgressFill.style.width = '0%';
    rxProgressPct.textContent = '0%';
    rxSpeed.textContent = '0.0 MB/s';
    rxEta.textContent = '--';
    rxProgressBytes.textContent = `0 B / ${formatBytes(data.filesize)}`;
    rxProgressChunks.textContent = `Chunk 0/${data.total_chunks}`;

    rxStartTime = Date.now();
    rxBytesReceived = 0;
    transferMode = 'detecting';
    updateTransferModeUI('rx', 'detecting');

    // Initialize Receiver Telemetry
    initChunkMatrix('rx', data.total_chunks);
    setupLocalDeviceCard('rx');
    resetSparkline('rx');

    connectReceiverWebSocket(code, data);
  } catch (err) {
    alert(err.message);
  }
}

function connectReceiverWebSocket(code, transferData) {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  receiverWs = new WebSocket(`${protocol}//${window.location.host}/ws/transfer/${code}?role=receiver`);

  receiverWs.onopen = () => {
    startRttPing('rx', receiverWs);
    sendPeerInfo('rx', receiverWs);
    receiverWs.send(JSON.stringify({ type: 'resume_request', missing_chunks: [] }));
  };

  receiverWs.onmessage = async (e) => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'peer_disconnected' && msg.role === 'sender') {
        rxStatusDot.className = 'status-dot red';
        rxStatusText.textContent = 'Sender disconnected. Waiting...';
      } else if (msg.type === 'peer_connected' && msg.role === 'sender') {
        rxStatusDot.className = 'status-dot green';
        rxStatusText.textContent = 'Receiving stream...';
        sendPeerInfo('rx', receiverWs);
      } else if (msg.type === 'signal_offer') {
        await handleReceiverSignalOffer(msg.sdp, transferData);
      } else if (msg.type === 'signal_candidate') {
        if (peerConnection && msg.candidate) {
          try {
            await peerConnection.addIceCandidate(new RTCIceCandidate(msg.candidate));
          } catch (_) {}
        }
      } else if (msg.type === 'peer_info') {
        if (msg.role === 'sender') {
          handlePeerInfo('rx', msg);
        }
      } else if (msg.type === 'pong') {
        handlePong('rx', msg);
      } else if (msg.type === 'peer_paused') {
        rxStatusDot.className = 'status-dot';
        rxStatusText.textContent = 'Sender paused transfer';
        rxSpeed.textContent = '0.0 MB/s';
        rxEta.textContent = 'Paused';
      } else if (msg.type === 'peer_resumed') {
        rxStatusDot.className = 'status-dot green';
        rxStatusText.textContent = 'Receiving stream...';
      } else if (msg.type === 'peer_cancelled') {
        rxStatusDot.className = 'status-dot red';
        rxStatusText.textContent = 'Transfer cancelled by sender';
        rxActiveControls.classList.add('hidden');
        rxClearedBox.classList.remove('hidden');
        rxStatusFinished.textContent = 'Transfer cancelled';
        if (rxWritableStream) {
          try { await rxWritableStream.abort(); } catch (_) {}
          rxWritableStream = null;
        }
      } else if (msg.type === 'chunk_data') {
        // Send ACK back over WebSocket relay
        if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
          receiverWs.send(JSON.stringify({ type: 'rx_ack', index: msg.index }));
        }
        await processReceivedChunk(msg.index, msg.data, transferData);
      } else if (msg.type === 'completed') {
        completeReceiverFile(transferData.filename, transferData.mime_type);
      }
    } catch (err) {
      console.warn('Receiver message error:', err);
    }
  };
}

async function completeReceiverFile(filename, mimeType) {
  rxStatusText.textContent = 'Transfer complete';
  rxStatusDot.className = 'status-dot green';
  rxProgressFill.style.width = '100%';
  rxProgressPct.textContent = '100%';
  rxEta.textContent = 'Done';
  rxActiveControls.classList.add('hidden');

  // If using FileSystem Access API, close writable stream
  if (rxWritableStream) {
    try {
      await rxWritableStream.close();
    } catch (closeErr) {
      console.error('Error closing file stream:', closeErr);
    }
    rxWritableStream = null;
    rxStatusFinished.textContent = 'File saved directly to your device.';
  } else if (rxChunksMap.size > 0) {
    // Fallback: assemble Blob in memory and trigger download
    const sortedChunks = [];
    for (let i = 0; i < rxReceivedIndices.size; i++) {
      if (rxChunksMap.has(i)) {
        sortedChunks.push(rxChunksMap.get(i));
      }
    }
    const blob = new Blob(sortedChunks, { type: mimeType || 'application/octet-stream' });
    const url = URL.createObjectURL(blob);
    const tempLink = document.createElement('a');
    tempLink.href = url;
    tempLink.download = filename || 'download';
    document.body.appendChild(tempLink);
    tempLink.click();
    document.body.removeChild(tempLink);
    setTimeout(() => {
      try { URL.revokeObjectURL(url); } catch (_) {}
    }, 60000);
    rxChunksMap.clear();
    rxStatusFinished.textContent = 'File downloaded to your browser.';
  }

  rxClearedBox.classList.remove('hidden');
}

btnCancelReceive.addEventListener('click', cancelReceive);

async function cancelReceive() {
  if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
    receiverWs.send(JSON.stringify({ type: 'cancel' }));
    receiverWs.close();
    receiverWs = null;
  }
  if (rxWritableStream) {
    try { await rxWritableStream.abort(); } catch (_) {}
    rxWritableStream = null;
  }
  rxChunksMap.clear();
  rxReceivedIndices.clear();
  resetReceiver();
}

btnReceiveAgain.addEventListener('click', () => {
  resetReceiver();
  receiveCodeInput.focus();
});

function resetReceiver() {
  if (receiverWs) {
    receiverWs.close();
    receiverWs = null;
  }
  if (rxWritableStream) {
    try { rxWritableStream.abort(); } catch (_) {}
    rxWritableStream = null;
  }
  rxChunksMap.clear();
  rxReceivedIndices.clear();
  stopRttPing('rx');
  resetChunkMatrix('rx');
  resetSparkline('rx');
  if (dataChannel) {
    try { dataChannel.close(); } catch (_) {}
    dataChannel = null;
  }
  if (peerConnection) {
    try { peerConnection.close(); } catch (_) {}
    peerConnection = null;
  }
  transferMode = 'detecting';
  updateTransferModeUI('rx', 'detecting');
  receiveCodeInput.value = '';
  receiverStepInput.classList.remove('hidden');
  receiverStepTransfer.classList.add('hidden');
  rxClearedBox.classList.add('hidden');
  rxProgressBox.classList.remove('hidden');
  rxActiveControls.classList.remove('hidden');
  if (rxPeerPill) rxPeerPill.classList.add('hidden');
}

// Initial UI setup
updateLocalDeviceDisplay();

