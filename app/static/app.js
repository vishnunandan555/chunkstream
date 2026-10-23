// ChunkStream Frontend Application Logic

// State
let selectedFile = null;
let currentTransferId = null;
let senderWs = null;
let receiverWs = null;
let isSenderPaused = false;
let isSending = false;
let senderChunkSize = 262144; // 256 KB
let totalChunks = 0;
let ackedChunks = new Set();
let inFlightChunks = new Set();
let failedChunks = new Set();
let retryCount = 0;
let bytesSentTotal = 0;
let transferStartTime = 0;

let rxTransferId = null;
let rxChunksReceived = new Set();
let rxTotalChunks = 0;
let rxAssembledParts = {};

// Helpers
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i];
}

function generateRandomCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let result = 'CS-';
  for (let i = 0; i < 4; i++) {
    result += chars.charAt(Math.floor(Math.random() * chars.length));
  }
  return result;
}

// SHA-256 for browser
async function computeSha256(arrayBuffer) {
  const hashBuffer = await crypto.subtle.digest('SHA-256', arrayBuffer);
  const hashArray = Array.from(new Uint8Array(hashBuffer));
  return hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
}

function arrayBufferToBase64(buffer) {
  let binary = '';
  const bytes = new Uint8Array(buffer);
  const len = bytes.byteLength;
  for (let i = 0; i < len; i++) {
    binary += String.fromCharCode(bytes[i]);
  }
  return window.btoa(binary);
}

// DOM Elements
const tabBtns = document.querySelectorAll('.nav-tab');
const tabPanels = document.querySelectorAll('.tab-panel');
const healthIndicator = document.getElementById('health-indicator');
const healthText = document.getElementById('health-text');
const incidentStatusIndicator = document.getElementById('incident-status-indicator');

// Tab Navigation
tabBtns.forEach(btn => {
  btn.addEventListener('click', () => {
    const targetTab = btn.getAttribute('data-tab');
    tabBtns.forEach(b => b.classList.remove('active'));
    tabPanels.forEach(p => p.classList.remove('active'));
    btn.classList.add('active');
    document.getElementById(`panel-${targetTab}`).classList.add('active');
    if (targetTab === 'receiver') {
      fetchActiveTransfers();
    }
  });
});

// ----------------------------------------------------
// SENDER LOGIC
// ----------------------------------------------------
const senderDropZone = document.getElementById('sender-drop-zone');
const senderFileInput = document.getElementById('sender-file-input');
const fileSummaryBox = document.getElementById('file-summary-box');
const selectedFileName = document.getElementById('selected-file-name');
const selectedFileSize = document.getElementById('selected-file-size');
const chunkSizeSelect = document.getElementById('chunk-size-select');
const chunkCalcText = document.getElementById('chunk-calc-text');
const transferIdInput = document.getElementById('transfer-id-input');
const copyTransferIdBtn = document.getElementById('copy-transfer-id-btn');
const regenTransferIdBtn = document.getElementById('regen-transfer-id-btn');
const btnStartTransfer = document.getElementById('btn-start-transfer');
const btnPauseTransfer = document.getElementById('btn-pause-transfer');
const btnResumeTransfer = document.getElementById('btn-resume-transfer');
const senderProgressBar = document.getElementById('sender-progress-bar');
const senderProgressPct = document.getElementById('sender-progress-pct');
const senderChunksAcked = document.getElementById('sender-chunks-acked');
const senderSpeed = document.getElementById('sender-speed');
const senderRetries = document.getElementById('sender-retries');
const senderStatusBadge = document.getElementById('sender-status-badge');
const senderChunkGrid = document.getElementById('sender-chunk-grid');

// Initialize Transfer Code
currentTransferId = generateRandomCode();
transferIdInput.value = currentTransferId;

regenTransferIdBtn.addEventListener('click', () => {
  if (isSending) return;
  currentTransferId = generateRandomCode();
  transferIdInput.value = currentTransferId;
});

copyTransferIdBtn.addEventListener('click', () => {
  navigator.clipboard.writeText(transferIdInput.value).then(() => {
    copyTransferIdBtn.textContent = 'Copied!';
    setTimeout(() => copyTransferIdBtn.textContent = 'Copy', 1500);
  });
});

// File Selection & Drag-and-Drop
senderDropZone.addEventListener('click', () => senderFileInput.click());

senderDropZone.addEventListener('dragover', (e) => {
  e.preventDefault();
  senderDropZone.classList.add('dragover');
});

senderDropZone.addEventListener('dragleave', () => {
  senderDropZone.classList.remove('dragover');
});

senderDropZone.addEventListener('drop', (e) => {
  e.preventDefault();
  senderDropZone.classList.remove('dragover');
  if (e.dataTransfer.files.length > 0) {
    handleFileSelect(e.dataTransfer.files[0]);
  }
});

senderFileInput.addEventListener('change', (e) => {
  if (e.target.files.length > 0) {
    handleFileSelect(e.target.files[0]);
  }
});

chunkSizeSelect.addEventListener('change', (e) => {
  senderChunkSize = parseInt(e.target.value, 10);
  if (selectedFile) {
    recalcChunks();
  }
});

function handleFileSelect(file) {
  selectedFile = file;
  selectedFileName.textContent = file.name;
  selectedFileSize.textContent = formatBytes(file.size);
  fileSummaryBox.classList.remove('hidden');
  btnStartTransfer.disabled = false;
  recalcChunks();
}

function recalcChunks() {
  totalChunks = Math.ceil(selectedFile.size / senderChunkSize);
  chunkCalcText.textContent = `${totalChunks} chunks planned`;
  buildSenderChunkGrid(totalChunks);
}

function buildSenderChunkGrid(count) {
  senderChunkGrid.innerHTML = '';
  ackedChunks.clear();
  inFlightChunks.clear();
  failedChunks.clear();

  for (let i = 0; i < count; i++) {
    const block = document.createElement('div');
    block.className = 'chunk-block';
    block.id = `sender-chunk-${i}`;
    block.title = `Chunk #${i}`;
    senderChunkGrid.appendChild(block);
  }
  senderChunksAcked.textContent = `0 / ${count}`;
  senderProgressPct.textContent = '0%';
  senderProgressBar.style.width = '0%';
}

function updateChunkBlock(index, status) {
  const el = document.getElementById(`sender-chunk-${index}`);
  if (!el) return;
  el.className = `chunk-block status-${status}`;
}

// Start Transfer
btnStartTransfer.addEventListener('click', async () => {
  if (!selectedFile || isSending) return;

  btnStartTransfer.disabled = true;
  btnPauseTransfer.disabled = false;
  btnResumeTransfer.disabled = true;
  chunkSizeSelect.disabled = true;
  senderFileInput.disabled = true;
  isSending = true;
  isSenderPaused = false;
  retryCount = 0;
  senderRetries.textContent = '0';
  bytesSentTotal = 0;
  transferStartTime = Date.now();

  senderStatusBadge.textContent = 'CONNECTING';
  senderStatusBadge.className = 'status-badge status-active';

  // 1. Compute whole-file hash if small/medium or quick init
  let fileHash = '';
  try {
    const fileBuffer = await selectedFile.slice(0, Math.min(selectedFile.size, 5 * 1024 * 1024)).arrayBuffer();
    fileHash = await computeSha256(fileBuffer);
  } catch (err) {
    console.warn("Could not compute preview hash:", err);
  }

  // 2. Initialize transfer metadata on backend
  try {
    const res = await fetch('/api/transfers', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        transfer_id: currentTransferId,
        filename: selectedFile.name,
        filesize: selectedFile.size,
        chunk_size: senderChunkSize,
        total_chunks: totalChunks,
        mime_type: selectedFile.type || 'application/octet-stream',
        checksum: fileHash
      })
    });
    if (!res.ok) throw new Error('Failed to create transfer session on server');
  } catch (e) {
    alert(`Transfer initialization error: ${e.message}`);
    btnStartTransfer.disabled = false;
    isSending = false;
    return;
  }

  // 3. Connect WebSocket
  connectSenderWebSocket();
});

function connectSenderWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/ws/transfer/${currentTransferId}?role=sender`;

  senderWs = new WebSocket(wsUrl);

  senderWs.onopen = () => {
    senderStatusBadge.textContent = 'STREAMING';
    senderStatusBadge.className = 'status-badge status-active';
    streamChunks();
  };

  senderWs.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleSenderSocketMessage(msg);
    } catch (e) {
      console.error("Invalid message received:", e);
    }
  };

  senderWs.onclose = () => {
    if (isSending && !isSenderPaused) {
      senderStatusBadge.textContent = 'DISCONNECTED';
      senderStatusBadge.className = 'status-badge status-error';
      btnResumeTransfer.disabled = false;
    }
  };
}

function handleSenderSocketMessage(msg) {
  if (msg.type === 'chunk_ack') {
    const idx = msg.index;
    inFlightChunks.delete(idx);
    ackedChunks.add(idx);
    updateChunkBlock(idx, 'acked');

    const pct = Math.round((ackedChunks.size / totalChunks) * 100);
    senderProgressPct.textContent = `${pct}%`;
    senderProgressBar.style.width = `${pct}%`;
    senderChunksAcked.textContent = `${ackedChunks.size} / ${totalChunks}`;

    // Speed calculation
    const elapsedSec = (Date.now() - transferStartTime) / 1000;
    if (elapsedSec > 0.5) {
      const mbps = ((bytesSentTotal / (1024 * 1024)) / elapsedSec).toFixed(2);
      senderSpeed.textContent = `${mbps} MB/s`;
    }

    if (ackedChunks.size === totalChunks) {
      finishTransfer();
    }
  } else if (msg.type === 'chunk_error') {
    const idx = msg.index;
    inFlightChunks.delete(idx);
    failedChunks.add(idx);
    retryCount++;
    senderRetries.textContent = retryCount;
    updateChunkBlock(idx, 'failed');

    // Auto-retry dropped chunk if retryable
    if (msg.retryable && !isSenderPaused) {
      setTimeout(() => {
        if (!ackedChunks.has(idx) && !isSenderPaused) {
          sendSingleChunk(idx);
        }
      }, 600);
    }
  } else if (msg.type === 'request_missing') {
    const missing = msg.missing_chunks || [];
    console.log("Peer requested missing chunks:", missing);
    missing.forEach(idx => {
      if (!ackedChunks.has(idx)) {
        sendSingleChunk(idx);
      }
    });
  }
}

async function streamChunks() {
  for (let i = 0; i < totalChunks; i++) {
    if (isSenderPaused || !isSending) break;
    if (ackedChunks.has(i)) continue;

    // Concurrency throttle
    while (inFlightChunks.size >= 3 && !isSenderPaused) {
      await new Promise(r => setTimeout(r, 40));
    }

    if (isSenderPaused) break;
    sendSingleChunk(i);
    // Slight throttle for smooth visualization
    await new Promise(r => setTimeout(r, 25));
  }
}

async function sendSingleChunk(index) {
  if (!senderWs || senderWs.readyState !== WebSocket.OPEN) return;
  if (ackedChunks.has(index)) return;

  inFlightChunks.add(index);
  updateChunkBlock(index, 'sending');

  const start = index * senderChunkSize;
  const end = Math.min(start + senderChunkSize, selectedFile.size);
  const slice = selectedFile.slice(start, end);
  const buffer = await slice.arrayBuffer();
  const hash = await computeSha256(buffer);
  const b64Data = arrayBufferToBase64(buffer);

  bytesSentTotal += buffer.byteLength;

  senderWs.send(JSON.stringify({
    type: 'chunk',
    index: index,
    total: totalChunks,
    data: b64Data,
    hash: hash
  }));
}

function finishTransfer() {
  isSending = false;
  senderStatusBadge.textContent = 'COMPLETED';
  senderStatusBadge.className = 'status-badge status-success';
  btnPauseTransfer.disabled = true;
  btnResumeTransfer.disabled = true;

  if (senderWs && senderWs.readyState === WebSocket.OPEN) {
    senderWs.send(JSON.stringify({ type: 'complete' }));
  }
}

btnPauseTransfer.addEventListener('click', () => {
  isSenderPaused = true;
  senderStatusBadge.textContent = 'PAUSED';
  senderStatusBadge.className = 'status-badge status-idle';
  btnPauseTransfer.disabled = true;
  btnResumeTransfer.disabled = false;
});

btnResumeTransfer.addEventListener('click', () => {
  isSenderPaused = false;
  isSending = true;
  senderStatusBadge.textContent = 'RESUMING';
  senderStatusBadge.className = 'status-badge status-active';
  btnPauseTransfer.disabled = false;
  btnResumeTransfer.disabled = true;

  if (!senderWs || senderWs.readyState !== WebSocket.OPEN) {
    connectSenderWebSocket();
  } else {
    streamChunks();
  }
});

// ----------------------------------------------------
// RECEIVER LOGIC
// ----------------------------------------------------
const rxTransferIdInput = document.getElementById('rx-transfer-id-input');
const rxConnectBtn = document.getElementById('rx-connect-btn');
const activeTransfersList = document.getElementById('active-transfers-list');
const refreshTransfersBtn = document.getElementById('refresh-transfers-btn');
const rxMetadataCard = document.getElementById('rx-metadata-card');
const rxMetaFilename = document.getElementById('rx-meta-filename');
const rxMetaFilesize = document.getElementById('rx-meta-filesize');
const rxMetaTotalChunks = document.getElementById('rx-meta-total-chunks');
const rxMetaStatus = document.getElementById('rx-meta-status');
const rxDisconnectBtn = document.getElementById('rx-disconnect-btn');
const rxResumeBtn = document.getElementById('rx-resume-btn');
const rxDownloadBtn = document.getElementById('rx-download-btn');
const rxStatusBadge = document.getElementById('rx-status-badge');
const rxProgressPct = document.getElementById('rx-progress-pct');
const rxChunksCount = document.getElementById('rx-chunks-count');
const rxMissingCount = document.getElementById('rx-missing-count');
const rxStreamStatus = document.getElementById('rx-stream-status');
const rxProgressBar = document.getElementById('rx-progress-bar');
const rxChunkGrid = document.getElementById('rx-chunk-grid');

refreshTransfersBtn.addEventListener('click', fetchActiveTransfers);

async function fetchActiveTransfers() {
  try {
    const res = await fetch('/api/transfers?limit=8');
    const list = await res.json();
    activeTransfersList.innerHTML = '';
    if (list.length === 0) {
      activeTransfersList.innerHTML = '<span class="empty-subtext">No active transfers on server</span>';
      return;
    }
    list.forEach(t => {
      const chip = document.createElement('button');
      chip.className = 'transfer-chip';
      chip.textContent = `${t.id} (${t.filename} - ${t.progress_percentage}%)`;
      chip.addEventListener('click', () => {
        rxTransferIdInput.value = t.id;
        connectReceiver();
      });
      activeTransfersList.appendChild(chip);
    });
  } catch (err) {
    console.error("Could not fetch active transfers:", err);
  }
}

rxConnectBtn.addEventListener('click', () => {
  connectReceiver();
});

async function connectReceiver() {
  const code = rxTransferIdInput.value.trim().toUpperCase();
  if (!code) {
    alert('Please enter a transfer code');
    return;
  }
  rxTransferId = code;

  // Fetch transfer details from server
  try {
    const res = await fetch(`/api/transfers/${rxTransferId}`);
    if (!res.ok) throw new Error(`Transfer ${rxTransferId} not found`);
    const data = await res.json();

    rxMetadataCard.classList.remove('hidden');
    rxMetaFilename.textContent = data.filename;
    rxMetaFilesize.textContent = formatBytes(data.filesize);
    rxMetaTotalChunks.textContent = data.total_chunks;
    rxMetaStatus.textContent = data.status.toUpperCase();

    rxTotalChunks = data.total_chunks;
    buildReceiverChunkGrid(data.total_chunks);

    // Populate already acknowledged chunks
    rxChunksReceived.clear();
    (data.acknowledged_chunks || []).forEach(idx => {
      rxChunksReceived.add(idx);
      updateRxChunkBlock(idx, 'acked');
    });
    updateRxStats();
  } catch (e) {
    alert(`Could not connect: ${e.message}`);
    return;
  }

  // Connect WebSocket
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/ws/transfer/${rxTransferId}?role=receiver`;
  receiverWs = new WebSocket(wsUrl);

  receiverWs.onopen = () => {
    rxStatusBadge.textContent = 'CONNECTED';
    rxStatusBadge.className = 'status-badge status-active';
    rxDisconnectBtn.disabled = false;
    rxResumeBtn.disabled = false;
    rxStreamStatus.textContent = 'Listening';

    // Send resume request to sync missing chunks
    receiverWs.send(JSON.stringify({ type: 'resume_request' }));
  };

  receiverWs.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleReceiverMessage(msg);
    } catch (e) {
      console.error("Receiver error:", e);
    }
  };

  receiverWs.onclose = () => {
    rxStatusBadge.textContent = 'DISCONNECTED';
    rxStatusBadge.className = 'status-badge status-idle';
    rxDisconnectBtn.disabled = true;
    rxStreamStatus.textContent = 'Offline';
  };
}

function buildReceiverChunkGrid(count) {
  rxChunkGrid.innerHTML = '';
  for (let i = 0; i < count; i++) {
    const block = document.createElement('div');
    block.className = 'chunk-block';
    block.id = `rx-chunk-${i}`;
    block.title = `Chunk #${i}`;
    rxChunkGrid.appendChild(block);
  }
}

function updateRxChunkBlock(index, status) {
  const el = document.getElementById(`rx-chunk-${index}`);
  if (!el) return;
  el.className = `chunk-block status-${status}`;
}

function updateRxStats() {
  const count = rxChunksReceived.size;
  const missing = rxTotalChunks - count;
  const pct = rxTotalChunks > 0 ? Math.round((count / rxTotalChunks) * 100) : 0;

  rxChunksCount.textContent = `${count} / ${rxTotalChunks}`;
  rxMissingCount.textContent = missing;
  rxProgressPct.textContent = `${pct}%`;
  rxProgressBar.style.width = `${pct}%`;

  if (count === rxTotalChunks && rxTotalChunks > 0) {
    rxStatusBadge.textContent = 'COMPLETED';
    rxStatusBadge.className = 'status-badge status-success';
    rxStreamStatus.textContent = 'All Chunks Stored';
    rxDownloadBtn.classList.remove('hidden');
    rxDownloadBtn.href = `/api/transfers/${rxTransferId}/download`;
  }
}

function handleReceiverMessage(msg) {
  if (msg.type === 'chunk_data') {
    const idx = msg.index;
    rxChunksReceived.add(idx);
    updateRxChunkBlock(idx, 'acked');
    updateRxStats();
  } else if (msg.type === 'progress') {
    rxMetaStatus.textContent = (msg.status || 'transferring').toUpperCase();
  } else if (msg.type === 'resume_state') {
    (msg.acknowledged_chunks || []).forEach(idx => {
      rxChunksReceived.add(idx);
      updateRxChunkBlock(idx, 'acked');
    });
    updateRxStats();
  } else if (msg.type === 'completed') {
    rxStatusBadge.textContent = 'COMPLETED';
    rxStatusBadge.className = 'status-badge status-success';
    rxDownloadBtn.classList.remove('hidden');
    rxDownloadBtn.href = msg.download_url || `/api/transfers/${rxTransferId}/download`;
  } else if (msg.type === 'resume_error') {
    alert(`Resume Failure Alert: ${msg.message}`);
    rxStreamStatus.textContent = 'Resume Error (F6)';
  }
}

// Receiver Controls
rxDisconnectBtn.addEventListener('click', () => {
  if (receiverWs) {
    receiverWs.close(1000, "User manual disconnect");
  }
});

rxResumeBtn.addEventListener('click', () => {
  if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
    receiverWs.send(JSON.stringify({ type: 'resume_request' }));
  } else {
    connectReceiver();
  }
});

// ----------------------------------------------------
// FAILURE SIMULATOR & LOG CONSOLE (Phase 4, Phase 6)
// ----------------------------------------------------
const triggerF3Btn = document.getElementById('trigger-f3-btn');
const triggerF4Btn = document.getElementById('trigger-f4-btn');
const triggerF5Btn = document.getElementById('trigger-f5-btn');
const triggerF6Btn = document.getElementById('trigger-f6-btn');
const triggerF1Btn = document.getElementById('trigger-f1-btn');
const triggerF2Btn = document.getElementById('trigger-f2-btn');
const btnResetScenarios = document.getElementById('btn-reset-scenarios');

const logStreamBox = document.getElementById('log-stream-box');
const logFilterSelect = document.getElementById('log-filter-select');
const btnCopyLogs = document.getElementById('btn-copy-logs');
const btnClearConsole = document.getElementById('btn-clear-console');

async function setScenario(scenarioKey) {
  try {
    const res = await fetch('/api/test/fail', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ scenario: scenarioKey })
    });
    const data = await res.json();
    incidentStatusIndicator.textContent = scenarioKey.toUpperCase();
    incidentStatusIndicator.className = 'tab-badge badge-active-incident';
    pollLogs();
  } catch (err) {
    console.error("Error setting scenario:", err);
  }
}

triggerF3Btn.addEventListener('click', () => setScenario('low_error'));
triggerF4Btn.addEventListener('click', () => setScenario('high_error'));
triggerF6Btn.addEventListener('click', () => setScenario('resume_failure'));
triggerF1Btn.addEventListener('click', () => setScenario('unhealthy'));

triggerF5Btn.addEventListener('click', async () => {
  if (!rxTransferId && !currentTransferId) {
    alert('Please enter or select an active transfer session first');
    return;
  }
  const id = rxTransferId || currentTransferId;
  await fetch(`/api/test/disconnect-receiver?transfer_id=${id}`, { method: 'POST' });
  pollLogs();
});

triggerF2Btn.addEventListener('click', async () => {
  if (confirm('F2 Crash Simulation: This will fatally crash the backend process. Are you sure?')) {
    try {
      await fetch('/api/test/crash', { method: 'POST' });
    } catch (e) {
      console.warn("Backend crash initiated");
    }
    alert('Crash signal dispatched. Backend process terminated with exit code 1.');
    checkHealth();
  }
});

btnResetScenarios.addEventListener('click', async () => {
  await fetch('/api/test/reset', { method: 'POST' });
  incidentStatusIndicator.textContent = 'NORMAL';
  incidentStatusIndicator.className = 'tab-badge';
  checkHealth();
  pollLogs();
});

// Logs Polling & Rendering
let lastSeenLogCount = 0;

async function pollLogs() {
  const minLevel = logFilterSelect.value;
  const url = minLevel ? `/api/logs?limit=80&min_level=${minLevel}` : '/api/logs?limit=80';
  try {
    const res = await fetch(url);
    if (!res.ok) return;
    const data = await res.json();
    renderLogs(data.logs || []);
  } catch (err) {
    // Backend might be crashed or unreachable
  }
}

function renderLogs(logs) {
  logStreamBox.innerHTML = '';
  if (logs.length === 0) {
    logStreamBox.innerHTML = '<div class="empty-subtext">No logs found matching filter</div>';
    return;
  }
  logs.forEach(log => {
    const row = document.createElement('div');
    const levelLower = (log.level || 'info').toLowerCase();
    row.className = `log-entry log-entry-${levelLower}`;

    const timeSpan = document.createElement('span');
    timeSpan.className = 'log-time';
    timeSpan.textContent = `[${log.timestamp.slice(11, 19)}]`;

    const levelSpan = document.createElement('span');
    levelSpan.className = 'log-level';
    levelSpan.textContent = `[${log.level}]`;

    const msgSpan = document.createElement('span');
    msgSpan.className = 'log-msg';
    msgSpan.textContent = log.raw_message || log.message;

    row.appendChild(timeSpan);
    row.appendChild(levelSpan);
    row.appendChild(msgSpan);
    logStreamBox.appendChild(row);
  });
  logStreamBox.scrollTop = logStreamBox.scrollHeight;
}

logFilterSelect.addEventListener('change', pollLogs);
btnClearConsole.addEventListener('click', () => logStreamBox.innerHTML = '');
btnCopyLogs.addEventListener('click', () => {
  const text = Array.from(logStreamBox.querySelectorAll('.log-entry'))
    .map(el => el.textContent)
    .join('\n');
  navigator.clipboard.writeText(text).then(() => {
    btnCopyLogs.textContent = 'Copied!';
    setTimeout(() => btnCopyLogs.textContent = 'Copy Logs', 1500);
  });
});

// Health Check Loop
async function checkHealth() {
  try {
    const res = await fetch('/api/health');
    if (res.ok) {
      const data = await res.json();
      healthIndicator.className = 'status-indicator status-green';
      healthText.textContent = 'Service Healthy';
      if (data.simulation_state && data.simulation_state.error_mode !== 'none') {
        incidentStatusIndicator.textContent = data.simulation_state.error_mode.toUpperCase();
        incidentStatusIndicator.className = 'tab-badge badge-active-incident';
      }
    } else {
      healthIndicator.className = 'status-indicator status-red';
      healthText.textContent = `Degraded (${res.status})`;
    }
  } catch (err) {
    healthIndicator.className = 'status-indicator status-red';
    healthText.textContent = 'Offline / Unreachable';
  }
}

// Polling intervals
setInterval(checkHealth, 4000);
setInterval(pollLogs, 2000);
checkHealth();
pollLogs();
