// ChunkStream Frontend Application Logic

// Global State
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

// Format utilities
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

// -------------------------------------------------------------------------
// TOP NAVIGATION & SCROLLING
// -------------------------------------------------------------------------
const navItems = document.querySelectorAll('.nav-item');
navItems.forEach(btn => {
  btn.addEventListener('click', () => {
    navItems.forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const targetId = btn.getAttribute('data-target');
    const targetEl = document.getElementById(targetId);
    if (targetEl) {
      targetEl.scrollIntoView({ behavior: 'smooth' });
    }
  });
});

// Studio View Switchers
const studioGrid = document.getElementById('studio-grid');
const viewSplitBtn = document.getElementById('view-split-btn');
const viewSenderBtn = document.getElementById('view-sender-btn');
const viewReceiverBtn = document.getElementById('view-receiver-btn');

viewSplitBtn.addEventListener('click', () => {
  studioGrid.className = 'studio-grid';
  viewSplitBtn.classList.add('active');
  viewSenderBtn.classList.remove('active');
  viewReceiverBtn.classList.remove('active');
});

viewSenderBtn.addEventListener('click', () => {
  studioGrid.className = 'studio-grid show-sender-only';
  viewSenderBtn.classList.add('active');
  viewSplitBtn.classList.remove('active');
  viewReceiverBtn.classList.remove('active');
});

viewReceiverBtn.addEventListener('click', () => {
  studioGrid.className = 'studio-grid show-receiver-only';
  viewReceiverBtn.classList.add('active');
  viewSplitBtn.classList.remove('active');
  viewSenderBtn.classList.remove('active');
  fetchActiveSessions();
});

// Hero Buttons
document.getElementById('btn-hero-sample').addEventListener('click', () => {
  generateSampleFileAndSelect();
  document.getElementById('studio-section').scrollIntoView({ behavior: 'smooth' });
});

document.getElementById('btn-hero-chaos').addEventListener('click', () => {
  document.getElementById('chaos-section').scrollIntoView({ behavior: 'smooth' });
});

// -------------------------------------------------------------------------
// SENDER LOGIC
// -------------------------------------------------------------------------
const senderDropZone = document.getElementById('sender-drop-zone');
const senderFileInput = document.getElementById('sender-file-input');
const fileMetaBar = document.getElementById('file-meta-bar');
const selectedFileName = document.getElementById('selected-file-name');
const selectedFileSize = document.getElementById('selected-file-size');
const selectedFileHash = document.getElementById('selected-file-hash');
const btnClearFile = document.getElementById('btn-clear-file');
const chunkSizeSelect = document.getElementById('chunk-size-select');
const chunkCountPreview = document.getElementById('chunk-count-preview');
const transferCodeInput = document.getElementById('transfer-code-input');
const btnNewCode = document.getElementById('btn-new-code');
const btnCopyCode = document.getElementById('btn-copy-code');
const btnShareLink = document.getElementById('btn-share-link');
const btnQuickSampleFile = document.getElementById('btn-quick-sample-file');

const btnStartStream = document.getElementById('btn-start-stream');
const btnPauseStream = document.getElementById('btn-pause-stream');
const btnResumeStream = document.getElementById('btn-resume-stream');

const senderStatusChip = document.getElementById('sender-status-chip');
const senderPct = document.getElementById('sender-pct');
const senderChunksCount = document.getElementById('sender-chunks-count');
const senderSpeedVal = document.getElementById('sender-speed-val');
const senderRetriesCount = document.getElementById('sender-retries-count');
const senderProgressFill = document.getElementById('sender-progress-fill');
const senderChunkViewport = document.getElementById('sender-chunk-viewport');

// Initialize Session Code
currentTransferId = generateRandomCode();
transferCodeInput.value = currentTransferId;

btnNewCode.addEventListener('click', () => {
  if (isSending) return;
  currentTransferId = generateRandomCode();
  transferCodeInput.value = currentTransferId;
});

btnCopyCode.addEventListener('click', () => {
  navigator.clipboard.writeText(transferCodeInput.value).then(() => {
    btnCopyCode.textContent = 'Copied!';
    setTimeout(() => btnCopyCode.textContent = 'Copy', 1500);
  });
});

btnShareLink.addEventListener('click', () => {
  const shareUrl = `${window.location.origin}/?rx=${transferCodeInput.value}`;
  navigator.clipboard.writeText(shareUrl).then(() => {
    btnShareLink.textContent = 'Link Copied!';
    setTimeout(() => btnShareLink.textContent = 'Share Link', 1500);
  });
});

// Drop Zone Actions
senderDropZone.addEventListener('click', (e) => {
  if (e.target !== btnQuickSampleFile && !btnQuickSampleFile.contains(e.target)) {
    senderFileInput.click();
  }
});

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

btnClearFile.addEventListener('click', () => {
  selectedFile = null;
  fileMetaBar.classList.add('hidden');
  senderDropZone.classList.remove('hidden');
  btnStartStream.disabled = true;
  senderChunkViewport.innerHTML = '<div class="viewport-placeholder">Select or generate a file to initialize chunk blocks</div>';
  senderPct.textContent = '0%';
  senderProgressFill.style.width = '0%';
  senderChunksCount.textContent = '0 / 0';
  chunkCountPreview.textContent = '0 chunks';
});

btnQuickSampleFile.addEventListener('click', (e) => {
  e.stopPropagation();
  generateSampleFileAndSelect();
});

function generateSampleFileAndSelect() {
  const sampleSize = 2 * 1024 * 1024; // 2 MB
  const buffer = new Uint8Array(sampleSize);
  // Fill with repeated identifiable pattern
  for (let i = 0; i < sampleSize; i++) {
    buffer[i] = (i % 256) ^ 0x5a;
  }
  const blob = new Blob([buffer], { type: 'application/octet-stream' });
  const file = new File([blob], 'demo_payload_2mb.bin', { type: 'application/octet-stream' });
  handleFileSelect(file);
}

async function handleFileSelect(file) {
  selectedFile = file;
  selectedFileName.textContent = file.name;
  selectedFileSize.textContent = formatBytes(file.size);
  selectedFileHash.textContent = 'Computing SHA-256...';

  senderDropZone.classList.add('hidden');
  fileMetaBar.classList.remove('hidden');
  btnStartStream.disabled = false;

  recalcChunks();

  // Compute file preview hash
  try {
    const previewBuffer = await file.slice(0, Math.min(file.size, 5 * 1024 * 1024)).arrayBuffer();
    const hash = await computeSha256(previewBuffer);
    selectedFileHash.textContent = `SHA-256: ${hash.slice(0, 12)}...`;
  } catch (err) {
    selectedFileHash.textContent = 'Hash ready';
  }
}

chunkSizeSelect.addEventListener('change', (e) => {
  senderChunkSize = parseInt(e.target.value, 10);
  if (selectedFile) {
    recalcChunks();
  }
});

function recalcChunks() {
  totalChunks = Math.ceil(selectedFile.size / senderChunkSize);
  chunkCountPreview.textContent = `${totalChunks} chunks`;
  buildSenderChunkMatrix(totalChunks);
}

function buildSenderChunkMatrix(count) {
  senderChunkViewport.innerHTML = '';
  ackedChunks.clear();
  inFlightChunks.clear();
  failedChunks.clear();

  for (let i = 0; i < count; i++) {
    const block = document.createElement('div');
    block.className = 'chunk-unit';
    block.id = `sender-chunk-${i}`;
    block.title = `Chunk #${i}`;
    senderChunkViewport.appendChild(block);
  }
  senderChunksCount.textContent = `0 / ${count}`;
  senderPct.textContent = '0%';
  senderProgressFill.style.width = '0%';
}

function setSenderChunkState(index, stateClass) {
  const el = document.getElementById(`sender-chunk-${index}`);
  if (!el) return;
  el.className = `chunk-unit state-${stateClass}`;
}

// Start Stream
btnStartStream.addEventListener('click', async () => {
  if (!selectedFile || isSending) return;

  btnStartStream.disabled = true;
  btnPauseStream.disabled = false;
  btnResumeStream.disabled = true;
  chunkSizeSelect.disabled = true;
  btnClearFile.disabled = true;
  isSending = true;
  isSenderPaused = false;
  retryCount = 0;
  senderRetriesCount.textContent = '0';
  bytesSentTotal = 0;
  transferStartTime = Date.now();

  senderStatusChip.textContent = 'CONNECTING';
  senderStatusChip.className = 'status-chip status-active';

  // 1. Whole file preview hash
  let previewHash = '';
  try {
    const buffer = await selectedFile.slice(0, Math.min(selectedFile.size, 5 * 1024 * 1024)).arrayBuffer();
    previewHash = await computeSha256(buffer);
  } catch (e) {
    console.warn("Could not compute hash:", e);
  }

  // 2. Initialize transfer session on server
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
        checksum: previewHash
      })
    });
    if (!res.ok) throw new Error('Failed to register transfer on server');
  } catch (e) {
    alert(`Transfer registration error: ${e.message}`);
    btnStartStream.disabled = false;
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
    senderStatusChip.textContent = 'STREAMING';
    senderStatusChip.className = 'status-chip status-active';
    streamChunks();
  };

  senderWs.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleSenderSocketMessage(msg);
    } catch (e) {
      console.error("Message parse error:", e);
    }
  };

  senderWs.onclose = () => {
    if (isSending && !isSenderPaused) {
      senderStatusChip.textContent = 'DISCONNECTED';
      senderStatusChip.className = 'status-chip status-error';
      btnResumeStream.disabled = false;
    }
  };
}

function handleSenderSocketMessage(msg) {
  if (msg.type === 'chunk_ack') {
    const idx = msg.index;
    inFlightChunks.delete(idx);
    ackedChunks.add(idx);
    setSenderChunkState(idx, 'acked');

    const pct = Math.round((ackedChunks.size / totalChunks) * 100);
    senderPct.textContent = `${pct}%`;
    senderProgressFill.style.width = `${pct}%`;
    senderChunksCount.textContent = `${ackedChunks.size} / ${totalChunks}`;

    // Speed calculation
    const elapsedSec = (Date.now() - transferStartTime) / 1000;
    if (elapsedSec > 0.4) {
      const mbps = ((bytesSentTotal / (1024 * 1024)) / elapsedSec).toFixed(2);
      senderSpeedVal.textContent = `${mbps} MB/s`;
    }

    if (ackedChunks.size === totalChunks) {
      finishSenderTransfer();
    }
  } else if (msg.type === 'chunk_error') {
    const idx = msg.index;
    inFlightChunks.delete(idx);
    failedChunks.add(idx);
    retryCount++;
    senderRetriesCount.textContent = retryCount;
    setSenderChunkState(idx, 'failed');

    if (msg.retryable && !isSenderPaused) {
      setTimeout(() => {
        if (!ackedChunks.has(idx) && !isSenderPaused) {
          sendSingleChunk(idx);
        }
      }, 500);
    }
  } else if (msg.type === 'request_missing') {
    const missing = msg.missing_chunks || [];
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

    // Sliding window of 3 chunks in-flight
    while (inFlightChunks.size >= 3 && !isSenderPaused) {
      await new Promise(r => setTimeout(r, 30));
    }

    if (isSenderPaused) break;
    sendSingleChunk(i);
    await new Promise(r => setTimeout(r, 20));
  }
}

async function sendSingleChunk(index) {
  if (!senderWs || senderWs.readyState !== WebSocket.OPEN) return;
  if (ackedChunks.has(index)) return;

  inFlightChunks.add(index);
  setSenderChunkState(index, 'sending');

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

function finishSenderTransfer() {
  isSending = false;
  senderStatusChip.textContent = 'COMPLETED';
  senderStatusChip.className = 'status-chip status-success';
  btnPauseStream.disabled = true;
  btnResumeStream.disabled = true;
  btnClearFile.disabled = false;

  if (senderWs && senderWs.readyState === WebSocket.OPEN) {
    senderWs.send(JSON.stringify({ type: 'complete' }));
  }
}

btnPauseStream.addEventListener('click', () => {
  isSenderPaused = true;
  senderStatusChip.textContent = 'PAUSED';
  senderStatusChip.className = 'status-chip status-idle';
  btnPauseStream.disabled = true;
  btnResumeStream.disabled = false;
});

btnResumeStream.addEventListener('click', () => {
  isSenderPaused = false;
  isSending = true;
  senderStatusChip.textContent = 'RESUMING';
  senderStatusChip.className = 'status-chip status-active';
  btnPauseStream.disabled = false;
  btnResumeStream.disabled = true;

  if (!senderWs || senderWs.readyState !== WebSocket.OPEN) {
    connectSenderWebSocket();
  } else {
    streamChunks();
  }
});

// -------------------------------------------------------------------------
// RECEIVER LOGIC
// -------------------------------------------------------------------------
const rxCodeInput = document.getElementById('rx-code-input');
const btnRxConnect = document.getElementById('btn-rx-connect');
const activeSessionsChips = document.getElementById('active-sessions-chips');
const btnRefreshSessions = document.getElementById('btn-refresh-sessions');
const rxInfoBox = document.getElementById('rx-info-box');
const rxFileName = document.getElementById('rx-file-name');
const rxFileSize = document.getElementById('rx-file-size');
const rxTotalChunksLabel = document.getElementById('rx-total-chunks-label');
const rxStateLabel = document.getElementById('rx-state-label');

const rxStatusChip = document.getElementById('receiver-status-chip');
const rxPct = document.getElementById('rx-pct');
const rxChunksCount = document.getElementById('rx-chunks-count');
const rxMissingCount = document.getElementById('rx-missing-count');
const rxSocketState = document.getElementById('rx-socket-state');
const rxProgressFill = document.getElementById('rx-progress-fill');
const rxChunkViewport = document.getElementById('rx-chunk-viewport');

const btnRxDisconnect = document.getElementById('btn-rx-disconnect');
const btnRxResume = document.getElementById('btn-rx-resume');
const btnRxDownload = document.getElementById('btn-rx-download');

btnRefreshSessions.addEventListener('click', fetchActiveSessions);

async function fetchActiveSessions() {
  try {
    const res = await fetch('/api/transfers?limit=8');
    const list = await res.json();
    activeSessionsChips.innerHTML = '';
    if (list.length === 0) {
      activeSessionsChips.innerHTML = '<span class="subtext">No active transfers on server</span>';
      return;
    }
    list.forEach(t => {
      const chip = document.createElement('button');
      chip.className = 'session-chip';
      chip.textContent = `${t.id} (${t.filename} • ${t.progress_percentage}%)`;
      chip.addEventListener('click', () => {
        rxCodeInput.value = t.id;
        connectReceiver();
      });
      activeSessionsChips.appendChild(chip);
    });
  } catch (err) {
    console.warn("Could not fetch active sessions:", err);
  }
}

btnRxConnect.addEventListener('click', () => connectReceiver());

async function connectReceiver() {
  const code = rxCodeInput.value.trim().toUpperCase();
  if (!code) {
    alert('Please enter or select a session code');
    return;
  }
  rxTransferId = code;

  try {
    const res = await fetch(`/api/transfers/${rxTransferId}`);
    if (!res.ok) throw new Error(`Transfer session ${rxTransferId} not found`);
    const data = await res.json();

    rxInfoBox.classList.remove('hidden');
    rxFileName.textContent = data.filename;
    rxFileSize.textContent = formatBytes(data.filesize);
    rxTotalChunksLabel.textContent = data.total_chunks;
    rxStateLabel.textContent = data.status.toUpperCase();

    rxTotalChunks = data.total_chunks;
    buildReceiverChunkMatrix(data.total_chunks);

    rxChunksReceived.clear();
    (data.acknowledged_chunks || []).forEach(idx => {
      rxChunksReceived.add(idx);
      setRxChunkState(idx, 'acked');
    });
    updateReceiverStats();
  } catch (e) {
    alert(`Could not connect: ${e.message}`);
    return;
  }

  // Connect WebSocket
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  const wsUrl = `${protocol}//${window.location.host}/ws/transfer/${rxTransferId}?role=receiver`;
  receiverWs = new WebSocket(wsUrl);

  receiverWs.onopen = () => {
    rxStatusChip.textContent = 'CONNECTED';
    rxStatusChip.className = 'status-chip status-active';
    rxSocketState.textContent = 'Connected (Live)';
    btnRxDisconnect.disabled = false;
    btnRxResume.disabled = false;

    // Send resume request to sync state
    receiverWs.send(JSON.stringify({ type: 'resume_request' }));
  };

  receiverWs.onmessage = (event) => {
    try {
      const msg = JSON.parse(event.data);
      handleReceiverMessage(msg);
    } catch (e) {
      console.error("Receiver parse error:", e);
    }
  };

  receiverWs.onclose = () => {
    rxStatusChip.textContent = 'OFFLINE';
    rxStatusChip.className = 'status-chip status-idle';
    rxSocketState.textContent = 'Disconnected';
    btnRxDisconnect.disabled = true;
  };
}

function buildReceiverChunkMatrix(count) {
  rxChunkViewport.innerHTML = '';
  for (let i = 0; i < count; i++) {
    const block = document.createElement('div');
    block.className = 'chunk-unit';
    block.id = `rx-chunk-${i}`;
    block.title = `Chunk #${i}`;
    rxChunkViewport.appendChild(block);
  }
}

function setRxChunkState(index, stateClass) {
  const el = document.getElementById(`rx-chunk-${index}`);
  if (!el) return;
  el.className = `chunk-unit state-${stateClass}`;
}

function updateReceiverStats() {
  const count = rxChunksReceived.size;
  const missing = rxTotalChunks - count;
  const pct = rxTotalChunks > 0 ? Math.round((count / rxTotalChunks) * 100) : 0;

  rxChunksCount.textContent = `${count} / ${rxTotalChunks}`;
  rxMissingCount.textContent = missing;
  rxPct.textContent = `${pct}%`;
  rxProgressFill.style.width = `${pct}%`;

  if (count === rxTotalChunks && rxTotalChunks > 0) {
    rxStatusChip.textContent = 'COMPLETED';
    rxStatusChip.className = 'status-chip status-success';
    rxSocketState.textContent = 'Assembled (Complete)';
    btnRxDownload.classList.remove('hidden');
    btnRxDownload.href = `/api/transfers/${rxTransferId}/download`;
  }
}

function handleReceiverMessage(msg) {
  if (msg.type === 'chunk_data') {
    const idx = msg.index;
    rxChunksReceived.add(idx);
    setRxChunkState(idx, 'acked');
    updateReceiverStats();
  } else if (msg.type === 'progress') {
    rxStateLabel.textContent = (msg.status || 'transferring').toUpperCase();
  } else if (msg.type === 'resume_state') {
    (msg.acknowledged_chunks || []).forEach(idx => {
      rxChunksReceived.add(idx);
      setRxChunkState(idx, 'acked');
    });
    updateReceiverStats();
  } else if (msg.type === 'completed') {
    rxStatusChip.textContent = 'COMPLETED';
    rxStatusChip.className = 'status-chip status-success';
    btnRxDownload.classList.remove('hidden');
    btnRxDownload.href = msg.download_url || `/api/transfers/${rxTransferId}/download`;
  } else if (msg.type === 'resume_error') {
    alert(`Resume Failure Alert: ${msg.message}`);
    rxSocketState.textContent = 'Conflict Error (F6)';
  }
}

btnRxDisconnect.addEventListener('click', () => {
  if (receiverWs) {
    receiverWs.close(1000, "User manual disconnect simulation");
  }
});

btnRxResume.addEventListener('click', () => {
  if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
    receiverWs.send(JSON.stringify({ type: 'resume_request' }));
  } else {
    connectReceiver();
  }
});

// -------------------------------------------------------------------------
// PRODPILOT CHAOS & FAULT INJECTION
// -------------------------------------------------------------------------
const chaosPill = document.getElementById('chaos-pill');
const btnResetChaos = document.getElementById('btn-reset-chaos');
const btnTriggerF3 = document.getElementById('btn-trigger-f3');
const btnTriggerF4 = document.getElementById('btn-trigger-f4');
const btnTriggerF5 = document.getElementById('btn-trigger-f5');
const btnTriggerF6 = document.getElementById('btn-trigger-f6');
const btnTriggerF1 = document.getElementById('btn-trigger-f1');
const btnTriggerF2 = document.getElementById('btn-trigger-f2');

async function triggerFaultScenario(scenarioKey) {
  try {
    const res = await fetch('/api/test/fail', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ scenario: scenarioKey })
    });
    const data = await res.json();
    chaosPill.textContent = scenarioKey.toUpperCase();
    chaosPill.className = 'badge-pill pill-active-fault';
    pollLogs();
  } catch (err) {
    console.error("Fault scenario error:", err);
  }
}

btnTriggerF3.addEventListener('click', () => triggerFaultScenario('low_error'));
btnTriggerF4.addEventListener('click', () => triggerFaultScenario('high_error'));
btnTriggerF6.addEventListener('click', () => triggerFaultScenario('resume_failure'));
btnTriggerF1.addEventListener('click', () => triggerFaultScenario('unhealthy'));

btnTriggerF5.addEventListener('click', async () => {
  const id = rxTransferId || currentTransferId;
  if (!id) {
    alert('Please enter or select a session code first');
    return;
  }
  await fetch(`/api/test/disconnect-receiver?transfer_id=${id}`, { method: 'POST' });
  pollLogs();
});

btnTriggerF2.addEventListener('click', async () => {
  if (confirm('F2 Crash Simulation: This will terminate the backend process with code 1. Are you sure?')) {
    try {
      await fetch('/api/test/crash', { method: 'POST' });
    } catch (e) {
      console.warn("Backend crash initiated");
    }
    alert('Fatal crash signal emitted (exit code 1). Process terminated.');
    checkHealth();
  }
});

btnResetChaos.addEventListener('click', async () => {
  await fetch('/api/test/reset', { method: 'POST' });
  chaosPill.textContent = 'NORMAL';
  chaosPill.className = 'badge-pill';
  checkHealth();
  pollLogs();
});

// -------------------------------------------------------------------------
// OBSERVABILITY LOG STREAMER
// -------------------------------------------------------------------------
const logTerminalBody = document.getElementById('log-terminal-body');
const logLevelFilter = document.getElementById('log-level-filter');
const logSearchInput = document.getElementById('log-search-input');
const btnCopyLogStream = document.getElementById('btn-copy-log-stream');
const btnClearLogStream = document.getElementById('btn-clear-log-stream');

async function pollLogs() {
  const minLevel = logLevelFilter.value;
  const url = minLevel ? `/api/logs?limit=100&min_level=${minLevel}` : '/api/logs?limit=100';
  try {
    const res = await fetch(url);
    if (!res.ok) return;
    const data = await res.json();
    renderLogs(data.logs || []);
  } catch (err) {
    // Backend may be temporarily down during crash simulation
  }
}

function renderLogs(logs) {
  const searchTerm = logSearchInput.value.toLowerCase().trim();
  logTerminalBody.innerHTML = '';

  const filtered = searchTerm
    ? logs.filter(l => (l.message || '').toLowerCase().includes(searchTerm))
    : logs;

  if (filtered.length === 0) {
    logTerminalBody.innerHTML = '<div class="log-line"><span class="log-content" style="color: #64748b;">No log entries match criteria</span></div>';
    return;
  }

  filtered.forEach(log => {
    const line = document.createElement('div');
    const levelLower = (log.level || 'info').toLowerCase();
    line.className = `log-line log-${levelLower}`;

    const timeSpan = document.createElement('span');
    timeSpan.className = 'log-timestamp';
    timeSpan.textContent = `[${log.timestamp.slice(11, 19)}]`;

    const tagSpan = document.createElement('span');
    tagSpan.className = 'log-tag';
    tagSpan.textContent = `[${log.level}]`;

    const msgSpan = document.createElement('span');
    msgSpan.className = 'log-content';
    msgSpan.textContent = log.raw_message || log.message;

    line.appendChild(timeSpan);
    line.appendChild(tagSpan);
    line.appendChild(msgSpan);
    logTerminalBody.appendChild(line);
  });
  logTerminalBody.scrollTop = logTerminalBody.scrollHeight;
}

logLevelFilter.addEventListener('change', pollLogs);
logSearchInput.addEventListener('input', pollLogs);
btnClearLogStream.addEventListener('click', () => logTerminalBody.innerHTML = '');

btnCopyLogStream.addEventListener('click', () => {
  const text = Array.from(logTerminalBody.querySelectorAll('.log-line'))
    .map(el => el.textContent)
    .join('\n');
  navigator.clipboard.writeText(text).then(() => {
    btnCopyLogStream.textContent = 'Copied!';
    setTimeout(() => btnCopyLogStream.textContent = 'Copy for ProdPilot', 1500);
  });
});

// -------------------------------------------------------------------------
// HEALTH PROBE & URL AUTO-CONNECT
// -------------------------------------------------------------------------
const healthDot = document.getElementById('health-dot');
const healthLabel = document.getElementById('health-label');

async function checkHealth() {
  try {
    const res = await fetch('/api/health');
    if (res.ok) {
      const data = await res.json();
      healthDot.className = 'pulse-dot green';
      healthLabel.textContent = 'System Healthy';
      if (data.simulation_state && data.simulation_state.error_mode !== 'none') {
        chaosPill.textContent = data.simulation_state.error_mode.toUpperCase();
        chaosPill.className = 'badge-pill pill-active-fault';
      }
    } else {
      healthDot.className = 'pulse-dot red';
      healthLabel.textContent = `Degraded (${res.status})`;
    }
  } catch (err) {
    healthDot.className = 'pulse-dot red';
    healthLabel.textContent = 'Offline / Unreachable';
  }
}

// Auto-join from URL parameter (e.g. ?rx=CS-XXXX or ?transfer=CS-XXXX)
function checkUrlAutoConnect() {
  const params = new URLSearchParams(window.location.search);
  const rxCode = params.get('rx') || params.get('transfer');
  if (rxCode) {
    rxCodeInput.value = rxCode.toUpperCase();
    document.getElementById('studio-section').scrollIntoView({ behavior: 'smooth' });
    connectReceiver();
  }
}

// Polling loops
setInterval(checkHealth, 4000);
setInterval(pollLogs, 2500);

checkHealth();
pollLogs();
checkUrlAutoConnect();
fetchActiveSessions();
