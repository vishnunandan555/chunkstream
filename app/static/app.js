// Theme Management
const themeToggle = document.getElementById('theme-toggle');
const iconSun = document.querySelector('.icon-sun');
const iconMoon = document.querySelector('.icon-moon');

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

const savedTheme = localStorage.getItem('cs_theme') || (window.matchMedia('(prefers-color-scheme: light)').matches ? 'light' : 'dark');
applyTheme(savedTheme);

if (themeToggle) {
  themeToggle.addEventListener('click', () => {
    const isLight = document.documentElement.getAttribute('data-theme') === 'light';
    const newTheme = isLight ? 'dark' : 'light';
    localStorage.setItem('cs_theme', newTheme);
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

// Global State
let currentFile = null;
let currentCode = null;
let senderWs = null;
let receiverWs = null;
let isStreaming = false;
let chunkSize = 262144; // 256 KB
let totalChunks = 0;
let ackedChunks = new Set();
let inFlightChunks = new Set();
let bytesSent = 0;
let senderStartTime = 0;

let rxBytesReceived = 0;
let rxStartTime = 0;
let rxPendingDownloadUrl = null;
let rxPendingFilename = null;

// DOM Elements
const viewHome = document.getElementById('view-home');
const viewSend = document.getElementById('view-send');
const viewReceive = document.getElementById('view-receive');
const homeLink = document.getElementById('home-link');

const btnChoiceSend = document.getElementById('btn-choice-send');
const btnChoiceReceive = document.getElementById('btn-choice-receive');
const btnBackFromSend = document.getElementById('btn-back-from-send');
const btnBackFromReceive = document.getElementById('btn-back-from-receive');

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
const btnSendAgain = document.getElementById('btn-send-again');
const btnResumeSend = document.getElementById('btn-resume-send');

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
const rxDownloadReadyBox = document.getElementById('rx-download-ready-box');
const btnTriggerDownload = document.getElementById('btn-trigger-download');
const rxClearedBox = document.getElementById('rx-cleared-box');
const btnReceiveAgain = document.getElementById('btn-receive-again');

// Navigation
function showView(view) {
  viewHome.classList.add('hidden');
  viewSend.classList.add('hidden');
  viewReceive.classList.add('hidden');
  view.classList.remove('hidden');
}

homeLink.addEventListener('click', () => {
  resetSender();
  resetReceiver();
  showView(viewHome);
});

btnChoiceSend.addEventListener('click', () => {
  resetSender();
  showView(viewSend);
  fileInput.click();
});

btnChoiceReceive.addEventListener('click', () => {
  resetReceiver();
  showView(viewReceive);
  setTimeout(() => receiveCodeInput.focus(), 100);
});

btnBackFromSend.addEventListener('click', () => {
  resetSender();
  showView(viewHome);
});

btnBackFromReceive.addEventListener('click', () => {
  resetReceiver();
  showView(viewHome);
});

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

btnCopyCode.addEventListener('click', () => {
  if (!currentCode) return;
  navigator.clipboard.writeText(currentCode).then(() => {
    btnCopyCode.textContent = 'Copied!';
    setTimeout(() => { btnCopyCode.textContent = 'Copy Code'; }, 1500);
  });
});

async function handleFileSelected(file) {
  currentFile = file;
  currentCode = generate4DigitCode();
  totalChunks = Math.ceil(file.size / chunkSize);
  ackedChunks.clear();
  inFlightChunks.clear();
  bytesSent = 0;
  isStreaming = false;

  sendFileName.textContent = file.name;
  sendFileSize.textContent = formatBytes(file.size);
  senderCodeDisplay.textContent = currentCode;

  senderStepPick.classList.add('hidden');
  senderStepTransfer.classList.remove('hidden');
  senderStatusText.textContent = 'Waiting for receiver to connect...';
  senderStatusDot.className = 'status-dot';
  senderProgressBox.classList.add('hidden');
  btnSendAgain.classList.add('hidden');
  btnResumeSend.classList.add('hidden');

  try {
    await fetch('/api/transfers', {
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
  } catch (err) {
    console.error('Failed to init transfer:', err);
  }

  connectSenderWebSocket();
}

function connectSenderWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  senderWs = new WebSocket(`${protocol}//${window.location.host}/ws/transfer/${currentCode}?role=sender`);

  senderWs.onmessage = (e) => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'peer_connected' && msg.role === 'receiver') {
        startStreamingChunks();
      } else if (msg.type === 'request_missing') {
        startStreamingChunks(msg.missing_chunks);
      } else if (msg.type === 'chunk_ack') {
        handleChunkAck(msg.index);
      } else if (msg.type === 'peer_disconnected' && msg.role === 'receiver') {
        isStreaming = false;
        senderStatusDot.className = 'status-dot red';
        senderStatusText.textContent = 'Receiver disconnected. Transfer paused.';
        btnResumeSend.classList.remove('hidden');
      }
    } catch (err) {
      console.warn('Sender WS error:', err);
    }
  };
}

function startStreamingChunks(missingList) {
  btnResumeSend.classList.add('hidden');
  if (isStreaming) {
    if (missingList && missingList.length > 0) {
      streamFileChunks(missingList);
    }
    return;
  }

  isStreaming = true;
  senderStartTime = Date.now();
  senderStatusText.textContent = 'Streaming file to receiver...';
  senderStatusDot.className = 'status-dot green';
  senderProgressBox.classList.remove('hidden');

  streamFileChunks(missingList);
}

async function streamFileChunks(missingList) {
  const chunksToSend = missingList && missingList.length > 0
    ? missingList
    : Array.from({ length: totalChunks }, (_, i) => i);

  for (const idx of chunksToSend) {
    if (!isStreaming) break;
    if (ackedChunks.has(idx)) continue;

    while (inFlightChunks.size >= 4) {
      if (!isStreaming) break;
      await new Promise(r => setTimeout(r, 15));
    }
    if (!isStreaming) break;

    inFlightChunks.add(idx);
    const start = idx * chunkSize;
    const end = Math.min(start + chunkSize, currentFile.size);
    const slice = currentFile.slice(start, end);
    const buffer = await slice.arrayBuffer();
    const b64 = arrayBufferToBase64(buffer);

    if (senderWs && senderWs.readyState === WebSocket.OPEN) {
      senderWs.send(JSON.stringify({
        type: 'chunk',
        index: idx,
        total: totalChunks,
        data: b64,
      }));
    }

    await new Promise(r => setTimeout(r, 5));
  }
}

function handleChunkAck(index) {
  inFlightChunks.delete(index);
  ackedChunks.add(index);
  bytesSent = Math.min(ackedChunks.size * chunkSize, currentFile.size);

  const pct = Math.round((ackedChunks.size / totalChunks) * 100);
  senderProgressFill.style.width = `${pct}%`;
  senderProgressPct.textContent = `${pct}%`;

  const elapsedSec = (Date.now() - senderStartTime) / 1000;
  if (elapsedSec > 0.2) {
    const speedBytesPerSec = bytesSent / elapsedSec;
    const mbps = (speedBytesPerSec / (1024 * 1024)).toFixed(1);
    senderSpeed.textContent = `${mbps} MB/s`;

    const remainingBytes = currentFile.size - bytesSent;
    const etaSec = speedBytesPerSec > 0 ? remainingBytes / speedBytesPerSec : 0;
    senderEta.textContent = formatDuration(etaSec);
  }

  senderProgressBytes.textContent = `${formatBytes(bytesSent)} / ${formatBytes(currentFile.size)}`;
  senderProgressChunks.textContent = `Chunk ${ackedChunks.size}/${totalChunks}`;

  if (ackedChunks.size === totalChunks) {
    senderStatusText.textContent = '✓ Transfer complete!';
    senderStatusDot.className = 'status-dot green';
    btnSendAgain.classList.remove('hidden');
    if (senderWs && senderWs.readyState === WebSocket.OPEN) {
      senderWs.send(JSON.stringify({ type: 'complete' }));
    }
  }
}

btnSendAgain.addEventListener('click', () => {
  resetSender();
  fileInput.click();
});

btnResumeSend.addEventListener('click', async () => {
  btnResumeSend.classList.add('hidden');
  senderStatusText.textContent = 'Resuming transfer...';
  senderStatusDot.className = 'status-dot';
  try {
    const res = await fetch(`/api/transfers/${currentCode}`);
    if (res.ok) {
      const info = await res.json();
      startStreamingChunks(info.missing_chunks);
    }
  } catch (err) {
    senderStatusText.textContent = 'Could not fetch transfer state';
    senderStatusDot.className = 'status-dot red';
  }
});

function resetSender() {
  if (senderWs) {
    senderWs.close();
    senderWs = null;
  }
  currentFile = null;
  currentCode = null;
  isStreaming = false;
  senderStepPick.classList.remove('hidden');
  senderStepTransfer.classList.add('hidden');
  fileInput.value = '';
}

// Receiver Flow
btnStartReceive.addEventListener('click', startReceiving);

receiveCodeInput.addEventListener('keydown', (e) => {
  if (e.key === 'Enter') startReceiving();
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

    receiverStepInput.classList.add('hidden');
    receiverStepTransfer.classList.remove('hidden');
    rxStatusText.textContent = 'Connecting to sender...';
    rxStatusDot.className = 'status-dot';
    rxProgressFill.style.width = '0%';
    rxProgressPct.textContent = '0%';
    rxSpeed.textContent = '0.0 MB/s';
    rxEta.textContent = '--';
    rxProgressBytes.textContent = `0 B / ${formatBytes(data.filesize)}`;
    rxProgressChunks.textContent = `Chunk 0/${data.total_chunks}`;
    rxProgressBox.classList.remove('hidden');
    rxDownloadReadyBox.classList.add('hidden');
    rxClearedBox.classList.add('hidden');

    rxStartTime = Date.now();
    rxBytesReceived = 0;

    connectReceiverWebSocket(code, data);
  } catch (err) {
    alert(err.message);
  }
}

function connectReceiverWebSocket(code, transferData) {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  receiverWs = new WebSocket(`${protocol}//${window.location.host}/ws/transfer/${code}?role=receiver`);

  let receivedChunks = new Set(transferData.acknowledged_chunks || []);
  const total = transferData.total_chunks;

  receiverWs.onopen = () => {
    rxStatusText.textContent = 'Connected. Waiting for chunks...';
    rxStatusDot.className = 'status-dot green';
    receiverWs.send(JSON.stringify({ type: 'resume_request' }));
  };

  receiverWs.onmessage = (e) => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'peer_disconnected' && msg.role === 'sender') {
        rxStatusDot.className = 'status-dot red';
        rxStatusText.textContent = 'Sender disconnected. Waiting...';
      } else if (msg.type === 'peer_connected' && msg.role === 'sender') {
        rxStatusDot.className = 'status-dot green';
        rxStatusText.textContent = 'Receiving file...';
      } else if (msg.type === 'chunk_data') {
        if (receiverWs && receiverWs.readyState === WebSocket.OPEN) {
          receiverWs.send(JSON.stringify({ type: 'rx_ack', index: msg.index }));
        }

        receivedChunks.add(msg.index);
        rxBytesReceived = Math.min(receivedChunks.size * transferData.chunk_size, transferData.filesize);

        const pct = Math.round((receivedChunks.size / total) * 100);
        rxProgressFill.style.width = `${pct}%`;
        rxProgressPct.textContent = `${pct}%`;

        const elapsedSec = (Date.now() - rxStartTime) / 1000;
        if (elapsedSec > 0.2) {
          const speedBytesPerSec = rxBytesReceived / elapsedSec;
          const mbps = (speedBytesPerSec / (1024 * 1024)).toFixed(1);
          rxSpeed.textContent = `${mbps} MB/s`;

          const remainingBytes = transferData.filesize - rxBytesReceived;
          const etaSec = speedBytesPerSec > 0 ? remainingBytes / speedBytesPerSec : 0;
          rxEta.textContent = formatDuration(etaSec);
        }

        rxProgressBytes.textContent = `${formatBytes(rxBytesReceived)} / ${formatBytes(transferData.filesize)}`;
        rxProgressChunks.textContent = `Chunk ${receivedChunks.size}/${total}`;

        if (receivedChunks.size === total) {
          showDownloadReady(code, transferData.filename);
        }
      } else if (msg.type === 'completed') {
        showDownloadReady(code, transferData.filename);
      }
    } catch (err) {
      console.warn('Receiver message error:', err);
    }
  };
}

function showDownloadReady(code, filename) {
  rxStatusText.textContent = '✓ File received & verified';
  rxStatusDot.className = 'status-dot green';
  rxProgressFill.style.width = '100%';
  rxProgressPct.textContent = '100%';
  rxEta.textContent = 'Done';

  rxPendingDownloadUrl = `/api/transfers/${code}/download`;
  rxPendingFilename = filename;
  rxDownloadReadyBox.classList.remove('hidden');
}

btnTriggerDownload.addEventListener('click', () => {
  if (!rxPendingDownloadUrl) return;

  const tempLink = document.createElement('a');
  tempLink.href = rxPendingDownloadUrl;
  tempLink.download = rxPendingFilename || 'download';
  document.body.appendChild(tempLink);
  tempLink.click();
  document.body.removeChild(tempLink);

  rxDownloadReadyBox.classList.add('hidden');
  rxProgressBox.classList.add('hidden');
  rxClearedBox.classList.remove('hidden');

  rxPendingDownloadUrl = null;
  rxPendingFilename = null;
});

btnReceiveAgain.addEventListener('click', () => {
  resetReceiver();
  receiveCodeInput.focus();
});

function resetReceiver() {
  if (receiverWs) {
    receiverWs.close();
    receiverWs = null;
  }
  receiveCodeInput.value = '';
  receiverStepInput.classList.remove('hidden');
  receiverStepTransfer.classList.add('hidden');
  rxDownloadReadyBox.classList.add('hidden');
  rxClearedBox.classList.add('hidden');
  rxProgressBox.classList.remove('hidden');
}
