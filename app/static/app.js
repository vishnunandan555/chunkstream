// ChunkStream Minimal Frontend Logic

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

// Format helper
function formatBytes(bytes) {
  if (bytes === 0) return '0 B';
  const k = 1024;
  const sizes = ['B', 'KB', 'MB', 'GB'];
  const i = Math.floor(Math.log(bytes) / Math.log(k));
  return parseFloat((bytes / Math.pow(k, i)).toFixed(1)) + ' ' + sizes[i];
}

function generate4DigitCode() {
  return Math.floor(1000 + Math.random() * 9000).toString();
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
const dropBox = document.getElementById('drop-box');
const senderStepPick = document.getElementById('sender-step-pick');
const senderStepTransfer = document.getElementById('sender-step-transfer');
const sendFileName = document.getElementById('send-file-name');
const sendFileSize = document.getElementById('send-file-size');
const senderCodeDisplay = document.getElementById('sender-code-display');
const btnCopyCode = document.getElementById('btn-copy-code');
const senderStatusText = document.getElementById('sender-status-text');
const senderStatusSub = document.getElementById('sender-status-sub');
const senderStatusDot = document.getElementById('sender-status-dot');
const senderProgressBox = document.getElementById('sender-progress-box');
const senderProgressFill = document.getElementById('sender-progress-fill');
const senderProgressPct = document.getElementById('sender-progress-pct');
const senderProgressDetails = document.getElementById('sender-progress-details');
const btnSendAgain = document.getElementById('btn-send-again');

// Receiver Elements
const receiverStepInput = document.getElementById('receiver-step-input');
const receiverStepTransfer = document.getElementById('receiver-step-transfer');
const receiveCodeInput = document.getElementById('receive-code-input');
const btnStartReceive = document.getElementById('btn-start-receive');
const rxFileName = document.getElementById('rx-file-name');
const rxFileSize = document.getElementById('rx-file-size');
const rxStatusText = document.getElementById('rx-status-text');
const rxProgressFill = document.getElementById('rx-progress-fill');
const rxProgressPct = document.getElementById('rx-progress-pct');
const rxProgressDetails = document.getElementById('rx-progress-details');
const rxDownloadLink = document.getElementById('rx-download-link');
const btnReceiveAgain = document.getElementById('btn-receive-again');

// Navigation between views
function showView(view) {
  viewHome.classList.add('hidden');
  viewSend.classList.add('hidden');
  viewReceive.classList.add('hidden');
  view.classList.remove('hidden');
}

homeLink.addEventListener('click', () => {
  resetAll();
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

// ----------------------------------------------------
// SENDER FLOW
// ----------------------------------------------------
dropBox.addEventListener('click', () => fileInput.click());

dropBox.addEventListener('dragover', (e) => {
  e.preventDefault();
  dropBox.classList.add('dragover');
});

dropBox.addEventListener('dragleave', () => dropBox.classList.remove('dragover'));

dropBox.addEventListener('drop', (e) => {
  e.preventDefault();
  dropBox.classList.remove('dragover');
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
  navigator.clipboard.writeText(currentCode).then(() => {
    btnCopyCode.textContent = 'Copied!';
    setTimeout(() => btnCopyCode.textContent = 'Copy Code', 1500);
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
  senderStatusSub.textContent = 'Transfer will begin automatically once receiver enters this code';
  senderStatusDot.className = 'pulse-dot';
  senderProgressBox.classList.add('hidden');
  btnSendAgain.classList.add('hidden');

  // Register transfer on server
  try {
    await fetch('/api/transfers', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        transfer_id: currentCode,
        filename: file.name,
        filesize: file.size,
        chunk_size: chunkSize,
        total_chunks: totalChunks
      })
    });
  } catch (err) {
    console.error("Could not register transfer:", err);
  }

  // Connect sender socket
  connectSenderWebSocket();
}

function connectSenderWebSocket() {
  const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
  senderWs = new WebSocket(`${protocol}//${window.location.host}/ws/transfer/${currentCode}?role=sender`);

  senderWs.onmessage = (e) => {
    try {
      const msg = JSON.parse(e.data);
      // Transfer ONLY starts when receiver connects and requests chunks!
      if (msg.type === 'peer_connected' && msg.role === 'receiver') {
        startStreamingChunks();
      } else if (msg.type === 'request_missing' || msg.type === 'receiver_ready') {
        startStreamingChunks(msg.missing_chunks);
      } else if (msg.type === 'chunk_ack') {
        handleChunkAck(msg.index);
      }
    } catch (err) {
      console.warn("Sender WS error:", err);
    }
  };
}

function startStreamingChunks(missing) {
  if (isStreaming) return;
  isStreaming = true;

  senderStatusText.textContent = 'Receiver connected! Sending file...';
  senderStatusSub.textContent = 'Streaming chunks in real time';
  senderStatusDot.className = 'pulse-dot green';
  senderProgressBox.classList.remove('hidden');

  streamFileChunks(missing);
}

async function streamFileChunks(missingList) {
  const chunksToSend = missingList && missingList.length > 0 
    ? missingList 
    : Array.from({ length: totalChunks }, (_, i) => i);

  for (const idx of chunksToSend) {
    if (ackedChunks.has(idx)) continue;

    // Concurrency limit
    while (inFlightChunks.size >= 3) {
      await new Promise(r => setTimeout(r, 25));
    }

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
        data: b64
      }));
    }

    await new Promise(r => setTimeout(r, 15));
  }
}

function handleChunkAck(index) {
  inFlightChunks.delete(index);
  ackedChunks.add(index);
  bytesSent = Math.min(ackedChunks.size * chunkSize, currentFile.size);

  const pct = Math.round((ackedChunks.size / totalChunks) * 100);
  senderProgressFill.style.width = `${pct}%`;
  senderProgressPct.textContent = `${pct}%`;
  senderProgressDetails.textContent = `${formatBytes(bytesSent)} / ${formatBytes(currentFile.size)}`;

  if (ackedChunks.size === totalChunks) {
    senderStatusText.textContent = '✓ Transfer complete!';
    senderStatusSub.textContent = 'Receiver has successfully downloaded the file';
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

// ----------------------------------------------------
// RECEIVER FLOW
// ----------------------------------------------------
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

  // Get transfer details from server
  try {
    const res = await fetch(`/api/transfers/${code}`);
    if (!res.ok) throw new Error('Transfer code not found or expired');
    const data = await res.json();

    rxFileName.textContent = data.filename;
    rxFileSize.textContent = formatBytes(data.filesize);

    receiverStepInput.classList.add('hidden');
    receiverStepTransfer.classList.remove('hidden');
    rxStatusText.textContent = 'Connecting to sender...';
    rxProgressFill.style.width = '0%';
    rxProgressPct.textContent = '0%';

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
    rxStatusText.textContent = 'Receiving file...';
    // Tell sender to start!
    receiverWs.send(JSON.stringify({ type: 'resume_request' }));
  };

  receiverWs.onmessage = (e) => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'chunk_data') {
        receivedChunks.add(msg.index);
        const pct = Math.round((receivedChunks.size / total) * 100);
        rxProgressFill.style.width = `${pct}%`;
        rxProgressPct.textContent = `${pct}%`;
        rxProgressDetails.textContent = `${formatBytes(receivedChunks.size * transferData.chunk_size)} / ${formatBytes(transferData.filesize)}`;

        if (receivedChunks.size === total) {
          triggerAutoDownload(code, transferData.filename);
        }
      } else if (msg.type === 'completed') {
        triggerAutoDownload(code, transferData.filename);
      }
    } catch (err) {
      console.warn("Receiver WS error:", err);
    }
  };
}

function triggerAutoDownload(code, filename) {
  rxStatusText.textContent = '✓ Download complete!';
  rxProgressFill.style.width = '100%';
  rxProgressPct.textContent = '100%';

  const downloadUrl = `/api/transfers/${code}/download`;
  rxDownloadLink.href = downloadUrl;
  rxDownloadLink.download = filename;
  rxDownloadLink.classList.remove('hidden');
  btnReceiveAgain.classList.remove('hidden');

  // Trigger automatic download
  const tempLink = document.createElement('a');
  tempLink.href = downloadUrl;
  tempLink.download = filename;
  document.body.appendChild(tempLink);
  tempLink.click();
  document.body.removeChild(tempLink);
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
  receiveCodeInput.value = '';
  receiverStepInput.classList.remove('hidden');
  receiverStepTransfer.classList.add('hidden');
  rxDownloadLink.classList.add('hidden');
  btnReceiveAgain.classList.add('hidden');
}

function resetAll() {
  resetSender();
  resetReceiver();
}
