// ChunkStream Minimal Frontend Logic with Speed & Peer Names

const ADJECTIVES = ['Brave', 'Swift', 'Cosmic', 'Silent', 'Neon', 'Solar', 'Lunar', 'Electric', 'Clever', 'Amber', 'Mystic', 'Frost', 'Bold', 'Agile', 'Shadow', 'Velvet'];
const ANIMALS = ['Falcon', 'Otter', 'Lynx', 'Dolphin', 'Fox', 'Eagle', 'Hawk', 'Badger', 'Panda', 'Tiger', 'Wolf', 'Phoenix', 'Rabbit', 'Owl', 'Panther', 'Koala'];

function generateRandomPeerName() {
  const adj = ADJECTIVES[Math.floor(Math.random() * ADJECTIVES.length)];
  const animal = ANIMALS[Math.floor(Math.random() * ANIMALS.length)];
  return `${adj} ${animal}`;
}

let myPeerName = sessionStorage.getItem('cs_peer_name');
if (!myPeerName) {
  myPeerName = generateRandomPeerName();
  sessionStorage.setItem('cs_peer_name', myPeerName);
}
document.getElementById('global-peer-name').textContent = myPeerName;

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
const senderSpeed = document.getElementById('sender-speed');
const senderProgressDetails = document.getElementById('sender-progress-details');
const senderChunkDetails = document.getElementById('sender-chunk-details');
const btnSendAgain = document.getElementById('btn-send-again');

// Receiver Elements
const receiverStepInput = document.getElementById('receiver-step-input');
const receiverStepTransfer = document.getElementById('receiver-step-transfer');
const receiveCodeInput = document.getElementById('receive-code-input');
const btnStartReceive = document.getElementById('btn-start-receive');
const rxFileName = document.getElementById('rx-file-name');
const rxFileSize = document.getElementById('rx-file-size');
const rxStatusText = document.getElementById('rx-status-text');
const rxStatusSub = document.getElementById('rx-status-sub');
const rxProgressBox = document.getElementById('rx-progress-box');
const rxProgressFill = document.getElementById('rx-progress-fill');
const rxProgressPct = document.getElementById('rx-progress-pct');
const rxSpeed = document.getElementById('rx-speed');
const rxProgressDetails = document.getElementById('rx-progress-details');
const rxChunkDetails = document.getElementById('rx-chunk-details');
const rxDownloadReadyBox = document.getElementById('rx-download-ready-box');
const btnTriggerDownload = document.getElementById('btn-trigger-download');
const rxClearedBox = document.getElementById('rx-cleared-box');
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
  senderStatusSub.textContent = `You are ${myPeerName}. Transfer begins once receiver connects`;
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

  senderWs.onopen = () => {
    // Announce peer name
    senderWs.send(JSON.stringify({ type: 'peer_info', peer_name: myPeerName, role: 'sender' }));
  };

  senderWs.onmessage = (e) => {
    try {
      const msg = JSON.parse(e.data);
      // Transfer ONLY starts when receiver connects and requests chunks!
      if (msg.type === 'peer_connected' && msg.role === 'receiver') {
        startStreamingChunks(null, msg.peer_name);
      } else if (msg.type === 'request_missing' || msg.type === 'receiver_ready') {
        startStreamingChunks(msg.missing_chunks, msg.peer_name);
      } else if (msg.type === 'peer_info' && msg.role === 'receiver') {
        senderStatusSub.textContent = `Connected to ${msg.peer_name || 'receiver'}`;
      } else if (msg.type === 'chunk_ack') {
        handleChunkAck(msg.index);
      }
    } catch (err) {
      console.warn("Sender WS error:", err);
    }
  };
}

function startStreamingChunks(missing, peerName) {
  if (isStreaming) return;
  isStreaming = true;
  senderStartTime = Date.now();

  const receiverLabel = peerName ? `to ${peerName}` : 'to receiver';
  senderStatusText.textContent = `Connected! Sending file ${receiverLabel}...`;
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
      await new Promise(r => setTimeout(r, 20));
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

    await new Promise(r => setTimeout(r, 10));
  }
}

function handleChunkAck(index) {
  inFlightChunks.delete(index);
  ackedChunks.add(index);
  bytesSent = Math.min(ackedChunks.size * chunkSize, currentFile.size);

  const pct = Math.round((ackedChunks.size / totalChunks) * 100);
  senderProgressFill.style.width = `${pct}%`;
  senderProgressPct.textContent = `${pct}%`;

  // Calculate speed
  const elapsedSec = (Date.now() - senderStartTime) / 1000;
  if (elapsedSec > 0.3) {
    const mbps = ((bytesSent / (1024 * 1024)) / elapsedSec).toFixed(1);
    senderSpeed.textContent = `${mbps} MB/s`;
  }

  senderProgressDetails.textContent = `${formatBytes(bytesSent)} / ${formatBytes(currentFile.size)}`;
  senderChunkDetails.textContent = `Chunk ${ackedChunks.size}/${totalChunks}`;

  if (ackedChunks.size === totalChunks) {
    senderStatusText.textContent = '✓ Transfer complete!';
    senderStatusSub.textContent = 'All chunks uploaded and verified';
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
    rxStatusSub.textContent = `You are ${myPeerName}`;
    rxProgressFill.style.width = '0%';
    rxProgressPct.textContent = '0%';
    rxSpeed.textContent = '0.0 MB/s';
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
    rxStatusText.textContent = 'Receiving file...';
    // Tell sender we are ready and announce name
    receiverWs.send(JSON.stringify({ 
      type: 'resume_request', 
      peer_name: myPeerName 
    }));
  };

  receiverWs.onmessage = (e) => {
    try {
      const msg = JSON.parse(e.data);
      if (msg.type === 'peer_info' && msg.role === 'sender') {
        rxStatusSub.textContent = `Receiving from ${msg.peer_name || 'sender'}`;
      } else if (msg.type === 'chunk_data') {
        receivedChunks.add(msg.index);
        rxBytesReceived = Math.min(receivedChunks.size * transferData.chunk_size, transferData.filesize);

        const pct = Math.round((receivedChunks.size / total) * 100);
        rxProgressFill.style.width = `${pct}%`;
        rxProgressPct.textContent = `${pct}%`;

        // Calculate speed
        const elapsedSec = (Date.now() - rxStartTime) / 1000;
        if (elapsedSec > 0.3) {
          const mbps = ((rxBytesReceived / (1024 * 1024)) / elapsedSec).toFixed(1);
          rxSpeed.textContent = `${mbps} MB/s`;
        }

        rxProgressDetails.textContent = `${formatBytes(rxBytesReceived)} / ${formatBytes(transferData.filesize)}`;
        rxChunkDetails.textContent = `Chunk ${receivedChunks.size}/${total}`;

        if (receivedChunks.size === total) {
          showDownloadReady(code, transferData.filename);
        }
      } else if (msg.type === 'completed') {
        showDownloadReady(code, transferData.filename);
      }
    } catch (err) {
      console.warn("Receiver WS error:", err);
    }
  };
}

function showDownloadReady(code, filename) {
  rxStatusText.textContent = '✓ File received!';
  rxStatusSub.textContent = 'Assembled and verified on server';
  rxProgressFill.style.width = '100%';
  rxProgressPct.textContent = '100%';

  rxPendingDownloadUrl = `/api/transfers/${code}/download`;
  rxPendingFilename = filename;

  // Show "Click here to download" button (do NOT auto-download yet!)
  rxDownloadReadyBox.classList.remove('hidden');
}

// User clicks download -> file is downloaded by browser -> then cleared!
btnTriggerDownload.addEventListener('click', () => {
  if (!rxPendingDownloadUrl) return;

  // 1. Trigger browser file download
  const tempLink = document.createElement('a');
  tempLink.href = rxPendingDownloadUrl;
  tempLink.download = rxPendingFilename || 'download';
  document.body.appendChild(tempLink);
  tempLink.click();
  document.body.removeChild(tempLink);

  // 2. Clear receiver view and show completion
  rxDownloadReadyBox.classList.add('hidden');
  rxProgressBox.classList.add('hidden');
  rxStatusText.textContent = '';
  rxStatusSub.textContent = '';
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

function resetAll() {
  resetSender();
  resetReceiver();
}
