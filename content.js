(function () {
  'use strict';

  // ─── inject page-context script ──────────────────────────────────────────────
  const s = document.createElement('script');
  s.src = chrome.runtime.getURL('injected.js');
  document.documentElement.prepend(s);
  s.onload = () => s.remove();

  // ─── icons ───────────────────────────────────────────────────────────────────
  const ICON_TRIGGER = `<svg width="24" height="24" viewBox="0 0 24 24" fill="none">
    <path d="M12 4v11" stroke="currentColor" stroke-width="1.8" stroke-linecap="round"/>
    <path d="M8.5 7.5L12 4l3.5 3.5" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round"/>
    <path d="M4 17.5h16" stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/>
    <path d="M7 21h10" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/>
  </svg>`;
  const ICON_UPLOAD = `<svg width="14" height="14" viewBox="0 0 14 14" fill="none">
    <path d="M7 1v8M4.5 3.5L7 1l2.5 2.5" stroke="currentColor" stroke-width="1.4" stroke-linecap="round" stroke-linejoin="round"/>
    <path d="M2 10.5v1.5a1 1 0 001 1h8a1 1 0 001-1v-1.5" stroke="currentColor" stroke-width="1.4" stroke-linecap="round"/>
  </svg>`;
  const ICON_CLOSE = `<svg width="12" height="12" viewBox="0 0 12 12" fill="none">
    <path d="M1.5 1.5l9 9M10.5 1.5l-9 9" stroke="currentColor" stroke-width="1.6" stroke-linecap="round"/>
  </svg>`;
  const ICON_SETTINGS = `<svg width="16" height="16" viewBox="0 0 16 16" fill="currentColor">
    <path d="M8 4.754a3.246 3.246 0 1 0 0 6.492 3.246 3.246 0 0 0 0-6.492zM5.754 8a2.246 2.246 0 1 1 4.492 0 2.246 2.246 0 0 1-4.492 0z"/>
    <path d="M9.796 1.343c-.527-1.79-3.065-1.79-3.592 0l-.094.319a.873.873 0 0 1-1.255.52l-.292-.16c-1.64-.892-3.433.902-2.54 2.541l.159.292a.873.873 0 0 1-.52 1.255l-.319.094c-1.79.527-1.79 3.065 0 3.592l.319.094a.873.873 0 0 1 .52 1.255l-.16.292c-.892 1.64.901 3.434 2.541 2.54l.292-.159a.873.873 0 0 1 1.255.52l.094.319c.527 1.79 3.065 1.79 3.592 0l.094-.319a.873.873 0 0 1 1.255-.52l.292.16c1.64.892 3.433-.902 2.54-2.541l-.159-.292a.873.873 0 0 1 .52-1.255l.319-.094c1.79-.527 1.79-3.065 0-3.592l-.319-.094a.873.873 0 0 1-.52-1.255l.16-.292c.892-1.64-.901-3.433-2.541-2.54l-.292.159a.873.873 0 0 1-1.255-.52l-.094-.319zm-2.633.283c.246-.835 1.428-.835 1.674 0l.094.319a1.873 1.873 0 0 0 2.693 1.115l.291-.16c.764-.415 1.6.42 1.184 1.185l-.159.292a1.873 1.873 0 0 0 1.116 2.692l.318.094c.835.246.835 1.428 0 1.674l-.319.094a1.873 1.873 0 0 0-1.115 2.693l.16.291c.415.764-.42 1.6-1.185 1.184l-.291-.159a1.873 1.873 0 0 0-2.693 1.116l-.094.318c-.246.835-1.428.835-1.674 0l-.094-.319a1.873 1.873 0 0 0-2.692-1.115l-.292.16c-.764.415-1.6-.42-1.184-1.185l.159-.291A1.873 1.873 0 0 0 1.945 8.93l-.319-.094c-.835-.246-.835-1.428 0-1.674l.319-.094A1.873 1.873 0 0 0 3.06 4.47l-.16-.292c-.415-.764.42-1.6 1.185-1.184l.292.159a1.873 1.873 0 0 0 2.692-1.115l.094-.319z"/>
  </svg>`;
  const STATUS_ICON = {
    pending: `<svg width="14" height="14" viewBox="0 0 14 14" fill="none"><circle cx="7" cy="7" r="5.5" stroke="#555" stroke-width="1.3"/><rect x="5" y="4.5" width="1.4" height="5" rx="0.5" fill="#555"/><rect x="7.6" y="4.5" width="1.4" height="5" rx="0.5" fill="#555"/></svg>`,
    uploading: `<svg class="vmu-spin" width="14" height="14" viewBox="0 0 14 14" fill="none"><circle cx="7" cy="7" r="5.5" stroke="rgba(38,136,235,0.2)" stroke-width="1.5"/><path d="M7 1.5A5.5 5.5 0 0112.5 7" stroke="#2688eb" stroke-width="1.5" stroke-linecap="round"/></svg>`,
    done: `<svg width="14" height="14" viewBox="0 0 14 14" fill="none"><circle cx="7" cy="7" r="5.5" fill="rgba(75,179,75,0.14)" stroke="#4bb34b" stroke-width="1.3"/><path d="M4.5 7l2 2 3-3" stroke="#4bb34b" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/></svg>`,
    error: `<svg width="14" height="14" viewBox="0 0 14 14" fill="none"><circle cx="7" cy="7" r="5.5" fill="rgba(230,70,70,0.12)" stroke="#e64646" stroke-width="1.3"/><path d="M5 5l4 4M9 5l-4 4" stroke="#e64646" stroke-width="1.3" stroke-linecap="round"/></svg>`,
  };

  // ─── state ───────────────────────────────────────────────────────────────────
  let fileQueue = [];
  let isProcessing = false;
  let uploadDoneCallback = null;
  let autoPlaylistRunning = false;
  let isPaused = false;
  let currentUploadingItem = null;
  let itemIdCounter = 0;

  // ─── playlist download state ──────────────────────────────────────────────────
  const dlTracks = new Map();   // trackId -> {id, title, artist, url}
  let dlCancelFlag = false;

  window.addEventListener('message', (e) => {
    if (e.source !== window) return;
    if (e.data?.type === 'VK_UPLOAD_DONE' && uploadDoneCallback) {
      uploadDoneCallback(e.data);
      uploadDoneCallback = null;
    }
    if (e.data?.type === 'VK_COVER_DONE' && window.__vmuCoverCallback) {
      window.__vmuCoverCallback(e.data);
      window.__vmuCoverCallback = null;
    }
    if (e.data?.type === 'VKD_HLS_PROGRESS') {
      const cb = hlsProgressHandlers.get(e.data.trackId);
      if (cb) cb(e.data.done, e.data.total);
    }
    if (e.data?.type === 'VKD_TRACK') {
      const t = e.data.track;
      if (t?.id && !dlTracks.has(t.id)) dlTracks.set(t.id, t);
    }
    if (e.data?.type === 'VK_UPLOAD_PROGRESS' && currentUploadingItem) {
      const total = e.data.total || 0;
      if (total > 0) {
        currentUploadingItem.progress = e.data.loaded / total;
        updateRowProgress(currentUploadingItem.id);
      }
    }
  });

  // ─── helpers ─────────────────────────────────────────────────────────────────
  const sleep = (ms) => new Promise(r => setTimeout(r, ms));
  const isMP3 = (f) => f.type === 'audio/mpeg' || f.name.toLowerCase().endsWith('.mp3');
  const fmtSize = (b) => b < 1048576 ? (b / 1024).toFixed(0) + ' KB' : (b / 1048576).toFixed(1) + ' MB';
  const escHtml = (s) => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

  function waitForElement(sel, ms) {
    return new Promise(resolve => {
      const el = document.querySelector(sel);
      if (el) { resolve(el); return; }
      const obs = new MutationObserver(() => {
        const found = document.querySelector(sel);
        if (found) { obs.disconnect(); resolve(found); }
      });
      obs.observe(document.body, { childList: true, subtree: true });
      setTimeout(() => { obs.disconnect(); resolve(null); }, ms);
    });
  }

  // ─── settings ────────────────────────────────────────────────────────────────
  const SETTINGS_KEY = 'vmu_settings_v2';
  let settings = { autoPlaylist: false, coverDataUrl: null, autoMeta: false, autoCoverFromId3: false };

  function loadSettings() {
    try {
      const s = localStorage.getItem(SETTINGS_KEY);
      if (s) Object.assign(settings, JSON.parse(s));
    } catch {}
  }

  function saveSettings() {
    try {
      localStorage.setItem(SETTINGS_KEY, JSON.stringify({
        autoPlaylist: settings.autoPlaylist,
        coverDataUrl: settings.coverDataUrl,
        autoMeta: settings.autoMeta,
        autoCoverFromId3: settings.autoCoverFromId3,
      }));
    } catch {}
  }

  loadSettings();

  // ─── filename → meta parser ───────────────────────────────────────────────────
  function parseMetaFromFilename(filename) {
    function cleanPart(s) {
      return s
        .replace(/_/g, ' ')           // underscores → spaces
        .replace(/\s+/g, ' ')         // collapse multiple spaces
        .replace(/^[\s\-–—_.,()\[\]]+|[\s\-–—_.,()\[\]]+$/g, '') // trim junk edges
        .trim();
    }
    // Strip extension, remove leading track number (e.g. "01. ", "02 - ")
    const base = filename.replace(/\.[^.]+$/, '').replace(/^\d+[\s.\-–—]+/, '').trim();
    const parts = base.split(/\s*[-–—]\s*/);
    if (parts.length >= 2) {
      return { artist: cleanPart(parts[0]), title: cleanPart(parts.slice(1).join(' – ')) };
    }
    return { artist: '', title: cleanPart(base) };
  }

  // ─── ID3v2.3 tag writer ───────────────────────────────────────────────────────
  function makeID3Frame(id, text) {
    const textBytes = new TextEncoder().encode(text);
    const frameData = new Uint8Array(1 + textBytes.length);
    frameData[0] = 3; // UTF-8 encoding byte
    frameData.set(textBytes, 1);
    const frame = new Uint8Array(10 + frameData.length);
    for (let i = 0; i < 4; i++) frame[i] = id.charCodeAt(i);
    const sz = frameData.length;
    frame[4] = (sz >> 24) & 0xff; frame[5] = (sz >> 16) & 0xff;
    frame[6] = (sz >> 8) & 0xff;  frame[7] = sz & 0xff;
    frame.set(frameData, 10);
    return frame;
  }

  function buildID3v2(frames) {
    const total = frames.reduce((s, f) => s + f.length, 0);
    const tag = new Uint8Array(10 + total);
    tag[0] = 0x49; tag[1] = 0x44; tag[2] = 0x33; // "ID3"
    tag[3] = 3; tag[4] = 0; tag[5] = 0; // version 2.3, no flags
    // Syncsafe integer for tag size
    tag[6] = (total >> 21) & 0x7f; tag[7] = (total >> 14) & 0x7f;
    tag[8] = (total >> 7) & 0x7f;  tag[9] = total & 0x7f;
    let off = 10;
    for (const f of frames) { tag.set(f, off); off += f.length; }
    return tag;
  }

  async function patchID3(file, artist, title) {
    const buf = await file.arrayBuffer();
    const v = new Uint8Array(buf);
    // Skip any existing ID3 tag so we don't double-wrap
    let audioStart = 0;
    if (v[0] === 0x49 && v[1] === 0x44 && v[2] === 0x33) {
      const tagSize = ((v[6]&0x7f)<<21)|((v[7]&0x7f)<<14)|((v[8]&0x7f)<<7)|(v[9]&0x7f);
      audioStart = 10 + tagSize;
    }
    const frames = [];
    if (artist) frames.push(makeID3Frame('TPE1', artist));
    if (title)  frames.push(makeID3Frame('TIT2', title));
    const newTag = buildID3v2(frames);
    const combined = new Uint8Array(newTag.length + v.length - audioStart);
    combined.set(newTag, 0);
    combined.set(v.subarray(audioStart), newTag.length);
    return new File([combined], file.name, { type: file.type || 'audio/mpeg' });
  }

  // ─── ID3 tag reader ───────────────────────────────────────────────────────────
  async function readID3(file) {
    try {
      // Larger slice so embedded APIC cover art (often 200KB–1MB JPEG) fits
      const buf = await file.slice(0, 2_097_152).arrayBuffer();
      const v = new Uint8Array(buf);
      if (v[0] !== 73 || v[1] !== 68 || v[2] !== 51) return {};
      const ver = v[3];
      const hasExt = !!(v[5] & 0x40);
      const tagSize = ((v[6]&0x7f)<<21)|((v[7]&0x7f)<<14)|((v[8]&0x7f)<<7)|(v[9]&0x7f);
      let p = 10;
      if (hasExt) p += ((v[p]<<24)|(v[p+1]<<16)|(v[p+2]<<8)|v[p+3]) + 4;
      const end = Math.min(10 + tagSize, v.length);
      const tags = {};
      while (p < end - 10 && v[p]) {
        const id = String.fromCharCode(v[p],v[p+1],v[p+2],v[p+3]);
        const sz = ver >= 4
          ? ((v[p+4]&0x7f)<<21)|((v[p+5]&0x7f)<<14)|((v[p+6]&0x7f)<<7)|(v[p+7]&0x7f)
          : (v[p+4]<<24)|(v[p+5]<<16)|(v[p+6]<<8)|v[p+7];
        p += 10;
        if (sz > 0 && sz < 32768 && id[0] === 'T') {
          const enc = v[p];
          const bytes = v.slice(p+1, p+sz);
          try {
            tags[id] = new TextDecoder(
              enc === 0 ? 'iso-8859-1' : enc === 3 ? 'utf-8' : 'utf-16'
            ).decode(bytes).replace(/\0/g,'').trim();
          } catch {
            tags[id] = new TextDecoder('utf-8',{fatal:false}).decode(bytes).replace(/\0/g,'').trim();
          }
        } else if (id === 'APIC' && sz > 0 && p + sz <= v.length && !tags.APIC) {
          try {
            const frameEnd = p + sz;
            const enc = v[p];
            let q = p + 1;
            // MIME type (ASCII, null-terminated)
            const mimeStart = q;
            while (q < frameEnd && v[q] !== 0) q++;
            const mime = String.fromCharCode(...v.slice(mimeStart, q));
            q++;                 // skip null
            if (q < frameEnd) q++; // picture type byte
            // Description (encoding-dependent null terminator)
            if (enc === 1 || enc === 2) {
              while (q + 1 < frameEnd && !(v[q] === 0 && v[q+1] === 0)) q++;
              q += 2;
            } else {
              while (q < frameEnd && v[q] !== 0) q++;
              q++;
            }
            const data = v.slice(q, frameEnd);
            if (data.length > 100 && /^image\//.test(mime)) {
              tags.APIC = { mime, data };
            }
          } catch {}
        }
        p += Math.max(0, sz);
      }
      return tags;
    } catch { return {}; }
  }

  // ─── cover compositor ─────────────────────────────────────────────────────────
  function makePerezalitoCover(coverDataUrl) {
    return new Promise(resolve => {
      const canvas = document.createElement('canvas');
      canvas.width = 1000; canvas.height = 1000;
      const ctx = canvas.getContext('2d');
      const img = new Image();
      img.onload = () => {
        ctx.drawImage(img, 0, 0, 1000, 1000);
        ctx.save();
        ctx.translate(500, 500);
        ctx.rotate(-Math.PI / 4);
        ctx.font = 'bold italic 132px Georgia,"Times New Roman",serif';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.shadowColor = 'rgba(0,0,0,0.55)';
        ctx.shadowBlur = 12;
        ctx.fillStyle = 'rgba(230,0,0,0.92)';
        ctx.fillText('перезалито', 0, 0);
        ctx.restore();
        canvas.toBlob(resolve, 'image/jpeg', 0.92);
      };
      img.onerror = () => resolve(null);
      img.src = coverDataUrl;
    });
  }

  function getVkUserId() {
    // URL-first: works for any user (/audiosXXX) or group (/audios-XXX) page
    const m = location.href.match(/audios(-?\d+)/);
    if (m) return m[1];
    // Fallback: localStorage key for personal pages only
    try {
      for (const k of Object.keys(localStorage)) {
        if (k.startsWith('audio_v21_track_')) {
          const id = k.slice('audio_v21_track_'.length);
          if (/^\d+$/.test(id)) return id;
        }
      }
    } catch {}
    return null;
  }

  function translateError(msg) {
    const map = {
      btn_create_playlist_not_found: 'Кнопка "Создать плейлист" не найдена. Перейдите на страницу своей музыки (/audios) и попробуйте снова.',
      dialog_not_opened: 'Диалог создания плейлиста не открылся.',
      save_btn_not_found: 'Кнопка "Сохранить" не найдена в диалоге.',
      save_playlist_timeout: 'Плейлист не был сохранён за 20 секунд.',
      playlist_id_not_found: 'Не удалось получить ID созданного плейлиста.',
    };
    return map[msg] || msg;
  }

  // Send message to injected.js and wait for response
  function pageCall(sendType, responseType, payload, timeoutMs = 15000) {
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => {
        window.removeEventListener('message', handler);
        reject(new Error(`Timeout: ${responseType}`));
      }, timeoutMs);
      function handler(e) {
        if (e.source !== window || e.data?.type !== responseType) return;
        window.removeEventListener('message', handler);
        clearTimeout(t);
        const d = e.data;
        d.ok !== false ? resolve(d) : reject(new Error(d.error || responseType + ' failed'));
      }
      window.addEventListener('message', handler);
      // Collect all transferable ArrayBuffers from payload
      const transfers = [];
      if (payload) {
        for (const v of Object.values(payload)) {
          if (v instanceof ArrayBuffer) transfers.push(v);
        }
      }
      window.postMessage({ type: sendType, ...payload }, '*', transfers);
    });
  }

  // ─── cover upload via VK dialog ───────────────────────────────────────────────
  async function uploadCoverViaDialog(coverBlob) {
    const buf = await coverBlob.arrayBuffer();
    return new Promise((resolve, reject) => {
      const t = setTimeout(() => {
        window.__vmuCoverCallback = null;
        reject(new Error('Timeout загрузки обложки'));
      }, 45000);
      window.__vmuCoverCallback = (data) => {
        clearTimeout(t);
        data.ok ? resolve() : reject(new Error(data.error || 'Ошибка обложки'));
      };
      window.postMessage({ type: 'VK_UPLOAD_COVER', buffer: buf }, '*', [buf]);
    });
  }

  // ─── status helpers ───────────────────────────────────────────────────────────
  function setPlaylistStatus(text, isError, progress) {
    const el = document.getElementById('vmu-pl-status');
    if (!el) return;
    let textEl = el.querySelector('.vmu-pl-status-text');
    let progWrap = el.querySelector('.vmu-pl-progress');
    let bar = el.querySelector('.vmu-pl-progress-bar');
    // Backfill DOM if a legacy element exists without children
    if (!textEl) {
      el.innerHTML = '<div class="vmu-pl-status-text"></div><div class="vmu-pl-progress"><div class="vmu-pl-progress-bar"></div></div>';
      textEl = el.querySelector('.vmu-pl-status-text');
      progWrap = el.querySelector('.vmu-pl-progress');
      bar = el.querySelector('.vmu-pl-progress-bar');
    }
    textEl.textContent = text || '';
    textEl.style.color = isError ? '#e64646' : '#4bb34b';
    el.style.display = text ? 'block' : 'none';
    if (progress && progress.total > 0) {
      const pct = Math.min(100, Math.max(0, (progress.loaded / progress.total) * 100));
      progWrap.style.display = 'block';
      bar.style.width = pct + '%';
      bar.classList.toggle('vmu-pl-progress-bar-error', !!isError);
    } else if (progWrap) {
      progWrap.style.display = 'none';
    }
  }

  // ─── auto-playlist flow ───────────────────────────────────────────────────────
  async function runAutoPlaylist(uploadedItems) {
    const done = uploadedItems.filter(i => i.status === 'done');
    if (!done.length) return;

    const ownerId = getVkUserId();
    if (!ownerId) { setPlaylistStatus('Ошибка: ID пользователя не найден', true); return; }

    setPlaylistStatus('Читаем метаданные…');

    try {
      const tagsList = done.map(i => i.tags || {});
      const albums  = [...new Set(tagsList.map(t => t.TALB).filter(Boolean))];
      const artists = [...new Set([
        ...tagsList.map(t => t.TPE2).filter(Boolean),
        ...tagsList.map(t => t.TPE1).filter(Boolean),
      ])].slice(0, 5);

      // Template: "Альбом (Год) Исполнитель"
      const album  = albums[0] || '';
      const year   = [...new Set(tagsList.map(t => t.TYER || t.TDRC?.substring(0,4)).filter(Boolean))][0] || '';
      const artist = artists[0] || '';
      let title = album;
      if (year)   title += ' (' + year + ')';
      if (artist) title += ' ' + artist;
      if (!title) title = done[0].file.name.replace(/\.mp3$/i,'') || 'Плейлист';

      // Description: название + подпись
      const description = title + '\nчеловек паук поможет каждому [vk.com/reuploadunder]';

      // Build track names list for matching in the edit dialog (in upload order)
      const trackNames = done.map(i => {
        const tags = i.tags || {};
        const tagArtist = tags.TPE1 || tags.TPE2 || '';
        const tagTitle = tags.TIT2 || '';
        if (tagArtist && tagTitle) return { artist: tagArtist, title: tagTitle };
        const name = i.file.name.replace(/\.mp3$/i, '');
        const parts = name.split(/\s*[-–—]\s*/);
        if (parts.length >= 2) return { artist: parts[0].trim(), title: parts.slice(1).join(' - ').trim() };
        return { artist: '', title: name };
      });

      // Prepare cover blob BEFORE opening the dialog (inject it during creation).
      // Priority: user-set base cover from settings → if the "Обложка из ID3"
      // toggle is on, first file's embedded ID3 APIC frame. Either source is
      // passed through makePerezalitoCover so the watermark applies uniformly.
      let coverBlob = null;
      let coverSource = null;
      if (settings.coverDataUrl) {
        coverSource = settings.coverDataUrl;
      } else if (settings.autoCoverFromId3) {
        const apicItem = done.find(i => i.tags?.APIC?.data);
        if (apicItem) {
          setPlaylistStatus('Извлекаем обложку из ID3…');
          const apic = apicItem.tags.APIC;
          const apicBlob = new Blob([apic.data], { type: apic.mime || 'image/jpeg' });
          coverSource = await new Promise(res => {
            const r = new FileReader();
            r.onload = () => res(r.result);
            r.onerror = () => res(null);
            r.readAsDataURL(apicBlob);
          });
        }
      }
      if (coverSource) {
        setPlaylistStatus('Готовим обложку…');
        coverBlob = await makePerezalitoCover(coverSource);
      }

      // Create playlist: opens VK dialog, fills fields, injects cover, selects tracks, saves
      setPlaylistStatus('Создаём плейлист…');
      const created = await pageCall('VK_CREATE_PLAYLIST', 'VK_PLAYLIST_CREATED', {
        title: title.slice(0, 255),
        description: description.slice(0, 1000),
        trackNames,
        coverBuf: coverBlob ? await coverBlob.arrayBuffer() : null,
      }, 60000);

      setPlaylistStatus(`✓ Плейлист «${title.slice(0,30)}» создан!`);
    } catch (err) {
      setPlaylistStatus(`Ошибка: ${translateError(err.message)}`, true);
      console.error('[VK Multi Upload]', err);
    }
  }

  async function openAndInjectCover(ownerId, playlistId, coverBlob) {
    let editBtn = null;
    for (let i = 0; i < 10; i++) {
      editBtn = findPlaylistEditBtn(playlistId);
      if (editBtn) break;
      await sleep(600);
    }
    if (!editBtn) throw new Error('Кнопка редактирования плейлиста не найдена');

    editBtn.click();
    await sleep(700);

    const coverEl = await waitForElement('.ape_cover', 3000);
    if (!coverEl) throw new Error('.ape_cover не найден в диалоге');

    await uploadCoverViaDialog(coverBlob);
  }

  function findPlaylistEditBtn(playlistId) {
    const links = document.querySelectorAll(`[href*="_${playlistId}"], [data-id="${playlistId}"], [data-playlist-id="${playlistId}"]`);
    for (const el of links) {
      let parent = el;
      for (let i = 0; i < 6; i++) {
        parent = parent.parentElement;
        if (!parent) break;
        const btn = parent.querySelector('button');
        if (btn && btn.textContent.includes('едактир')) return btn;
      }
    }
    return null;
  }

  // ─── duplicate finder ─────────────────────────────────────────────────────────
  function getPlaylistInfoFromUrl() {
    const url = location.href;
    // /music/playlist/-206614096_44  or  /music/playlist/-206614096_44_accesshash
    const m1 = url.match(/playlist\/([-\d]+)_(\d+)(?:_([a-zA-Z0-9_]+))?/);
    if (m1) return { ownerId: m1[1], playlistId: m1[2], accessHash: m1[3] || null };
    // ?z=audio_playlist-206614096_44  or  ?z=audio_playlist-206614096_44_accesshash
    const m2 = url.match(/audio_playlist([-\d]+)_(\d+)(?:_([a-zA-Z0-9_]+))?/);
    if (m2) return { ownerId: m2[1], playlistId: m2[2], accessHash: m2[3] || null };
    // legacy ?playlist_id=&owner_id=
    const m3 = url.match(/playlist_id=(\d+)/);
    const m4 = url.match(/[?&]owner_id=([-\d]+)/);
    if (m3 && m4) return { ownerId: m4[1], playlistId: m3[1], accessHash: null };
    return null;
  }

  // Find the playlist container in DOM to scope track search
  function findPlaylistContainer(plInfo) {
    if (plInfo) {
      const plKey = `${plInfo.ownerId}_${plInfo.playlistId}`;
      // Playlist opened in a modal dialog
      const modal = [...document.querySelectorAll('[class*="vkitInternalModalBox"]')]
        .find(m => m.getBoundingClientRect().width > 0);
      if (modal) return modal;
      // Old VK playlist container with matching class
      const oldPl = document.querySelector(`[class*="_audio_pl_${plKey}"]`);
      if (oldPl) return oldPl.closest('.audio_pl_snippet, .AudioPlaylistSnippet') || oldPl;
      // Playlist page container
      const plPage = document.querySelector(`.audio_pl_snippet__list, .AudioPlaylistSnippet__list, [class*="PlaylistAudioList"]`);
      if (plPage) return plPage;
    }
    // Playlist page — use main content area, not the whole page (excludes player bar, recommendations, etc.)
    return document.querySelector('#content, .page_block, [class*="AudioBlock"], [class*="CatalogBlock"]') || document;
  }

  // Parse tracks from DOM scoped to the playlist container
  function getTracksFromDOM(plInfo) {
    const container = findPlaylistContainer(plInfo);
    const tracks = [];
    const seen = new Set();

    const rows = container.querySelectorAll(
      '.audio_row[data-full-id], [data-full-id], [data-audio-id], .AudioRow'
    );

    for (const row of rows) {
      const fullId = row.dataset.fullId || row.dataset.audioId;
      if (!fullId || seen.has(fullId)) continue;

      const titleEl = row.querySelector('.audio_title, .ai_title, [class*="audio_title"], [class*="AudioRow__title"]');
      const artistEl = row.querySelector('.audio_artist, .ai_artist, [class*="audio_artist"], [class*="AudioRow__artist"]');

      if (titleEl || artistEl) {
        seen.add(fullId);
        tracks.push({
          id: fullId.split('_')[1] || fullId,
          owner_id: fullId.split('_')[0] || '',
          fullId,
          title: titleEl?.textContent?.trim() || '',
          artist: artistEl?.textContent?.trim() || '',
        });
      }
    }

    // Fallback: parse from audio link hrefs within container
    if (!tracks.length) {
      const links = container.querySelectorAll('a[href^="/audio"]');
      for (const link of links) {
        const m = link.href.match(/\/audio(-?\d+)_(\d+)/);
        if (!m) continue;
        const fullId = `${m[1]}_${m[2]}`;
        if (seen.has(fullId)) continue;
        seen.add(fullId);
        tracks.push({
          id: m[2],
          owner_id: m[1],
          fullId,
          title: link.textContent?.trim() || '',
          artist: '',
        });
      }
    }

    return tracks;
  }

  // Expand the playlist popup: click "Показать все" to load the first batch,
  // then scroll the page-level scroll container (VK lazy-loads more tracks as
  // its IntersectionObserver hits the bottom of the popup). Harvests every row
  // via fiber stamps + DOM fallback. Loops until no new tracks appear for a
  // few iterations. Works for any playlist size (verified on 71-track sample).
  // Try to read the playlist's declared track count out of the popup header.
  // VK renders something like "1000 треков" / "1 трек" / "23 записи" near the
  // title; pull the first such number we find. Returns null if not visible.
  function getPlaylistTotalFromModal(modal) {
    if (!modal) return null;
    const text = (modal.textContent || '').slice(0, 4000);
    const m = text.match(/(\d[\d\s ]{0,6})\s*(?:трек(?:а|ов)?|записе?[йяи]|композици[йия])/i);
    if (!m) return null;
    const n = parseInt(m[1].replace(/[\s ]/g, ''), 10);
    return Number.isFinite(n) && n > 0 ? n : null;
  }

  async function expandPlaylistModal(onProgress) {
    const modal = [...document.querySelectorAll('[class*="vkitInternalModalBox"]')]
      .find(m => m.getBoundingClientRect().width > 0);
    if (!modal) return [];

    const declaredTotal = getPlaylistTotalFromModal(modal);

    const showAll = modal.querySelector('[class*="showAll"], [class*="ShowAll"]')
      || [...modal.querySelectorAll('a, button, [role="button"], div, span')].find(el => {
        const t = (el.textContent || '').trim().toLowerCase();
        return t === 'показать все' || t === 'показать всё' || t === 'show all';
      });
    if (showAll) {
      showAll.click();
      await sleep(900);
    }

    // VK virtualizes the playlist popup against the page-level scroller, not
    // a container inside the modal. Find the first ancestor (or any element)
    // that's actually scrollable — that's where scrolling triggers lazy load.
    const findScroller = () => [...document.querySelectorAll('*')].find(el => {
      const cs = getComputedStyle(el);
      return (cs.overflowY === 'auto' || cs.overflowY === 'scroll') && el.scrollHeight > el.clientHeight + 10;
    }) || document.scrollingElement;

    const collected = new Map();

    async function harvest() {
      window.postMessage({ type: 'VKD_MARK_ROWS' }, '*');
      await sleep(80);
      const rows = modal.querySelectorAll('[class*="vkitAudioRow__root"], .audio_row, [data-full-id]');
      for (const r of rows) {
        let id = null, title = '', artist = '', fullId = null, isBlocked = false;
        if (r.dataset && r.dataset.vmuTrack) {
          try {
            const t = JSON.parse(r.dataset.vmuTrack);
            id = t.id; fullId = t.id; title = t.title || ''; artist = t.artist || '';
            isBlocked = !!t.isBlocked;
          } catch {}
        }
        if (!id && r.dataset && r.dataset.fullId) {
          id = r.dataset.fullId; fullId = id;
          title = (r.querySelector('.audio_title, .ai_title, [class*="title"]')?.textContent || '').trim();
          artist = (r.querySelector('.audio_artist, .ai_artist, [class*="performers"]')?.textContent || '').trim();
        }
        if (id && !collected.has(id)) collected.set(id, { id, fullId, title, artist, isBlocked });
      }
    }

    // Snapshot the scroll position before we start jumping to the bottom to
    // trigger lazy-loading, so we can put the user back where they were.
    const initialScroller = findScroller();
    const initialScrollTop = initialScroller ? initialScroller.scrollTop : 0;

    await harvest();
    const reportProgress = () => {
      if (!onProgress) return;
      try { onProgress(collected.size, declaredTotal); } catch {}
    };
    reportProgress();
    let stable = 0, lastSize = collected.size;
    const MAX_ITER = 60;
    for (let i = 0; i < MAX_ITER && stable < 4; i++) {
      const sc = findScroller();
      sc.scrollTop = sc.scrollHeight;
      await sleep(500);
      await harvest();
      reportProgress();
      if (collected.size === lastSize) stable++; else { stable = 0; lastSize = collected.size; }
    }

    // Restore the original scroll position. Rows stay mounted (no
    // virtualization here once VK has loaded them), so the user lands back
    // where they started with the full playlist already in the DOM.
    const sc = findScroller();
    if (sc) sc.scrollTop = initialScrollTop;

    return [...collected.values()];
  }

  async function scanForDuplicates(plInfoArg, statusCallback) {
    const pl = plInfoArg || getPlaylistInfoFromUrl();
    const report = statusCallback || ((msg, isError, progress) => setPlaylistStatus(msg, isError, progress));

    if (!pl) {
      report('Перейдите на страницу плейлиста для поиска дубликатов', true);
      return;
    }

    clearDupeMarkers();
    report('Раскрываем плейлист…');

    try {
      let tracks = await expandPlaylistModal((loaded, total) => {
        if (total) report(`Раскрываем плейлист… ${loaded} / ${total}`, false, { loaded, total });
        else report(`Раскрываем плейлист… ${loaded}`);
      });
      if (!tracks.length) tracks = getTracksFromDOM(pl);

      if (!tracks.length) {
        report('Загружаем через API…');
        try {
          const result = await pageCall('VK_LOAD_PLAYLIST', 'VK_PLAYLIST_LOADED', {
            ownerId: pl.ownerId,
            playlistId: pl.playlistId,
            offset: 0,
          }, 10000);
          try {
            const raw = JSON.parse(result.raw || '{}');
            const list = raw?.payload?.[1];
            if (Array.isArray(list)) tracks = parseTracksFromPayload(list);
          } catch {}
        } catch (apiErr) {
          report(`Не удалось загрузить треки: ${apiErr.message}`, true);
          return;
        }
      }

      if (!tracks.length) {
        report('Треки не найдены', true);
        return;
      }

      const seen = new Map();
      const dupes = [];
      const dupeIndexSet = new Set();
      const groupsByKey = new Map();
      for (let i = 0; i < tracks.length; i++) {
        const track = tracks[i];
        const key = `${track.artist}|||${track.title}`.toLowerCase().trim();
        if (!key.includes('|||') || (!track.artist && !track.title)) continue;
        if (seen.has(key)) {
          const orig = seen.get(key);
          dupes.push({ track, original: orig.track });
          dupeIndexSet.add(i);
          dupeIndexSet.add(orig.index);
          let g = groupsByKey.get(key);
          if (!g) {
            const label = `${orig.track.artist || ''}${orig.track.artist ? ' — ' : ''}${orig.track.title || ''}`.trim()
              || `Трек ${orig.index + 1}`;
            g = { label, indices: [orig.index] };
            groupsByKey.set(key, g);
          }
          g.indices.push(i);
        } else {
          seen.set(key, { track, index: i });
        }
      }
      const dupeGroups = [...groupsByKey.values()].sort((a, b) => a.indices[0] - b.indices[0]);

      const blockedIndices = [];
      for (let i = 0; i < tracks.length; i++) if (tracks[i].isBlocked) blockedIndices.push(i);

      if (!dupes.length && !blockedIndices.length) {
        report(`В ${tracks.length} треках всё чисто`);
        return;
      }

      const msgParts = [];
      if (dupes.length) msgParts.push(`Дубликатов: ${dupes.length}`);
      if (blockedIndices.length) msgParts.push(`Недоступных: ${blockedIndices.length}`);
      report(`${msgParts.join(' · ')} из ${tracks.length}`);

      const dupeIndices = [...dupeIndexSet].sort((a, b) => a - b);
      if (dupes.length) highlightDuplicateTracks(dupes);
      if (blockedIndices.length) highlightBlockedTracks(tracks, blockedIndices);
      buildIssuePanel(tracks, dupeIndices, blockedIndices, dupeGroups);
    } catch (err) {
      report(`Ошибка: ${err.message}`, true);
    }
  }

  // Highlight every row whose track participates in a duplicate set (both the
  // original first occurrence and the dupes), so users can spot the pairs
  // directly in the playlist popup. Replaces the old delete-dialog flow.
  function highlightDuplicateTracks(dupes) {
    document.querySelectorAll('.vmu-dupe-highlight').forEach(el => el.classList.remove('vmu-dupe-highlight'));
    const ids = new Set();
    for (const d of dupes) {
      if (d.track?.fullId) ids.add(d.track.fullId);
      if (d.original?.fullId) ids.add(d.original.fullId);
      if (d.track?.id) ids.add(String(d.track.id));
      if (d.original?.id) ids.add(String(d.original.id));
    }
    const rows = document.querySelectorAll('[data-full-id], [data-vmu-track], [class*="vkitAudioRow__root"], .audio_row');
    for (const row of rows) {
      let rowId = row.dataset?.fullId || null;
      if (!rowId && row.dataset?.vmuTrack) {
        try { rowId = JSON.parse(row.dataset.vmuTrack).id; } catch {}
      }
      if (rowId && ids.has(rowId)) row.classList.add('vmu-dupe-highlight');
    }
  }

  // Mark blocked / VK-unavailable tracks with a red accent. Detected from
  // entity.data.isBlocked (or null url) — set by markRowTrackData in injected.js.
  function highlightBlockedTracks(tracks, blockedIndices) {
    const ids = new Set();
    for (const i of blockedIndices) {
      const t = tracks[i];
      if (t?.fullId) ids.add(t.fullId);
      if (t?.id) ids.add(String(t.id));
    }
    const rows = document.querySelectorAll('[data-full-id], [data-vmu-track], [class*="vkitAudioRow__root"], .audio_row');
    for (const row of rows) {
      let rowId = row.dataset?.fullId || null;
      if (!rowId && row.dataset?.vmuTrack) {
        try { rowId = JSON.parse(row.dataset.vmuTrack).id; } catch {}
      }
      if (rowId && ids.has(rowId)) row.classList.add('vmu-blocked-highlight');
    }
  }

  // Clear highlight rows and tear down the minimap (used at scan start, and
  // automatically when the playlist modal closes).
  function clearDupeMarkers() {
    document.querySelectorAll('.vmu-dupe-highlight, .vmu-blocked-highlight').forEach(el => {
      el.classList.remove('vmu-dupe-highlight');
      el.classList.remove('vmu-blocked-highlight');
    });
    const panel = document.getElementById('vmu-issue-panel');
    if (panel) {
      panel._vmuCleanup?.();
      panel.remove();
    }
  }

  // Helper: scroll a popup row into view and flash it. Used by both the
  // minimap markers and the blocked-list entries.
  function focusModalRowByTrackId(modal, trackId) {
    let row = null;
    try { row = modal.querySelector(`[data-full-id="${CSS.escape(String(trackId))}"]`); } catch {}
    if (!row) {
      row = [...modal.querySelectorAll('[data-vmu-track]')].find(r => {
        try { return JSON.parse(r.dataset.vmuTrack).id === trackId; } catch { return false; }
      }) || null;
    }
    if (!row) return;
    row.scrollIntoView({ behavior: 'smooth', block: 'center' });
    row.classList.remove('vmu-dupe-flash');
    void row.offsetWidth;
    row.classList.add('vmu-dupe-flash');
    setTimeout(() => row.classList.remove('vmu-dupe-flash'), 1400);
  }

  // Unified issue panel: small marker strip (dupes amber + blocked red) on
  // the left edge, optional scrollable list of blocked tracks on the right
  // with a single header (counts + copy-to-clipboard). Click any marker or
  // list row to jump to that track in the playlist popup.
  function buildIssuePanel(tracks, dupeIndices, blockedIndices, dupeGroups) {
    dupeGroups = dupeGroups || [];
    document.getElementById('vmu-issue-panel')?.remove();
    const modal = [...document.querySelectorAll('[class*="vkitInternalModalBox"]')]
      .find(m => m.getBoundingClientRect().width > 0);
    if (!modal || (!dupeIndices.length && !blockedIndices.length)) return;

    const hasListContent = blockedIndices.length > 0 || dupeGroups.length > 0;
    const panel = document.createElement('div');
    panel.id = 'vmu-issue-panel';
    panel.classList.toggle('vmu-ip-has-list', hasListContent);

    // ── marker column ────────────────────────────────────────────────────
    const mm = document.createElement('div');
    mm.className = 'vmu-ip-mm';
    // Show numeric badge only when there is no separate list to label things
    if (!blockedIndices.length && dupeIndices.length) {
      const badge = document.createElement('div');
      badge.className = 'vmu-ip-mm-badge';
      badge.textContent = String(dupeIndices.length);
      badge.title = `Дубликатов: ${dupeIndices.length}`;
      mm.appendChild(badge);
    }
    const inner = document.createElement('div');
    inner.className = 'vmu-ip-mm-inner';
    mm.appendChild(inner);

    const total = tracks.length;
    const blockedSet = new Set(blockedIndices);
    const all = [
      ...blockedIndices.map(idx => ({ idx, blocked: true })),
      ...dupeIndices.filter(idx => !blockedSet.has(idx)).map(idx => ({ idx, blocked: false })),
    ];
    for (const { idx, blocked } of all) {
      const t = tracks[idx] || {};
      const mark = document.createElement('button');
      mark.type = 'button';
      mark.className = 'vmu-dupe-marker' + (blocked ? ' vmu-dupe-marker-blocked' : '');
      // Place each marker at the centre of its track-slot so the first and last
      // ones stay inside the column instead of being half-clipped at the edges.
      mark.style.top = `${((idx + 0.5) / Math.max(1, total)) * 100}%`;
      const label = `${t.artist || ''}${t.artist ? ' — ' : ''}${t.title || ''}`.trim() || `Трек ${idx + 1}`;
      mark.title = (blocked ? '[недоступен] ' : '') + label;
      mark.addEventListener('click', e => {
        e.preventDefault(); e.stopPropagation();
        const trackId = t?.fullId || t?.id;
        if (trackId) focusModalRowByTrackId(modal, trackId);
      });
      inner.appendChild(mark);
    }
    panel.appendChild(mm);

    // ── list column (blocked + duplicate groups) ─────────────────────────
    if (hasListContent) {
      const right = document.createElement('div');
      right.className = 'vmu-ip-listcol';

      const stats = [];
      if (blockedIndices.length) stats.push(`Недоступно: ${blockedIndices.length}`);
      if (dupeIndices.length) stats.push(`Дубликаты: ${dupeIndices.length}`);
      const head = document.createElement('div');
      head.className = 'vmu-bl-head';
      head.innerHTML = `<span class="vmu-bl-title">${stats.join(' · ')}</span>
        <button class="vmu-bl-copy" type="button" title="Скопировать недоступные">
          <svg width="14" height="14" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="7" y="3" width="10" height="12" rx="2"/><path d="M3 7a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-2"/></svg>
          <span class="vmu-bl-copy-label">Копировать</span>
        </button>`;
      right.appendChild(head);

      const list = document.createElement('div');
      list.className = 'vmu-bl-list';

      const blockedLines = [];
      if (blockedIndices.length) {
        const sectionHead = document.createElement('div');
        sectionHead.className = 'vmu-bl-section-head vmu-bl-section-blocked';
        sectionHead.textContent = `Недоступные · ${blockedIndices.length}`;
        list.appendChild(sectionHead);
        for (const idx of blockedIndices) {
          const t = tracks[idx] || {};
          const label = `${t.artist || ''}${t.artist ? ' — ' : ''}${t.title || ''}`.trim() || `Трек ${idx + 1}`;
          blockedLines.push(label);
          const row = document.createElement('button');
          row.type = 'button';
          row.className = 'vmu-bl-item';
          row.title = label;
          const rowText = document.createElement('span');
          rowText.className = 'vmu-bl-item-text';
          rowText.textContent = label;
          row.appendChild(rowText);
          row.addEventListener('click', e => {
            e.preventDefault(); e.stopPropagation();
            const trackId = t?.fullId || t?.id;
            if (trackId) focusModalRowByTrackId(modal, trackId);
          });
          list.appendChild(row);
        }
      }

      if (dupeGroups.length) {
        const sectionHead = document.createElement('div');
        sectionHead.className = 'vmu-bl-section-head vmu-bl-section-dupes';
        sectionHead.textContent = `Дубликаты · ${dupeGroups.length} ${dupeGroups.length === 1 ? 'группа' : 'групп'}`;
        list.appendChild(sectionHead);
        for (const g of dupeGroups) {
          const group = document.createElement('div');
          group.className = 'vmu-bl-group';

          const title = document.createElement('div');
          title.className = 'vmu-bl-group-title';
          title.title = g.label;
          const nameSpan = document.createElement('span');
          nameSpan.className = 'vmu-bl-group-name';
          nameSpan.textContent = g.label;
          const countSpan = document.createElement('span');
          countSpan.className = 'vmu-bl-group-count';
          countSpan.textContent = '×' + g.indices.length;
          title.append(nameSpan, countSpan);
          group.appendChild(title);

          const positions = document.createElement('div');
          positions.className = 'vmu-bl-group-positions';
          for (const idx of g.indices) {
            const t = tracks[idx] || {};
            const pos = document.createElement('button');
            pos.type = 'button';
            pos.className = 'vmu-bl-pos';
            pos.textContent = `#${idx + 1}`;
            pos.title = g.label;
            pos.addEventListener('click', e => {
              e.preventDefault(); e.stopPropagation();
              const trackId = t?.fullId || t?.id;
              if (trackId) focusModalRowByTrackId(modal, trackId);
            });
            positions.appendChild(pos);
          }
          group.appendChild(positions);
          list.appendChild(group);
        }
      }

      right.appendChild(list);

      const copyBtn = head.querySelector('.vmu-bl-copy');
      const copyText = blockedLines.length
        ? blockedLines.join('\n')
        : dupeGroups.map(g => `${g.label} (×${g.indices.length}: ${g.indices.map(i => '#' + (i + 1)).join(', ')})`).join('\n');
      if (!blockedLines.length) copyBtn.title = 'Скопировать дубликаты';
      copyBtn.addEventListener('click', e => {
        e.preventDefault(); e.stopPropagation();
        const labelEl = copyBtn.querySelector('.vmu-bl-copy-label');
        const orig = labelEl?.textContent || 'Копировать';
        navigator.clipboard.writeText(copyText).then(() => {
          if (labelEl) labelEl.textContent = 'Скопировано';
          copyBtn.classList.add('vmu-bl-copy-ok');
          setTimeout(() => {
            if (!copyBtn.isConnected) return;
            if (labelEl) labelEl.textContent = orig;
            copyBtn.classList.remove('vmu-bl-copy-ok');
          }, 1500);
        }).catch(() => {
          if (labelEl) labelEl.textContent = 'Ошибка';
          setTimeout(() => { if (copyBtn.isConnected && labelEl) labelEl.textContent = orig; }, 1500);
        });
      });
      panel.appendChild(right);
    }

    document.body.appendChild(panel);

    const positionPanel = () => {
      const r = modal.getBoundingClientRect();
      if (r.width === 0) return;
      panel.style.left = (r.right + 6) + 'px';
      const top = Math.max(60, Math.min(r.top + 220, window.innerHeight - 320));
      panel.style.top = top + 'px';
    };
    positionPanel();
    const ro = new ResizeObserver(positionPanel);
    ro.observe(modal);
    window.addEventListener('resize', positionPanel);
    window.addEventListener('scroll', positionPanel, true);
    const mo = new MutationObserver(() => {
      if (!document.body.contains(modal) || modal.getBoundingClientRect().width === 0) {
        panel._vmuCleanup?.();
        panel.remove();
      }
    });
    mo.observe(document.body, { childList: true, subtree: true });
    panel._vmuCleanup = () => {
      ro.disconnect();
      mo.disconnect();
      window.removeEventListener('resize', positionPanel);
      window.removeEventListener('scroll', positionPanel, true);
    };
  }

  function parseTracksFromPayload(list) {
    const tracks = [];
    for (const item of list) {
      if (Array.isArray(item) && item.length >= 5) {
        // VK internal format: [id, owner_id, url, url2, title, artist, ...]
        tracks.push({
          id: String(item[0]),
          owner_id: String(item[1]),
          fullId: `${item[1]}_${item[0]}`,
          title: String(item[3] || item[4] || ''),
          artist: String(item[4] || item[5] || ''),
        });
      }
    }
    return tracks;
  }

  // ─── settings panel ───────────────────────────────────────────────────────────
  let settingsPanelOpen = false;

  function toggleSettings() {
    settingsPanelOpen = !settingsPanelOpen;
    const panel = document.getElementById('vmu-settings-panel');
    const btn = document.getElementById('vmu-settings-btn');
    if (panel) panel.style.display = settingsPanelOpen ? 'block' : 'none';
    if (btn) btn.style.color = settingsPanelOpen ? '#2688eb' : '';
  }

  function buildSettingsPanel() {
    const hasCover = !!settings.coverDataUrl;
    return `
      <div id="vmu-settings-panel" style="display:none">
        <div class="vmu-settings-section">
          <div class="vmu-setting-row">
            <div class="vmu-setting-info">
              <span class="vmu-setting-label">Авто-плейлист</span>
              <span class="vmu-setting-hint">Создать плейлист после загрузки</span>
            </div>
            <label class="vmu-toggle">
              <input type="checkbox" id="vmu-ap-toggle" ${settings.autoPlaylist ? 'checked' : ''}>
              <span class="vmu-toggle-track"></span>
            </label>
          </div>

          <div id="vmu-cover-row" class="vmu-setting-row ${settings.autoPlaylist ? '' : 'vmu-row-disabled'}">
            <div class="vmu-setting-info">
              <span class="vmu-setting-label">Обложка</span>
              <span class="vmu-setting-hint">Выберите базовый JPG/PNG (1000×1000)</span>
            </div>
            <div style="display:flex;align-items:center;gap:8px;flex-shrink:0">
              ${hasCover ? `<div id="vmu-cover-preview" title="Нажмите для смены"></div>` : ''}
              <label class="vmu-cover-pick-btn" title="Выбрать обложку">
                ${hasCover ? '↺' : '+ Выбрать'}
                <input type="file" id="vmu-cover-input" accept="image/*" style="display:none">
              </label>
              ${hasCover ? `<button id="vmu-cover-clear" title="Удалить обложку">✕</button>` : ''}
            </div>
          </div>
        </div>

        <div class="vmu-settings-section">
          <div class="vmu-setting-row">
            <div class="vmu-setting-info">
              <span class="vmu-setting-label">Авто-метаданные</span>
              <span class="vmu-setting-hint">Заполнить исполнителя и название из имени файла, если теги пусты</span>
            </div>
            <label class="vmu-toggle">
              <input type="checkbox" id="vmu-meta-toggle" ${settings.autoMeta ? 'checked' : ''}>
              <span class="vmu-toggle-track"></span>
            </label>
          </div>

          <div class="vmu-setting-row ${settings.autoPlaylist && !settings.coverDataUrl ? '' : 'vmu-row-disabled'}" id="vmu-id3cover-row">
            <div class="vmu-setting-info">
              <span class="vmu-setting-label">Обложка из ID3</span>
              <span class="vmu-setting-hint">Использовать встроенную обложку первого трека (если нет выбранной выше)</span>
            </div>
            <label class="vmu-toggle">
              <input type="checkbox" id="vmu-id3cover-toggle" ${settings.autoCoverFromId3 ? 'checked' : ''}>
              <span class="vmu-toggle-track"></span>
            </label>
          </div>
        </div>

        <div id="vmu-pl-status" style="display:none">
          <div class="vmu-pl-status-text"></div>
          <div class="vmu-pl-progress"><div class="vmu-pl-progress-bar"></div></div>
        </div>
      </div>`;
  }

  function attachSettingsHandlers() {
    const toggle = document.getElementById('vmu-ap-toggle');
    if (toggle) {
      toggle.addEventListener('change', () => {
        settings.autoPlaylist = toggle.checked;
        saveSettings();
        const coverRow = document.getElementById('vmu-cover-row');
        if (coverRow) coverRow.classList.toggle('vmu-row-disabled', !settings.autoPlaylist);
        const id3Row = document.getElementById('vmu-id3cover-row');
        if (id3Row) id3Row.classList.toggle('vmu-row-disabled', !(settings.autoPlaylist && !settings.coverDataUrl));
      });
    }

    const id3CoverToggle = document.getElementById('vmu-id3cover-toggle');
    if (id3CoverToggle) {
      id3CoverToggle.addEventListener('change', () => {
        settings.autoCoverFromId3 = id3CoverToggle.checked;
        saveSettings();
      });
    }

    const coverInput = document.getElementById('vmu-cover-input');
    if (coverInput) {
      coverInput.addEventListener('change', async (e) => {
        const file = e.target.files[0];
        if (!file) return;
        const reader = new FileReader();
        reader.onload = () => {
          settings.coverDataUrl = reader.result;
          saveSettings();
          // Rebuild settings panel to show preview
          const panel = document.getElementById('vmu-settings-panel');
          if (panel) {
            const wasOpen = settingsPanelOpen;
            rebuildSettingsPanel();
            if (wasOpen) document.getElementById('vmu-settings-panel').style.display = 'block';
          }
        };
        reader.readAsDataURL(file);
        e.target.value = '';
      });
    }

    const coverClear = document.getElementById('vmu-cover-clear');
    if (coverClear) {
      coverClear.addEventListener('click', () => {
        settings.coverDataUrl = null;
        saveSettings();
        rebuildSettingsPanel();
        if (settingsPanelOpen) document.getElementById('vmu-settings-panel').style.display = 'block';
      });
    }

    const coverPreview = document.getElementById('vmu-cover-preview');
    if (coverPreview && settings.coverDataUrl) {
      coverPreview.style.cssText = `width:32px;height:32px;border-radius:4px;background:url('${settings.coverDataUrl}') center/cover;cursor:pointer;flex-shrink:0`;
    }

    const metaToggle = document.getElementById('vmu-meta-toggle');
    if (metaToggle) {
      metaToggle.addEventListener('change', () => {
        settings.autoMeta = metaToggle.checked;
        saveSettings();
      });
    }

  }

  function rebuildSettingsPanel() {
    const panel = document.getElementById('vmu-settings-panel');
    if (!panel) return;
    const newPanel = document.createElement('div');
    newPanel.innerHTML = buildSettingsPanel();
    panel.replaceWith(newPanel.firstElementChild);
    attachSettingsHandlers();
  }

  // ─── retry / copy helpers ──────────────────────────────────────────────────
  function retryOne(idx) {
    if (fileQueue[idx] && fileQueue[idx].status === 'error') {
      fileQueue[idx].status = 'pending';
      fileQueue[idx].errorMsg = null;
      renderQueue();
      if (!isProcessing) processQueue();
    }
  }

  function retryAll() {
    let any = false;
    fileQueue.forEach(item => {
      if (item.status === 'error') { item.status = 'pending'; item.errorMsg = null; any = true; }
    });
    if (any) { renderQueue(); if (!isProcessing) processQueue(); }
  }

  // Cancel an item: if it's uploading, abort the XHR (status flips to
  // pending again via the catch in processQueue once the abort comes back);
  // if it's queued/done/error, just drop it from the queue.
  function cancelOne(idx) {
    const item = fileQueue[idx];
    if (!item) return;
    if (item.status === 'uploading') {
      item.abortReason = 'cancel';
      window.postMessage({ type: 'VMU_CANCEL_UPLOAD' }, '*');
    } else {
      fileQueue.splice(idx, 1);
      renderQueue();
    }
  }

  // Pause / resume the queue. If a track is uploading, abort it so the user
  // sees the pause take effect immediately — it'll get re-uploaded from
  // scratch on resume (VK upload protocol doesn't support resume).
  function togglePause() {
    isPaused = !isPaused;
    if (isPaused) {
      const upl = fileQueue.find(i => i.status === 'uploading');
      if (upl) {
        upl.abortReason = 'pause';
        window.postMessage({ type: 'VMU_CANCEL_UPLOAD' }, '*');
      }
    } else if (!isProcessing) {
      processQueue();
    }
    renderQueue();
  }

  // Update only the progress bar of one item, without touching the rest of
  // the DOM (called many times per second during upload).
  function updateRowProgress(itemId) {
    const item = fileQueue.find(i => i.id === itemId);
    if (!item) return;
    const list = document.getElementById('vmu-list');
    const bar = list?.querySelector(`.vmu-progress[data-id="${itemId}"]`);
    if (bar) bar.style.width = `${Math.round((item.progress || 0) * 100)}%`;
  }

  function copyFailed() {
    const names = fileQueue.filter(f => f.status === 'error').map(f => f.file.name);
    if (!names.length) return;
    navigator.clipboard.writeText(names.join('\n')).then(() => {
      const btn = document.getElementById('vmu-copy-failed');
      if (btn) {
        const orig = btn.textContent;
        btn.textContent = '✓ Скопировано';
        setTimeout(() => { if (btn.isConnected) btn.textContent = orig; }, 1500);
      }
    }).catch(() => {});
  }

  // ─── render ───────────────────────────────────────────────────────────────────
  function renderQueue() {
    const list = document.getElementById('vmu-list');
    if (!list) return;

    list.innerHTML = fileQueue.map((item, idx) => {
      const raw = item.file.name.replace(/\.mp3$/i, '');
      const name = raw.length > 48 ? raw.slice(0, 46) + '…' : raw;
      const errHtml = item.errorMsg ? `<span class="vmu-errmsg">${item.errorMsg}</span>` : '';
      const retryBtn = item.status === 'error' ? `<button class="vmu-retry-btn" data-idx="${idx}" title="Повторить">↺</button>` : '';
      const cancelTitle = item.status === 'uploading' ? 'Отменить загрузку' : 'Убрать из очереди';
      const cancelBtn = `<button class="vmu-cancel-btn" data-idx="${idx}" title="${cancelTitle}">✕</button>`;
      const progressBar = item.status === 'uploading'
        ? `<div class="vmu-progress" data-id="${item.id}" style="width:${Math.round((item.progress || 0) * 100)}%"></div>`
        : '';
      return `<div class="vmu-item vmu-${item.status}">
        <span class="vmu-icon">${STATUS_ICON[item.status]}</span>
        <span class="vmu-info">
          <span class="vmu-name" title="${item.file.name}">${name}</span>${errHtml}
        </span>
        <span class="vmu-sz">${fmtSize(item.file.size)}</span>
        ${retryBtn}
        ${cancelBtn}
        ${progressBar}
      </div>`;
    }).join('');

    const counts = { pending: 0, uploading: 0, done: 0, error: 0 };
    fileQueue.forEach(f => counts[f.status]++);

    let txt = '';
    if (isPaused && (counts.pending || counts.uploading)) txt = `На паузе · в очереди: ${counts.pending + counts.uploading}`;
    else if (counts.uploading)                txt = 'Загружается…';
    else if (counts.pending)             txt = `В очереди: ${counts.pending}`;
    else if (counts.error && counts.done) txt = `Загружено: ${counts.done}, ошибок: ${counts.error}`;
    else if (counts.done)                txt = `Все треки загружены: ${counts.done}`;
    else if (counts.error)               txt = `Ошибок: ${counts.error}`;

    const st = document.getElementById('vmu-status');
    if (st) st.textContent = txt;

    const hasWork = counts.pending > 0 || counts.uploading > 0;
    const allSettled = fileQueue.length > 0 && !hasWork;
    const buttons = [];
    if (hasWork) {
      const pauseIcon = isPaused
        ? `<svg width="14" height="14" viewBox="0 0 14 14" fill="currentColor"><path d="M4 2 L11 7 L4 12 Z"/></svg>`
        : `<svg width="14" height="14" viewBox="0 0 14 14" fill="currentColor"><rect x="3" y="2" width="3" height="10" rx="0.8"/><rect x="8" y="2" width="3" height="10" rx="0.8"/></svg>`;
      buttons.push(`<button class="vmu-action-btn vmu-pause-btn ${isPaused ? 'vmu-resume' : ''}" id="vmu-pause-btn" data-vmu-tip="${isPaused ? 'Продолжить' : 'Пауза'}">${pauseIcon}</button>`);
    }
    if (allSettled && counts.error > 0) {
      buttons.push(`<button class="vmu-action-btn" id="vmu-copy-failed" data-vmu-tip="Скопировать имена ошибок">
          <svg width="14" height="14" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><rect x="7" y="3" width="10" height="12" rx="2"/><path d="M3 7a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-2"/></svg>
        </button>`);
      buttons.push(`<button class="vmu-action-btn" id="vmu-retry-all" data-vmu-tip="Повторить все">
          <svg width="14" height="14" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.7" stroke-linecap="round" stroke-linejoin="round"><path d="M17 5v5h-5"/><path d="M16.7 14a7 7 0 1 1-1.4-7.4L17 8"/></svg>
        </button>`);
    }
    renderUploadSidePanel(buttons.join(''));
  }

  // ─── queue ────────────────────────────────────────────────────────────────────
  function addFiles(files) {
    if (!files.length) return;
    autoPlaylistRunning = false;
    files.forEach(f => {
      const item = { id: ++itemIdCounter, file: f, status: 'pending', errorMsg: null, tags: {}, progress: 0 };
      fileQueue.push(item);
      // Read ID3 tags asynchronously (non-blocking)
      readID3(f).then(tags => { item.tags = tags; }).catch(() => {});
    });
    renderQueue();
    if (!isProcessing) processQueue();
  }

  // ─── helpers to find VK's upload dialog (new and old VK) ────────────────────
  function getUploadDialog() {
    // New VK: vkitInternalModalBox containing audio file input
    const newDlg = [...document.querySelectorAll('[class*="vkitInternalModalBox"]')]
      .find(m => m.getBoundingClientRect().width > 0 &&
                 (m.querySelector('input[accept*="audio"], input[accept*="mp3"]') ||
                  m.querySelector('[data-testid="UploadAudio_SelectFileButton"]')));
    if (newDlg) return newDlg;
    // Old VK fallback
    return document.querySelector('.audio_add_box') || null;
  }
  function getUploadDialogBody(box) {
    return box?.querySelector('[class*="vkitModalBody__container"]') || box;
  }

  // ─── global drag & drop interceptor ──────────────────────────────────────────
  // VK's own upload dialog (.audio_add_box) overlays an invisible native drop
  // target on top of our embedded panel, so e.target during drop is VK's element,
  // not ours — `embedded.contains(e.target)` never matches. VK's handler then
  // grabs the dropped files into its native uploader (via the preserved vkInput
  // listeners), so tracks upload silently without ever reaching our queue/UI.
  // The actual interception (window-capture + preventDefault/stopImmediatePropagation,
  // matched by drop COORDINATES against our panel's bounding box) lives in
  // injector_early.js, which runs at document_start — before VK's own bundle
  // attaches its window-level drag/drop listeners, so ours fire first regardless
  // of same-node listener order. It forwards matches here via a custom event.
  window.addEventListener('vmu-files-dropped', e => {
    addFiles([...e.detail.files].filter(isMP3));
  });

  function setBlockAudioHide(block) {
    window.postMessage({ type: 'VMU_BLOCK_AUDIO_HIDE', block }, '*');
  }

  async function processQueue() {
    if (isProcessing) return;
    if (isPaused) return;
    const next = fileQueue.find(f => f.status === 'pending');
    console.log('[VMU QUEUE] processQueue: pending=', fileQueue.filter(f => f.status === 'pending').length,
      'uploading=', fileQueue.filter(f => f.status === 'uploading').length,
      'done=', fileQueue.filter(f => f.status === 'done').length,
      'error=', fileQueue.filter(f => f.status === 'error').length,
      'next=', next?.file?.name);
    if (!next) {
      setBlockAudioHide(false);
      renderQueue();
      if (fileQueue.length > 0 && !fileQueue.some(f => f.status === 'uploading')) {
        // Trigger auto-playlist if enabled (once per completed batch)
        if (settings.autoPlaylist && !autoPlaylistRunning) {
          autoPlaylistRunning = true;
          if (!settingsPanelOpen) toggleSettings();
          runAutoPlaylist([...fileQueue]).finally(() => { autoPlaylistRunning = false; });
        }
      }
      return;
    }

    setBlockAudioHide(true);
    isProcessing = true;
    next.status = 'uploading';
    next.progress = 0;
    renderQueue();

    try {
      await uploadOne(next);
      next.status = 'done';
      next.progress = 1;
    } catch (err) {
      if (err.message === '__ABORTED__') {
        const reason = next.abortReason || 'cancel';
        next.progress = 0;
        delete next.abortReason;
        if (reason === 'cancel') {
          const i = fileQueue.indexOf(next);
          if (i >= 0) fileQueue.splice(i, 1);
        } else {
          next.status = 'pending'; // paused — keep for resume
        }
      } else {
        next.status = 'error';
        next.errorMsg = err.message;
        console.warn('[VK Multi Upload]', err.message);
        // VK rate limit (code 8) — fail the rest of the batch immediately, every
        // subsequent file would just hit the same wall and produce noise.
        const code = err.vkCode;
        if (code === 8 || code === '8') {
          for (const f of fileQueue) {
            if (f.status === 'pending') { f.status = 'error'; f.errorMsg = err.message; }
          }
        }
      }
    }

    currentUploadingItem = null;
    renderQueue();
    isProcessing = false;
    // Drop the hold-open flag the moment there's nothing more pending, so
    // the user can immediately close the panel without waiting out the 2s
    // settle delay below.
    if (!fileQueue.some(f => f.status === 'pending' || f.status === 'uploading')) {
      setBlockAudioHide(false);
    }
    await sleep(2000);
    processQueue();
  }

  async function uploadOne(item) {
    let file = item.file;
    console.log('[VMU UPLOAD] uploadOne start:', file.name, 'size=', file.size);
    // Snapshot dialog state for diagnostics
    try {
      const box = getUploadDialog();
      const layer = box?.closest('#box_layer, .popup_box_container') || box;
      console.log('[VMU UPLOAD] dialog state', {
        boxOpen: !!box,
        boxStyle: box ? box.style.cssText.slice(0, 100) : null,
        boxClassExtras: box ? box.className : null,
        layerPointerEvents: layer ? getComputedStyle(layer).pointerEvents : null,
        layerOpacity: layer ? getComputedStyle(layer).opacity : null,
        layerDisplay: layer ? getComputedStyle(layer).display : null,
        layerCount: document.querySelectorAll('#box_layer').length,
      });
    } catch (err) { console.log('[VMU UPLOAD] dialog state read failed', err.message); }

    if (settings.autoMeta) {
      const tags = item.tags || {};
      const hasArtist = !!(tags.TPE1 || tags.TPE2);
      const hasTitle = !!tags.TIT2;
      if (!hasArtist || !hasTitle) {
        const parsed = parseMetaFromFilename(file.name);
        const artist = hasArtist ? (tags.TPE1 || tags.TPE2) : parsed.artist;
        const title = hasTitle ? tags.TIT2 : parsed.title;
        if (artist || title) {
          try { file = await patchID3(file, artist, title); } catch {}
        }
      }
    }

    currentUploadingItem = item;

    // Set the callback before injection to avoid losing a fast upload response.
    const uploadPromise = new Promise((resolve, reject) => {
      const t = setTimeout(() => {
        console.log('[VMU UPLOAD] 90s timeout —', file.name);
        uploadDoneCallback = null;
        reject(new Error('Timeout загрузки (90s)'));
      }, 90_000);
      uploadDoneCallback = data => {
        clearTimeout(t);
        console.log('[VMU UPLOAD] VK_UPLOAD_DONE received:', file.name, 'aborted=', !!data.aborted, 'error=', !!data.error, 'errorMsg=', data.errorMsg, 'code=', data.errorCode, 'resp=', (data.response || '').slice(0, 100));
        if (data.aborted) { reject(new Error('__ABORTED__')); return; }
        if (data.error) {
          const e = new Error(data.errorMsg || 'Ошибка сети');
          e.vkCode = data.errorCode;
          reject(e);
          return;
        }
        try {
          const r = JSON.parse(data.response);
          if (r.error_code) {
            console.log('[VMU UPLOAD] VK error_code=', r.error_code, r.error_msg);
            reject(new Error(`VK ${r.error_code}: ${r.error_msg}`));
          } else { resolve(r); }
        } catch { resolve(); }
      };
    });

    const buffer = await file.arrayBuffer();
    console.log('[VMU UPLOAD] sending VK_INJECT_FILE for', file.name);
    await new Promise((resolve, reject) => {
      const t = setTimeout(() => reject(new Error('Timeout инжекта')), 5000);
      const handler = e => {
        if (e.source !== window || e.data?.type !== 'VK_FILE_INJECTED') return;
        window.removeEventListener('message', handler);
        clearTimeout(t);
        console.log('[VMU UPLOAD] VK_FILE_INJECTED ok=', e.data.ok, 'err=', e.data.error);
        e.data.ok ? resolve() : reject(new Error(e.data.error));
      };
      window.addEventListener('message', handler);
      window.postMessage({ type: 'VK_INJECT_FILE', name: file.name, mimeType: file.type || 'audio/mpeg', buffer }, '*', [buffer]);
    });

    await uploadPromise;
    await sleep(2000);
  }

  // ─── Embed full UI into VK's native upload dialog ────────────────────────────
  function buildEmbeddedUI() {
    const wrap = document.createElement('div');
    wrap.id = 'vmu-embedded';
    wrap.innerHTML = `
      ${buildSettingsPanel()}

      <div id="vmu-dropzone">
        <div class="vmu-dz-label">Перетащите MP3 файлы сюда</div>
        <div class="vmu-dz-hint">не более 200 МБ каждый</div>
        <label class="vmu-pick-btn">
          ${ICON_UPLOAD}
          Выбрать файлы
          <input type="file" id="vmu-input" accept=".mp3,audio/mpeg" multiple>
        </label>
      </div>

      <div id="vmu-list"></div>

      <div id="vmu-footer">
        <span id="vmu-status"></span>
      </div>
    `;
    return wrap;
  }

  // Floating side panel rendered OUTSIDE the .audio_add_box popup, pinned to
  // its right edge. Same approach as the playlist dupe/blocked-tracks panel.
  // Contains icon-only action buttons (pause, copy-failed, retry-all). Auto-
  // disappears when there's nothing actionable.
  function renderUploadSidePanel(buttonsHtml) {
    if (!buttonsHtml) {
      const old = document.getElementById('vmu-upload-side');
      if (old) { old._vmuCleanup?.(); old.remove(); }
      return;
    }
    const box = getUploadDialog();
    const popup = box?.closest('.popup_box_container') || box;
    if (!popup) {
      const old = document.getElementById('vmu-upload-side');
      if (old) { old._vmuCleanup?.(); old.remove(); }
      return;
    }
    let panel = document.getElementById('vmu-upload-side');
    if (!panel) {
      panel = document.createElement('div');
      panel.id = 'vmu-upload-side';
      document.body.appendChild(panel);

      const positionPanel = () => {
        const r = popup.getBoundingClientRect();
        if (r.width === 0) {
          panel._vmuCleanup?.();
          panel.remove();
          return;
        }
        panel.style.left = (r.right + 8) + 'px';
        // Vertically centered on the popup's right edge
        const ph = panel.offsetHeight || 0;
        panel.style.top = Math.max(8, r.top + (r.height - ph) / 2) + 'px';
      };
      positionPanel();
      // Re-center once the panel has its real height after the first render
      requestAnimationFrame(positionPanel);
      const ro = new ResizeObserver(positionPanel);
      ro.observe(popup);
      window.addEventListener('resize', positionPanel);
      window.addEventListener('scroll', positionPanel, true);
      const mo = new MutationObserver(() => {
        if (!document.body.contains(popup) || popup.getBoundingClientRect().width === 0) {
          panel._vmuCleanup?.();
          panel.remove();
        }
      });
      mo.observe(document.body, { childList: true, subtree: true });
      panel._vmuCleanup = () => {
        ro.disconnect();
        mo.disconnect();
        window.removeEventListener('resize', positionPanel);
        window.removeEventListener('scroll', positionPanel, true);
      };
      panel.addEventListener('click', e => {
        if (e.target.closest('#vmu-copy-failed')) copyFailed();
        else if (e.target.closest('#vmu-retry-all')) retryAll();
        else if (e.target.closest('#vmu-pause-btn')) togglePause();
      });
    }
    panel.innerHTML = buttonsHtml;
    // Re-center vertically against the popup after the content height changes
    const popup2 = getUploadDialog()?.closest('.popup_box_container') || getUploadDialog();
    if (popup2) {
      const r = popup2.getBoundingClientRect();
      const ph = panel.offsetHeight || 0;
      panel.style.top = Math.max(8, r.top + (r.height - ph) / 2) + 'px';
    }
  }

  // Place "Очистить" in the same row as VK's native "Выбрать из своих аудиозаписей"
  // link in the dialog footer (outside .audio_add_box, so it survives box.innerHTML reset)
  function tryInjectClearButton() {
    const pickLink = [...document.querySelectorAll('a, button')]
      .find(el => (el.textContent || '').trim() === 'Выбрать из своих аудиозаписей');
    if (!pickLink) return;

    const btn = document.createElement('button');
    btn.id = 'vmu-clear';
    btn.className = 'vmu-clear-native';
    btn.textContent = 'Очистить';
    btn.addEventListener('click', () => {
      fileQueue = fileQueue.filter(f => f.status === 'uploading' || f.status === 'pending');
      renderQueue();
    });
    pickLink.insertAdjacentElement('afterend', btn);
  }

  // Move the settings gear into VK's native dialog header, right after the title
  // "Выберите аудиозапись на вашем компьютере" (outside .audio_add_box, so it
  // survives box.innerHTML reset)
  function tryInjectSettingsIntoHeader() {
    if (document.getElementById('vmu-settings-btn')) return;

    const btn = document.createElement('button');
    btn.id = 'vmu-settings-btn';
    btn.className = 'vmu-settings-btn-header';
    btn.title = 'Настройки';
    btn.innerHTML = ICON_SETTINGS;
    btn.addEventListener('click', toggleSettings);

    // New VK: inject into the header's after slot (right side, visually aligns with ✕)
    const box = getUploadDialog();
    const afterSlot = box?.querySelector('[class*="vkitModalHeader__after"]');
    if (afterSlot) {
      afterSlot.appendChild(btn);
      return;
    }

    // Old VK fallback — inject into the title element
    const titleEl = [...document.querySelectorAll('div, span, h1, h2, h3, p')]
      .find(el => el.children.length === 0 && (el.textContent || '').trim() === 'Выберите аудиозапись на вашем компьютере');
    if (!titleEl) return;
    titleEl.style.display = 'flex';
    titleEl.style.alignItems = 'center';
    titleEl.appendChild(btn);
  }

  function injectIntoVkDialog(box) {
    if (box.dataset.vmuInjected) return;
    box.dataset.vmuInjected = '1';

    // Save VK's original file input (with its VK event listeners intact)
    const vkInput = box.querySelector('input[type="file"]');
    if (vkInput) {
      vkInput.setAttribute('data-vmu-vk', '1');
      vkInput.style.cssText = 'position:absolute;opacity:0;pointer-events:none;width:0;height:0;overflow:hidden;';
    }

    // In new VK inject into modal body only (keep header/close btn intact)
    const body = getUploadDialogBody(box);
    body.innerHTML = '';
    body.appendChild(buildEmbeddedUI());

    // Re-append VK's input so it stays in the DOM with its event listeners
    if (vkInput) body.appendChild(vkInput);

    attachEmbeddedHandlers();
    renderQueue();
  }

  function attachEmbeddedHandlers() {
    document.getElementById('vmu-input')?.addEventListener('change', e => {
      addFiles([...e.target.files].filter(isMP3));
      e.target.value = '';
    });

    document.getElementById('vmu-list')?.addEventListener('click', e => {
      const retryBtn = e.target.closest('.vmu-retry-btn');
      if (retryBtn) { retryOne(parseInt(retryBtn.dataset.idx, 10)); return; }
      const cancelBtn = e.target.closest('.vmu-cancel-btn');
      if (cancelBtn) { cancelOne(parseInt(cancelBtn.dataset.idx, 10)); return; }
    });

    // The side action buttons live in a floating panel outside the popup; its
    // click listener is wired up in renderUploadSidePanel().

    attachSettingsHandlers();
  }

  // ─── dupes button injection into playlists ────────────────────────────────────
  // ─── dupes button inside playlist dialog ─────────────────────────────────────
  let _pendingDupesPlaylist = null;

  // Single delegated listener — catches ANY click on ANY playlist card element
  document.addEventListener('click', e => {
    const card = e.target.closest('[class*="_audio_pl_"]');
    if (!card) return;
    const m = card.className.match(/_audio_pl_([-\d]+_\d+)/);
    if (!m) return;
    const parts = m[1].split('_');
    _pendingDupesPlaylist = { ownerId: parts[0], playlistId: parts[1] };
  }, true);

  function showToast(msg, isError) {
    document.getElementById('vmu-toast')?.remove();
    const el = document.createElement('div');
    el.id = 'vmu-toast';
    el.textContent = msg;
    el.style.cssText = `position:fixed;bottom:20px;left:20px;z-index:999999;background:${isError ? '#b71c1c' : '#1b5e20'};color:#fff;padding:10px 16px;border-radius:8px;font-size:13px;font-family:-apple-system,BlinkMacSystemFont,Roboto,"Helvetica Neue",sans-serif;box-shadow:0 4px 16px rgba(0,0,0,.5);max-width:340px;word-break:break-word;pointer-events:none;`;
    document.body.appendChild(el);
    setTimeout(() => el.remove(), 5000);
  }

  // Persistent progress toast — same slot as showToast but updates in place
  // and shows an optional progress bar. Pass kind: 'done' or 'error' to
  // auto-dismiss after 4s; 'progress' keeps it on screen until next update.
  function showProgressToast(title, opts) {
    const kind = opts?.kind || 'progress';
    const pct = typeof opts?.pct === 'number' ? Math.max(0, Math.min(100, opts.pct)) : null;
    const id = opts?.id || 'vmu-progress';
    let el = document.getElementById(id);
    if (!el) {
      // Clear any plain toast so they don't stack on top
      document.getElementById('vmu-toast')?.remove();
      el = document.createElement('div');
      el.id = id;
      el.style.cssText = `position:fixed;bottom:20px;left:20px;z-index:999999;color:#fff;padding:10px 14px 12px;border-radius:8px;font-size:13px;font-family:-apple-system,BlinkMacSystemFont,Roboto,"Helvetica Neue",sans-serif;box-shadow:0 4px 16px rgba(0,0,0,.5);min-width:240px;max-width:340px;word-break:break-word;pointer-events:none;transition:background .25s ease;`;
      el.innerHTML = `<div class="vmu-pt-row" style="display:flex;align-items:center;gap:8px;"><span class="vmu-pt-spin" style="width:12px;height:12px;border-radius:50%;border:2px solid rgba(255,255,255,.4);border-top-color:#fff;animation:vmu-spin 1s linear infinite;flex:0 0 auto;"></span><span class="vmu-pt-text" style="flex:1 1 auto;"></span></div><div class="vmu-pt-bar" style="margin-top:8px;height:4px;background:rgba(255,255,255,.18);border-radius:2px;overflow:hidden;display:none;"><div class="vmu-pt-fill" style="height:100%;width:0%;background:#fff;transition:width .18s ease;"></div></div>`;
      if (!document.getElementById('vmu-pt-style')) {
        const s = document.createElement('style');
        s.id = 'vmu-pt-style';
        s.textContent = '@keyframes vmu-spin{to{transform:rotate(360deg)}}';
        document.head.appendChild(s);
      }
      document.body.appendChild(el);
    }
    el.style.background = kind === 'error' ? '#b71c1c' : kind === 'done' ? '#1b5e20' : '#0d47a1';
    el.querySelector('.vmu-pt-text').textContent = title || '';
    const spin = el.querySelector('.vmu-pt-spin');
    spin.style.display = kind === 'progress' ? '' : 'none';
    const bar = el.querySelector('.vmu-pt-bar');
    const fill = el.querySelector('.vmu-pt-fill');
    if (pct !== null) { bar.style.display = ''; fill.style.width = pct + '%'; }
    else if (kind !== 'progress') bar.style.display = 'none';
    clearTimeout(el._vmuTimer);
    if (kind === 'done' || kind === 'error') {
      el._vmuTimer = setTimeout(() => el.remove(), 4000);
    }
  }

  // Per-trackId HLS progress callbacks registered by downloadSingleTrack
  const hlsProgressHandlers = new Map();

  function makeDupesBtn(plInfo) {
    const btn = document.createElement('button');
    btn.className = 'vmu-dupes-dialog-btn';
    btn.setAttribute('data-vmu-dupes-dialog', '1');
    btn.setAttribute('data-vmu-tip', 'Проверить на дубликаты');
    btn.innerHTML = `<svg width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><rect x="7" y="3" width="10" height="10" rx="2"/><path d="M3 7a2 2 0 0 0-2 2v8a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2v-2"/></svg>`;
    btn.addEventListener('click', e => {
      e.preventDefault();
      e.stopPropagation();
      btn.disabled = true;
      scanForDuplicates(plInfo, (msg, isError) => {
        showToast(msg, isError);
      }).finally(() => { btn.disabled = false; });
    });
    return btn;
  }

  // Two download buttons for the playlist edit dialog: one downloads each track
  // as a separate file, the other bundles them into a single ZIP.
  function makeDlDialogBtn(plInfo, mode) {
    const isZip = mode === 'zip';
    const btn = document.createElement('button');
    btn.className = 'vmu-dl-dialog-btn vmu-dl-dialog-btn-' + mode;
    btn.setAttribute('data-vmu-dl-dialog', mode);
    btn.setAttribute('data-vmu-tip', isZip ? 'Скачать ZIP' : 'Скачать треками');
    btn.innerHTML = isZip
      ? `<svg width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M3.5 6.5L5 4h10l1.5 2.5v9a1 1 0 0 1-1 1H4.5a1 1 0 0 1-1-1v-9z"/><path d="M3.5 7h13"/><path d="M10 10v4M8 12.2l2 2 2-2"/></svg>`
      : `<svg width="20" height="20" viewBox="0 0 20 20" fill="none" stroke="currentColor" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"><path d="M10 3v10M7 10l3 3 3-3"/><path d="M3 15h14"/></svg>`;
    btn.addEventListener('click', e => {
      e.preventDefault();
      e.stopPropagation();
      if (document.getElementById('vmu-dlp-strip')) return;
      startDlSession(plInfo, mode);
    });
    return btn;
  }

  function tryInjectDupesIntoEditDialog() {
    // Prefer URL-derived info — it may contain accessHash from popup URL (?z=audio_playlist...)
    const urlInfo = getPlaylistInfoFromUrl();
    const plInfo = urlInfo || _pendingDupesPlaylist;
    if (!plInfo) return;

    // Find the VISIBLE modal — React keeps multiple instances, only one has non-zero dimensions
    const modal = [...document.querySelectorAll('[class*="vkitInternalModalBox"]')]
      .find(m => m.getBoundingClientRect().width > 0);
    if (!modal) return;
    if (modal.querySelector('input[type="text"]')) return;
    if (modal.querySelector('[data-vmu-dupes-dialog]')) return;

    const listenBtn = [...modal.querySelectorAll('button')]
      .find(b => b.textContent.trim() === 'Слушать');
    if (!listenBtn) return;

    const btnGroup = listenBtn.closest('[class*="vkuiButtonGroup"]');
    if (!btnGroup) return;

    // Create a new row below the existing buttons
    const flexParent = btnGroup.parentElement?.parentElement;
    if (!flexParent) return;

    const newRow = document.createElement('div');
    newRow.style.cssText = 'display:flex;gap:8px;flex-basis:100%;';
    newRow.appendChild(makeDupesBtn(plInfo));
    newRow.appendChild(makeDlDialogBtn(plInfo, 'individual'));
    newRow.appendChild(makeDlDialogBtn(plInfo, 'zip'));
    flexParent.appendChild(newRow);
  }

  // ─── playlist download feature ────────────────────────────────────────────────


  // ─── Embedded download progress (no backdrop) ────────────────────────────────
  // The progress strip is injected into the visible playlist modal between its
  // header and body. If no modal is open, falls back to a fixed floating bar.
  // Per-track status is shown as a badge inside the row's right-side slot.
  let _dlpStrip = null;

  function startDlSession(plInfo, mode) {
    dlpInit(mode);
    runPlaylistDownload(plInfo, mode);
  }

  function getActiveModal() {
    return [...document.querySelectorAll('[class*="vkitInternalModalBox"]')]
      .find(m => m.getBoundingClientRect().width > 0) || null;
  }

  function dlpInit(mode) {
    dlpClose();
    const strip = document.createElement('div');
    strip.id = 'vmu-dlp-strip';
    strip.className = 'vmu-dlp-strip';
    strip.innerHTML = `
      <div class="vmu-dlp-row1">
        <span class="vmu-dlp-mode">${mode === 'zip' ? 'ZIP' : 'Треки'}</span>
        <span class="vmu-dlp-phase">Готовим скачивание…</span>
        <span class="vmu-dlp-counter"></span>
        <button class="vmu-dlp-stop" type="button">Остановить</button>
      </div>
      <div class="vmu-dlp-barwrap"><div class="vmu-dlp-bar" style="width:0%"></div></div>
      <div class="vmu-dlp-error" style="display:none"></div>`;
    const modal = getActiveModal();
    if (modal) {
      // Insert between header and body
      const header = modal.querySelector('[class*="vkitAudioListBoxHeader__root"]');
      const body = modal.querySelector('[class*="vkitModalBody__container"]');
      if (header && body && header.parentNode === body.parentNode) {
        body.parentNode.insertBefore(strip, body);
      } else {
        modal.appendChild(strip);
      }
      strip.classList.add('vmu-dlp-in-modal');
    } else {
      strip.classList.add('vmu-dlp-floating');
      document.body.appendChild(strip);
    }
    strip.querySelector('.vmu-dlp-stop').onclick = () => {
      dlCancelFlag = true;
      const b = strip.querySelector('.vmu-dlp-stop');
      if (b) { b.textContent = 'Останавливаем…'; b.disabled = true; }
    };
    _dlpStrip = strip;
  }

  function dlpClose() {
    document.getElementById('vmu-dlp-strip')?.remove();
    // Also clear any per-row badges
    document.querySelectorAll('.vmu-row-status').forEach(n => n.remove());
    _dlpStrip = null;
  }

  function dlSetPhase(text) {
    const el = _dlpStrip?.querySelector('.vmu-dlp-phase');
    if (el) el.textContent = text;
  }

  function dlSetProgress(done, total) {
    const bar = _dlpStrip?.querySelector('.vmu-dlp-bar');
    const cnt = _dlpStrip?.querySelector('.vmu-dlp-counter');
    if (bar) bar.style.width = total > 0 ? Math.round((done / total) * 100) + '%' : '0%';
    if (cnt) cnt.textContent = total > 0 ? `${done}/${total}` : '';
  }

  function dlSetError(msg) {
    const el = _dlpStrip?.querySelector('.vmu-dlp-error');
    if (!el) return;
    if (msg) { el.style.display = ''; el.textContent = msg; }
    else el.style.display = 'none';
  }

  function dlSetFinished(ok) {
    if (!_dlpStrip) return;
    _dlpStrip.classList.add(ok ? 'vmu-dlp-done' : 'vmu-dlp-error-state');
    const stop = _dlpStrip.querySelector('.vmu-dlp-stop');
    if (stop) { stop.textContent = 'Закрыть'; stop.disabled = false; stop.onclick = dlpClose; }
  }

  // Per-track row badges removed — progress is now communicated only through
  // the strip. Kept as no-ops so existing call sites in runPlaylistDownload
  // don't need to change.
  function dlAddRow() {}
  function dlUpdateRow() {}

  async function scrollToCollect() {
    let last = dlTracks.size, unchanged = 0;
    while (unchanged < 4 && !dlCancelFlag) {
      window.postMessage({ type: 'VKD_SCROLL_LOAD' }, '*');
      await sleep(750);
      const now = dlTracks.size;
      dlSetPhase(`Прокручиваем список… найдено ${now}`);
      unchanged = now === last ? unchanged + 1 : 0;
      last = now;
    }
  }

  function sendDlMsg(url, filename) {
    return new Promise(resolve => {
      try {
        chrome.runtime.sendMessage({ type: 'VKD_DOWNLOAD', url, filename }, res => resolve(res));
      } catch { resolve({ ok: false }); }
    });
  }

  function dlSanitize(name) {
    return name.replace(/[<>:"/\\|?*]/g, '').replace(/\s+/g, ' ').trim().slice(0, 200);
  }

  function dlPad(n, total) {
    return String(n).padStart(String(total).length, '0');
  }

  // ── Extract tracks from popup DOM (React fiber) ──────────────────────────
  function extractTracksFromPopupDOM() {
    const tracks = [];
    const seen = new Set();

    // New VK uses vkitAudioRow__root with CSS modules hash
    const modal = [...document.querySelectorAll('[class*="vkitInternalModalBox"]')]
      .find(m => m.getBoundingClientRect().width > 0);
    const container = modal || document;
    const rows = container.querySelectorAll('[class*="vkitAudioRow__root"], .AudioRow, [data-full-id]');

    for (const row of rows) {
      try {
        const fiberKey = Object.keys(row).find(k => k.startsWith('__reactFiber$') || k.startsWith('__reactInternalInstance$'));
        if (fiberKey) {
          const fiber = row[fiberKey];
          const props = fiber.memoizedProps || fiber.pendingProps;
          const entity = props?.track?.entity;
          if (entity) {
            const identity = entity.data?.identity;
            const ownerId = identity?.ownerId;
            const audioId = identity?.id;
            const trackId = (ownerId && audioId) ? `${ownerId}_${audioId}` : `dom_${tracks.length}`;
            if (seen.has(trackId)) continue;
            seen.add(trackId);

            const artistName = entity.authors?.main?.[0]?.name || entity.subtitle || '';
            tracks.push({
              id: trackId,
              title: entity.title || '',
              artist: artistName,
              url: entity.url || null,
              duration: entity.duration || 0,
            });
            continue;
          }
        }

        // Fallback for old VK: data-full-id + text content
        const fullId = row.dataset?.fullId;
        if (!fullId) continue;
        if (seen.has(fullId)) continue;
        seen.add(fullId);

        const titleEl = row.querySelector('[class*="title_inner"], .ai_title');
        const artistEl = row.querySelector('[class*="performers"], .ai_artist');
        tracks.push({
          id: fullId,
          title: (titleEl?.textContent || '').trim(),
          artist: (artistEl?.textContent || '').trim(),
          url: null,
          duration: 0,
        });
      } catch {}
    }
    return tracks;
  }

  function isGoodUrl(url) {
    return url && url.startsWith('http') && !url.includes('/a2/') && !url.includes('.m3u8');
  }

  async function resolveDirectUrls(tracks) {
    // Only try to resolve tracks that DON'T already have good direct URLs
    const needResolve = tracks.filter(t => !isGoodUrl(t.url));
    const ids = needResolve.map(t => t.id).filter(id => !id.startsWith('dom_'));
    if (!ids.length) { console.log('[vmu] all tracks already have direct URLs'); return; }
    console.log('[vmu] resolving', ids.length, 'tracks via reload_audio');

    try {
      const result = await pageCall('VKD_RELOAD_AUDIO', 'VKD_RELOAD_AUDIO_DONE', { ids }, 15000);
      if (result?.resolved) {
        for (const [trackId, url] of Object.entries(result.resolved)) {
          const t = dlTracks.get(trackId);
          // Only update if the track doesn't already have a good URL
          if (t && !isGoodUrl(t.url) && isGoodUrl(url)) {
            console.log('[vmu] resolved', trackId, '->', url.substring(0, 60));
            t.url = url;
          }
        }
      }
    } catch (e) {
      console.warn('[vmu] resolveDirectUrls failed:', e.message);
    }
  }

  // ─── Inline ZIP writer (store method, no compression) ────────────────────────
  // MP3/AAC don't compress meaningfully; store-only keeps the code small and the
  // builder fast. Each file entry is: local header + name + data; central dir
  // appended at the end, then End-Of-Central-Directory record.
  function vmuZipBuild(files) {
    const enc = new TextEncoder();
    if (!vmuZipBuild._crcTbl) {
      const t = new Uint32Array(256);
      for (let i = 0; i < 256; i++) {
        let c = i;
        for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
        t[i] = c >>> 0;
      }
      vmuZipBuild._crcTbl = t;
    }
    const T = vmuZipBuild._crcTbl;
    const crc32 = (buf) => {
      let c = 0xFFFFFFFF;
      for (let i = 0; i < buf.length; i++) c = T[(c ^ buf[i]) & 0xff] ^ (c >>> 8);
      return (c ^ 0xFFFFFFFF) >>> 0;
    };

    const parts = [];
    const central = [];
    let offset = 0;
    const dosTime = 0, dosDate = 0x21; // 1980-01-01

    for (const f of files) {
      const nameBytes = enc.encode(f.name);
      const data = f.data instanceof Uint8Array ? f.data : new Uint8Array(f.data);
      const crc = crc32(data);
      const size = data.length;

      const lh = new Uint8Array(30 + nameBytes.length);
      const lv = new DataView(lh.buffer);
      lv.setUint32(0, 0x04034b50, true);
      lv.setUint16(4, 20, true);
      lv.setUint16(6, 0x0800, true); // UTF-8 filename
      lv.setUint16(8, 0, true);      // store
      lv.setUint16(10, dosTime, true);
      lv.setUint16(12, dosDate, true);
      lv.setUint32(14, crc, true);
      lv.setUint32(18, size, true);
      lv.setUint32(22, size, true);
      lv.setUint16(26, nameBytes.length, true);
      lv.setUint16(28, 0, true);
      lh.set(nameBytes, 30);
      parts.push(lh, data);

      const ch = new Uint8Array(46 + nameBytes.length);
      const cv = new DataView(ch.buffer);
      cv.setUint32(0, 0x02014b50, true);
      cv.setUint16(4, 20, true);
      cv.setUint16(6, 20, true);
      cv.setUint16(8, 0x0800, true);
      cv.setUint16(10, 0, true);
      cv.setUint16(12, dosTime, true);
      cv.setUint16(14, dosDate, true);
      cv.setUint32(16, crc, true);
      cv.setUint32(20, size, true);
      cv.setUint32(24, size, true);
      cv.setUint16(28, nameBytes.length, true);
      cv.setUint16(30, 0, true);
      cv.setUint16(32, 0, true);
      cv.setUint16(34, 0, true);
      cv.setUint16(36, 0, true);
      cv.setUint32(38, 0, true);
      cv.setUint32(42, offset, true);
      ch.set(nameBytes, 46);
      central.push(ch);

      offset += lh.length + data.length;
    }

    let centralSize = 0;
    for (const c of central) centralSize += c.length;
    const centralOffset = offset;
    const eocd = new Uint8Array(22);
    const ev = new DataView(eocd.buffer);
    ev.setUint32(0, 0x06054b50, true);
    ev.setUint16(8, files.length, true);
    ev.setUint16(10, files.length, true);
    ev.setUint32(12, centralSize, true);
    ev.setUint32(16, centralOffset, true);
    return new Blob([...parts, ...central, eocd], { type: 'application/zip' });
  }

  async function runPlaylistDownload(plInfo, mode) {
    dlCancelFlag = false;
    // Snapshot tracks already captured during popup load (early injection path)
    const preloaded = new Map(dlTracks);
    dlTracks.clear();
    window.postMessage({ type: 'VKD_RESET_DL' }, '*');

    try {
      if (plInfo?.ownerId && plInfo?.playlistId) {
        dlSetPhase('Загружаем треки плейлиста…');
        await new Promise(resolve => {
          const t = setTimeout(resolve, 30000);
          const h = e => {
            if (e.source === window && e.data?.type === 'VKD_SECTIONS_DONE') {
              clearTimeout(t); window.removeEventListener('message', h); resolve();
            }
          };
          window.addEventListener('message', h);
          window.postMessage({ type: 'VKD_LOAD_SECTIONS', ownerId: plInfo.ownerId, playlistId: plInfo.playlistId, accessHash: plInfo.accessHash || null }, '*');
        });
      } else {
        dlSetPhase('Прокручиваем список…');
        await scrollToCollect();
      }

      if (dlCancelFlag) { dlSetPhase('Отменено'); return; }

      // ALWAYS extract from popup DOM via React fiber (runs in page context via injected.js)
      dlSetPhase('Читаем треки из попапа…');

      await expandPlaylistModal((loaded, total) => {
        if (total) {
          dlSetPhase(`Раскрываем плейлист… ${loaded} / ${total}`);
          dlSetProgress(loaded, total);
        } else {
          dlSetPhase(`Раскрываем плейлист… ${loaded}`);
        }
      });

      // Extract via page context (injected.js can read React fiber, content.js cannot)
      // Fiber extraction is authoritative — it reads only tracks visible in the playlist popup
      let fiberTracks = [];
      try {
        const domResult = await pageCall('VKD_EXTRACT_DOM', 'VKD_EXTRACT_DOM_DONE', {}, 10000);
        fiberTracks = domResult?.tracks || [];
        console.log('[vmu] fiber extracted:', fiberTracks.length, 'tracks');
      } catch (e) {
        console.warn('[vmu] fiber extraction failed:', e.message);
      }

      if (fiberTracks.length > 0) {
        // Use ONLY fiber tracks (they come from the playlist popup, not from random API responses)
        // Enrich them with URLs from intercepted data if fiber didn't have a URL
        const fiberMap = new Map();
        for (const t of fiberTracks) {
          const intercepted = dlTracks.get(t.id) || preloaded.get(t.id);
          if (!t.url && intercepted?.url) t.url = intercepted.url;
          fiberMap.set(t.id, t);
        }
        dlTracks.clear();
        fiberMap.forEach((v, k) => dlTracks.set(k, v));
      } else {
        // Fallback: merge preloaded snapshot if fiber failed
        if (preloaded.size > 0) {
          preloaded.forEach((v, k) => { if (!dlTracks.has(k)) dlTracks.set(k, v); });
        }
      }

      console.log('[vmu] final playlist tracks:', dlTracks.size);

      // Build download queue — ALL tracks with any URL (direct or HLS)
      const queue = [...dlTracks.values()].filter(t => t.url && t.url.startsWith('http'));
      console.log('[vmu] download queue:', queue.length, '(direct:', queue.filter(t => isGoodUrl(t.url)).length, ', HLS:', queue.filter(t => !isGoodUrl(t.url)).length, ')');

      const total = queue.length;

      if (total === 0) {
        const hlsCount = [...dlTracks.values()].filter(t => t.url && (t.url.includes('/a2/') || t.url.includes('.m3u8'))).length;
        const noUrl = [...dlTracks.values()].filter(t => !t.url || !t.url.startsWith('http')).length;
        const msg = dlTracks.size === 0
          ? 'Треки не найдены. Откройте плейлист и попробуйте ещё раз'
          : `Нет прямых ссылок (HLS: ${hlsCount}, без URL: ${noUrl}). VK отдаёт стрим-формат`;
        console.warn('[vmu]', msg);
        dlSetPhase(msg);
        dlSetFinished(false);
        return;
      }

      dlSetPhase(`Начинаем скачивание: ${total} треков`);
      dlSetProgress(0, total);
      console.log('[vmu] download queue:', total, 'tracks, first url:', queue[0]?.url?.substring(0, 100));

      // Phase 2 — fetch loop. In ZIP mode we accumulate Uint8Arrays + filenames
      // and build a single archive at the end. In individual mode we download
      // each blob via chrome.downloads as it lands.
      const zipFiles = [];
      let done = 0, errors = 0;
      const wantBuffer = (mode === 'zip');

      for (let i = 0; i < queue.length; i++) {
        if (dlCancelFlag) break;
        const track = queue[i];
        dlAddRow(track, 'uploading');
        dlSetPhase(`${i + 1}/${total} — ${track.artist ? track.artist + ' — ' : ''}${track.title}`);

        const meta = [track.artist, track.title].filter(s => String(s || '').trim()).join(' - ') || 'track';
        const fn = dlSanitize(`${dlPad(i + 1, total)} - ${meta}`);
        const isHls = track.url.includes('/a2/') || track.url.includes('.m3u8');
        let res, ext = 'mp3', bytes = null;

        try {
          if (isHls) {
            const hlsUrl = track.url.includes('.m3u8') ? track.url : track.url + '/index.m3u8';
            const r = await pageCall('VKD_HLS_DOWNLOAD', 'VKD_HLS_DOWNLOAD_DONE', { url: hlsUrl, trackId: track.id, returnBuffer: wantBuffer }, 300000);
            if (r?.ok) {
              ext = r.ext || 'ts';
              if (wantBuffer) { bytes = r.buffer; res = { ok: true }; }
              else { res = await sendDlMsg(r.blobUrl, fn + '.' + ext); }
            } else res = { ok: false, error: r?.error || 'HLS failed' };
          } else {
            const r = await pageCall('VKD_FETCH_BLOB', 'VKD_FETCH_BLOB_DONE', { url: track.url, trackId: track.id, returnBuffer: wantBuffer }, 180000);
            if (r?.ok) {
              if (wantBuffer) { bytes = r.buffer; res = { ok: true }; }
              else { res = await sendDlMsg(r.blobUrl, fn + '.mp3'); }
            } else res = { ok: false, error: r?.error || 'fetch failed' };
          }
        } catch (e) { res = { ok: false, error: e.message }; }

        console.log('[vmu] dl', i + 1, res?.ok ? 'OK' : ('ERR: ' + res?.error), isHls ? 'HLS' : 'direct');
        if (res?.ok) {
          done++;
          dlUpdateRow(track.id, 'done');
          if (wantBuffer && bytes) zipFiles.push({ name: fn + '.' + ext, data: new Uint8Array(bytes) });
        } else {
          errors++;
          dlUpdateRow(track.id, 'error', res?.error);
        }
        dlSetProgress(done, total);
        await sleep(120);
      }

      // ZIP mode: build the archive and trigger one download
      if (wantBuffer && zipFiles.length > 0 && !dlCancelFlag) {
        dlSetPhase(`Собираем ZIP… (${zipFiles.length} треков)`);
        const playlistName = dlSanitize(getPlaylistTitle() || 'playlist');
        const zipBlob = vmuZipBuild(zipFiles);
        const blobUrl = URL.createObjectURL(zipBlob);
        const zipRes = await sendDlMsg(blobUrl, playlistName + '.zip');
        if (!zipRes?.ok) errors++;
      }

      dlSetPhase(dlCancelFlag
        ? `Остановлено — ${done} из ${total}`
        : `Готово · ${done}${errors ? `, ошибок ${errors}` : ''} из ${total}`);
      dlSetFinished(!dlCancelFlag && errors === 0);
    } catch (err) {
      dlSetPhase('Ошибка: ' + err.message);
      dlSetFinished(false);
      console.error('[VK Multi Upload DL]', err);
    }
  }

  function getPlaylistTitle() {
    const modal = getActiveModal();
    if (!modal) return null;
    const title = modal.querySelector('[class*="vkitAudioListBoxHeader__info"] a, [class*="vkitAudioListBoxHeader__info"] [class*="TextClamp"]');
    return title?.textContent?.trim() || null;
  }

  // ─── Single-track download on hover ──────────────────────────────────────────

  const ICON_DL_SINGLE = `<svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor"><path d="M10 2.5a.75.75 0 0 1 .75.75v8.19l2.72-2.72a.75.75 0 1 1 1.06 1.06l-4 4a.75.75 0 0 1-1.06 0l-4-4a.75.75 0 0 1 1.06-1.06l2.72 2.72V3.25A.75.75 0 0 1 10 2.5zM3.5 14.25a.75.75 0 0 1 .75.75v1.5h11.5V15a.75.75 0 0 1 1.5 0v2.25a.75.75 0 0 1-.75.75H3.5a.75.75 0 0 1-.75-.75V15a.75.75 0 0 1 .75-.75z"/></svg>`;

  function getTrackDataFromRow(row) {
    // New VK: data stamped by injected.js (React fiber is not visible from the isolated world)
    if (row.dataset?.vmuTrack) {
      try {
        const t = JSON.parse(row.dataset.vmuTrack);
        if (t?.id) return t;
      } catch {}
    }
    // Old VK: data-full-id + DOM text
    const fullId = row.dataset?.fullId || (row.className.match(/_audio_row_(\S+)/) || [])[1];
    if (fullId) {
      const titleEl = row.querySelector('.audio_title, .ai_title, [class*="audio_title"], [class*="AudioRow__title"], .audio_row__title_inner');
      const artistEl = row.querySelector('.audio_artist, .ai_artist, [class*="audio_artist"], [class*="AudioRow__artist"], .audio_row__performers a');
      // reload_audio requires per-track hashes (actionHash/urlHash) as auth —
      // they live at index 13 of the data-audio array: add/edit/action/delete/replace/url/restore
      let reloadId = null;
      try {
        const da = JSON.parse(row.getAttribute('data-audio'));
        const h = String(da[13] || '').split('/');
        if (h[2] && h[5]) reloadId = `${da[1]}_${da[0]}_${h[2]}_${h[5]}`;
      } catch {}
      return {
        id: fullId,
        title: (titleEl?.textContent || '').trim(),
        artist: (artistEl?.textContent || '').trim(),
        url: null,
        reloadId,
      };
    }
    return null;
  }

  async function downloadSingleTrack(track, btnEl) {
    const label = `${track.artist ? track.artist + ' — ' : ''}${track.title}`;
    showProgressToast(`Получение ссылки… ${label}`, { kind: 'progress' });
    if (!track.url) {
      try {
        const result = await pageCall('VKD_RELOAD_AUDIO', 'VKD_RELOAD_AUDIO_DONE', { ids: [track.reloadId || track.id] }, 8000);
        if (result?.resolved?.[track.id]) track.url = result.resolved[track.id];
      } catch {}
    }
    if (!track.url) {
      showProgressToast('Не удалось получить ссылку на трек', { kind: 'error' });
      return;
    }

    btnEl.classList.add('vmu-single-dl-loading');
    const fn = dlSanitize([track.artist, track.title].filter(s => String(s || '').trim()).join(' - ') || 'track');
    const isHls = track.url.includes('/a2/') || track.url.includes('.m3u8');
    let res;

    try {
      if (isHls) {
        showProgressToast(`Подготовка HLS… ${label}`, { kind: 'progress', pct: 0 });
        hlsProgressHandlers.set(track.id, (done, total) => {
          const pct = total ? Math.round((done / total) * 100) : 0;
          showProgressToast(`Скачивание ${pct}% · ${label}`, { kind: 'progress', pct });
        });
        const hlsUrl = track.url.includes('.m3u8') ? track.url : track.url + '/index.m3u8';
        const hlsResult = await pageCall('VKD_HLS_DOWNLOAD', 'VKD_HLS_DOWNLOAD_DONE', { url: hlsUrl, trackId: track.id }, 300000);
        hlsProgressHandlers.delete(track.id);
        if (hlsResult?.ok && hlsResult.blobUrl) {
          showProgressToast(`Сохранение файла… ${label}`, { kind: 'progress', pct: 100 });
          res = await sendDlMsg(hlsResult.blobUrl, fn + '.' + (hlsResult.ext || 'ts'));
        } else {
          res = { ok: false, error: hlsResult?.error || 'HLS failed' };
        }
      } else {
        showProgressToast(`Скачивание… ${label}`, { kind: 'progress' });
        const fetchResult = await pageCall('VKD_FETCH_BLOB', 'VKD_FETCH_BLOB_DONE', { url: track.url, trackId: track.id }, 120000);
        if (fetchResult?.ok && fetchResult.blobUrl) {
          res = await sendDlMsg(fetchResult.blobUrl, fn + '.mp3');
        } else {
          res = { ok: false, error: fetchResult?.error || 'fetch failed' };
        }
      }
    } catch (e) {
      res = { ok: false, error: e.message };
    } finally {
      hlsProgressHandlers.delete(track.id);
    }

    btnEl.classList.remove('vmu-single-dl-loading');
    if (res?.ok) {
      showProgressToast(`Готово · ${label}`, { kind: 'done' });
    } else {
      showProgressToast('Ошибка: ' + (res?.error || 'unknown'), { kind: 'error' });
    }
  }

  // Single body-portal tooltip for all download buttons. CSS ::after tooltips
  // get clipped by VK's gallery containers (ui_gallery__inner_cont has
  // overflow: hidden) on the first row of every column; rendering the tooltip
  // as a position:fixed element appended to body escapes any ancestor clip.
  function getDlTooltipEl() {
    let el = document.getElementById('vmu-tooltip');
    if (!el) {
      el = document.createElement('div');
      el.id = 'vmu-tooltip';
      document.body.appendChild(el);
    }
    return el;
  }
  function showDlTooltip(btn) {
    const text = btn.getAttribute('data-vmu-tip');
    if (!text) return;
    const el = getDlTooltipEl();
    el.textContent = text;
    // Default flavour matches VK's old tt_w.tt_black tooltip (used across the
    // legacy audio rows and top-bar icons). Buttons inside the new vkui
    // playlist modal get the vkui-style tooltip via .vmu-tooltip-new.
    const isNewVk = btn.classList.contains('vmu-single-dl-vkit')
      || btn.classList.contains('vmu-single-dl-after');
    el.classList.toggle('vmu-tooltip-new', isNewVk);
    // Reset placement modifier before measuring so layout reflects the
    // default-above tail height.
    el.classList.remove('vmu-tooltip-below');
    el.classList.add('vmu-tooltip-show');
    const br = btn.getBoundingClientRect();
    const tr = el.getBoundingClientRect();
    // 8 px gap so the 5 px tail tip just touches the icon's edge.
    const GAP = 8;
    let top = br.top - tr.height - GAP;
    if (top < 4) {
      top = br.bottom + GAP;
      el.classList.add('vmu-tooltip-below');
    }
    let left = br.left + br.width / 2 - tr.width / 2;
    left = Math.max(4, Math.min(window.innerWidth - tr.width - 4, left));
    el.style.top = top + 'px';
    el.style.left = left + 'px';
  }
  function hideDlTooltip() {
    const el = document.getElementById('vmu-tooltip');
    if (el) el.classList.remove('vmu-tooltip-show');
  }

  function makeSingleDlBtn(row, extraClass) {
    const btn = document.createElement('button');
    btn.className = 'vmu-single-dl' + (extraClass ? ' ' + extraClass : '');
    btn.innerHTML = ICON_DL_SINGLE;
    btn.setAttribute('data-vmu-tip', 'Скачать');
    // Stop mousedown/pointerdown so VK doesn't start playback through our button
    for (const evt of ['mousedown', 'pointerdown', 'touchstart']) {
      btn.addEventListener(evt, e => { e.stopPropagation(); e.stopImmediatePropagation(); }, true);
    }
    btn.addEventListener('mouseenter', () => showDlTooltip(btn));
    btn.addEventListener('mouseleave', hideDlTooltip);
    btn.addEventListener('click', e => {
      e.preventDefault();
      e.stopPropagation();
      e.stopImmediatePropagation();
      hideDlTooltip();
      if (btn.classList.contains('vmu-single-dl-loading')) return;
      const track = getTrackDataFromRow(row);
      if (!track) { showToast('Не удалось определить трек', true); return; }
      downloadSingleTrack(track, btn);
    }, true);
    return btn;
  }

  function injectSingleDlBtn(row) {
    if (!getTrackDataFromRow(row)) return;

    // Prefer buttonGroup: VK's own action panel. Works for both page-level rows
    // and playlist-modal rows (modal rows have buttonGroup too, but VK mounts it
    // after first sweep — so if a previous sweep placed the button into the
    // after-slot fallback, migrate it here now that the proper slot exists).
    const btnGroup = row.querySelector('[class*="buttonGroup"]');
    if (btnGroup) {
      const existing = row.querySelector('.vmu-single-dl');
      if (existing && existing.parentElement === btnGroup) return;
      if (existing) existing.remove();
      btnGroup.prepend(makeSingleDlBtn(row, 'vmu-single-dl-vkit'));
      return;
    }

    if (row.querySelector('.vmu-single-dl')) return;

    // Fallback: vkitAudioRow__after slot. Used when buttonGroup is not in DOM
    // yet (rare). A subsequent sweep with buttonGroup present will migrate it.
    const after = row.querySelector('[class*="vkitAudioRow__after"]');
    if (after) {
      after.classList.add('vmu-after-host');
      after.prepend(makeSingleDlBtn(row, 'vmu-single-dl-after'));
      return;
    }
    // Old VK: actions container appears only on hover — handled by the observer below
  }

  // Old VK: .audio_row__actions is created on row hover and removed on mouseleave —
  // inject immediately when it appears (no debounce, otherwise the hover is missed)
  new MutationObserver(muts => {
    for (const mut of muts) {
      for (const node of mut.addedNodes) {
        if (node.nodeType !== 1) continue;
        const acts = node.matches?.('.audio_row__actions') ? [node]
          : node.querySelectorAll ? [...node.querySelectorAll('.audio_row__actions')] : [];
        for (const act of acts) {
          if (act.querySelector('.vmu-single-dl')) continue;
          const row = act.closest('.audio_row, [data-full-id]');
          if (!row || !getTrackDataFromRow(row)) continue;
          act.prepend(makeSingleDlBtn(row, 'vmu-single-dl-act'));
        }
      }
    }
  }).observe(document.body, { childList: true, subtree: true });

  // Ask injected.js to stamp data-vmu-track on vkit rows (fiber data lives in
  // the page world), then inject buttons once attributes are in place. We do
  // three passes — virtualized lists sometimes finish mounting after the first
  // mark; the cheap re-scans catch any rows that landed late.
  function markAndInjectAll() {
    const sweep = () => {
      window.postMessage({ type: 'VKD_MARK_ROWS' }, '*');
      setTimeout(() => {
        const rows = document.querySelectorAll('[class*="vkitAudioRow__root"], .AudioRow, .audio_row, [data-full-id]');
        for (const row of rows) injectSingleDlBtn(row);
      }, 50);
    };
    sweep();
    setTimeout(sweep, 200);
    setTimeout(sweep, 600);
  }

  let _dlBtnTimer = null;
  function scanAndInjectDlBtns() {
    clearTimeout(_dlBtnTimer);
    _dlBtnTimer = setTimeout(markAndInjectAll, 150);
  }

  // Dedicated watcher for popup modal — scans at 400ms and 900ms after it appears,
  // then attaches an inner observer that re-runs markAndInjectAll whenever the
  // virtualized list swaps rows (scroll inside the modal).
  let _modalDlTimer1 = null, _modalDlTimer2 = null;
  let _modalInnerObs = null;
  function attachModalInnerObserver(modal) {
    if (!modal || modal.__vmuObsAttached) return;
    modal.__vmuObsAttached = true;
    let t = null;
    const obs = new MutationObserver(muts => {
      for (const mut of muts) {
        for (const node of mut.addedNodes) {
          if (node.nodeType !== 1) continue;
          if (node.matches?.('[class*="vkitAudioRow__root"]') || node.querySelector?.('[class*="vkitAudioRow__root"]')) {
            clearTimeout(t);
            t = setTimeout(markAndInjectAll, 60);
            return;
          }
        }
      }
    });
    obs.observe(modal, { childList: true, subtree: true });
    _modalInnerObs = obs;
  }
  new MutationObserver(muts => {
    for (const mut of muts) {
      for (const node of mut.addedNodes) {
        if (node.nodeType !== 1) continue;
        const modalNode = node.matches?.('[class*="vkitInternalModalBox"]')
          ? node
          : node.querySelector?.('[class*="vkitInternalModalBox"]');
        if (!modalNode) continue;
        clearTimeout(_modalDlTimer1); clearTimeout(_modalDlTimer2);
        _modalDlTimer1 = setTimeout(markAndInjectAll, 400);
        _modalDlTimer2 = setTimeout(() => { markAndInjectAll(); attachModalInnerObserver(modalNode); }, 900);
      }
    }
  }).observe(document.body, { childList: true, subtree: true });

  let _dupesDialogTimer = null;
  // ─── SPA watcher + dialog watcher ────────────────────────────────────────────
  let lastHref = location.href;
  new MutationObserver(() => {
    // SPA navigation: reset state
    if (location.href !== lastHref) {
      lastHref = location.href;
      _pendingDupesPlaylist = null;
      fileQueue = [];
      autoPlaylistRunning = false;
      isProcessing = false;
      uploadDoneCallback = null;
      dlTracks.clear();
      dlCancelFlag = true;
      dlpClose();
    }

    // Inject into VK's upload dialog whenever it appears
    const box = getUploadDialog();
    if (box && !box.dataset.vmuInjected) injectIntoVkDialog(box);

    // Place "Очистить" and settings gear (old VK only — new VK header survives injection)
    if (getUploadDialog() && !document.getElementById('vmu-clear')) tryInjectClearButton();
    if (getUploadDialog() && !document.getElementById('vmu-settings-btn')) tryInjectSettingsIntoHeader();

    // Inject download buttons on music/playlist pages
    // Inject dupes button into playlist edit dialog (debounced)
    clearTimeout(_dupesDialogTimer);
    _dupesDialogTimer = setTimeout(tryInjectDupesIntoEditDialog, 300);

    // Inject single-track download buttons
    scanAndInjectDlBtns();
  }).observe(document.body, { childList: true, subtree: true });
})();