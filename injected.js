// Runs in page context — intercepts XHR/fetch and handles file/UI injection from content script
(function () {
  if (window.__vkMultiUploadInjected) return;
  window.__vkMultiUploadInjected = true;

  const pause = ms => new Promise(r => setTimeout(r, ms));

  // ── VK audio_api_unavailable URL deobfuscator ────────────────────────────────
  // VK serves obfuscated URLs of the form:
  //   https://vk.com/mp3/audio_api_unavailable.mp3?extra=<base64-ish>#<key>
  // The real CDN URL is encoded in `extra` and the op-chain key is in the fragment.
  // Decoding requires the current user's VK id. Ported from yuru-yuri/vk-audio-url-decoder.
  const VK_AUDIO_ALPHABET = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMN0PQRSTUVWXYZO123456789+/=';

  function vkAudioDecode(url) {
    if (!url || typeof url !== 'string' || !url.includes('audio_api_unavailable')) return url;
    const uid = window.vk && window.vk.id;
    if (!uid) return url;
    const after = url.split('?extra=')[1];
    if (!after) return url;
    const [extraPart, hashPart] = after.split('#');
    const N = VK_AUDIO_ALPHABET;

    const decR = (e) => {
      if (!e || e.length % 4 === 1) return false;
      let o = 0, t = 0, r = '';
      for (let a = 0; a < e.length; a++) {
        const i = N.indexOf(e[a]);
        if (i !== -1) {
          t = (o % 4) ? 64 * t + i : i;
          o++;
          if ((o - 1) % 4) {
            const c = String.fromCharCode(255 & (t >> (-2 * o & 6)));
            if (c !== '\x00') r += c;
          }
        }
      }
      return r;
    };
    const v = (e) => e.split('').reverse().join('');
    const r = (e, t) => {
      const arr = e.split('');
      const o = N + N;
      let a = arr.length;
      while (a) { a--; const i = o.indexOf(arr[a]); if (i !== -1) arr[a] = o[i - t]; }
      return arr.join('');
    };
    const decS = (e, t) => {
      const eLen = e.length;
      const out = new Array(eLen);
      let o = eLen;
      t = Math.abs(t);
      while (o) { o--; t = (eLen * (o + 1) ^ (t + o)) % eLen; out[o] = t; }
      return out;
    };
    const s = (e, t) => {
      const eLen = e.length;
      if (!eLen) return e;
      const idx = decS(e, t);
      const arr = e.split('');
      for (let o = 1; o < eLen; o++) {
        const sw = idx[eLen - 1 - o];
        const tmp = arr[sw]; arr[sw] = arr[o]; arr[o] = tmp;
      }
      return arr.join('');
    };
    const i = (e, t) => { const n = parseInt(t, 10); return isNaN(n) ? e : s(e, n ^ uid); };
    const x = (e, t) => {
      let out = ''; const tc = t.charCodeAt(0);
      for (let k = 0; k < e.length; k++) out += String.fromCharCode(e.charCodeAt(k) ^ tc);
      return out;
    };
    const OPS = { v, r, s, i, x };

    const nStr = hashPart ? decR(hashPart) : '';
    let t = decR(extraPart);
    if (typeof nStr !== 'string' || !t) return url;
    const ops = nStr ? nStr.split('\t') : [];
    let k = ops.length;
    while (k--) {
      const parts = ops[k].split('\v');
      const opName = parts.shift();
      parts.unshift(t);
      const fn = OPS[opName];
      if (!fn || parts.length < 2) return url;
      try { t = fn(...parts); } catch { return url; }
    }
    return (typeof t === 'string' && t.startsWith('http')) ? t : url;
  }

  // ── Always-on close escape hatch ────────────────────────────────────────────
  // Even when our hold-open patches block VK's close path, the user must be
  // able to dismiss the panel manually. Intercept clicks on the popup's X
  // icon (popup_box_close) and on any "Закрыть" button inside .audio_add_box,
  // and remove the popup directly — bypasses both the patched .hide() and
  // boxQueue._hide(). Triggered via a capture-phase document listener so we
  // run before VK's own delegated handlers.
  // Close-button intercept. Once our patches kept the popup open through a
  // VK close attempt, VK's internal state for that box is dead and a second
  // native close can't happen (b.hide() returns true but the DOM never
  // unmounts). The user-facing close must therefore drop the hold-open flag
  // AND tear down the popup ourselves.
  document.addEventListener('click', (e) => {
    const t = e.target;
    if (!t || !t.closest) return;
    const box = document.querySelector('.audio_add_box');
    if (!box) return;
    const popup = box.closest('.popup_box_container');
    if (!popup) return;
    const layer = popup.closest('#box_layer') || popup.parentElement;
    const isXIcon = !!t.closest('.box_x_button, ._box_x_button, .popup_box_close, [class*="popup_box_close"], [class*="DismissButton"]');
    const closeBtn = t.closest('button');
    const isCloseBtn = closeBtn && /^\s*Закрыть\s*$/.test(closeBtn.textContent || '') && layer && layer.contains(closeBtn);
    if (isXIcon || isCloseBtn) {
      console.log('[VMU CLOSE] user-close intercepted — flag off + DOM removal');
      e.preventDefault(); e.stopPropagation(); e.stopImmediatePropagation();
      __vmuBlockAudioBoxHide = false;
      clearArmedStyles();
      try { popup.remove(); } catch {}
      // Strip the dark backdrop too — VK normally fades these out when its
      // own close path runs; ours doesn't, so the page stays dimmed.
      for (const sel of ['#box_layer_bg', '#box_layer_wrap', '.box_layer_wrap', '#box_layer', '.box_layer']) {
        for (const el of document.querySelectorAll(sel)) {
          if (!el.querySelector('.popup_box_container')) el.style.display = 'none';
        }
      }
      for (const el of document.querySelectorAll('.scroll_fix_wrap.fixed')) {
        if (!el.querySelector('.popup_box_container')) {
          el.style.display = 'none';
          el.classList.remove('fixed');
        }
      }
      document.body.classList.remove('popup_open', 'has_popup', 'noscroll');
      document.body.style.removeProperty('overflow');
    }
  }, true);

  // ── Preserve VK's Upload manager state across uploads ───────────────────────
  // VK clears Upload.options/obj/vars/uploadUrls[0] right after the first
  // upload finishes (Upload.deinit / onUploadCompleteAll path). All later
  // uploads then have nothing to drive — Upload.onFileApiSend(0, ...) bails
  // because options[0] is undefined. We snapshot the slot the first time we
  // see it populated, restore it before/around each subsequent upload, and
  // suppress deinit while our queue is still working.
  let __vmuUploadSnapshot = null;
  function snapshotUploadSlot() {
    try {
      const U = window.Upload;
      if (!U || !U.options || !U.options[0]) return;
      __vmuUploadSnapshot = {
        options: U.options[0],
        obj: U.obj?.[0],
        vars: U.vars?.[0],
        types: U.types?.[0],
        url: U.uploadUrls?.[0],
        dropbox: U.dropbox?.[0],
        files: U.files?.[0],
      };
      console.log('[VMU UPLOAD] snapshot saved, url=', String(__vmuUploadSnapshot.url || '').slice(0, 60));
    } catch (e) { console.log('[VMU UPLOAD] snapshot failed', e.message); }
  }
  function restoreUploadSlot() {
    const snap = __vmuUploadSnapshot;
    const U = window.Upload;
    if (!snap || !U) return false;
    try {
      U.options = U.options || {};
      U.obj     = U.obj     || {};
      U.vars    = U.vars    || {};
      U.types   = U.types   || {};
      U.uploadUrls = U.uploadUrls || {};
      U.dropbox = U.dropbox || {};
      U.files   = U.files   || {};
      if (!U.options[0]) {
        U.options[0]    = snap.options;
        U.obj[0]        = snap.obj;
        U.vars[0]       = snap.vars;
        U.types[0]      = snap.types;
        U.uploadUrls[0] = snap.url;
        U.dropbox[0]    = snap.dropbox;
        U.files[0]      = snap.files;
        // Reset per-upload counters so VK treats this as a fresh queue
        const o = U.options[0];
        o.uploading = false;
        o.filesQueue = [];
        o.filesTotalSize = 0;
        o.filesTotalCount = 0;
        o.filesLoadedSize = 0;
        o.filesLoadedCount = 0;
        delete window.cur?.multiProgressIndex;
        delete window.cur?.nextQueues;
        console.log('[VMU UPLOAD] slot restored from snapshot');
        return true;
      }
      return false;
    } catch (e) { console.log('[VMU UPLOAD] restore failed', e.message); return false; }
  }
  function patchUploadManager() {
    const U = window.Upload;
    if (!U || U.__vmuPatched) return;
    U.__vmuPatched = true;
    // Suppress deinit while our queue is still working
    const origDeinit = U.deinit && U.deinit.bind(U);
    if (origDeinit) {
      U.deinit = function (...args) {
        if (__vmuBlockAudioBoxHide) {
          console.log('[VMU UPLOAD] blocked Upload.deinit', args[0]);
          return;
        }
        return origDeinit(...args);
      };
    }
    console.log('[VMU UPLOAD] Upload manager patched');
  }

  // ── Prevent VK from auto-closing the audio upload popup ──────────────────────
  // VK's popup system is window.boxQueue; after a successful upload it calls
  // boxQueue._hide(id) (and the instance's .hide()) on the audio_add_box popup.
  // content.js toggles __vmuBlockAudioBoxHide via postMessage while the queue
  // still has files to upload; while it's true, any hide call against a popup
  // that contains .audio_add_box becomes a no-op.
  let __vmuBlockAudioBoxHide = false;
  Object.defineProperty(window, '__vmuFlag', {
    get() { return __vmuBlockAudioBoxHide; },
    configurable: true,
  });

  function boxHasAudioAddBox(box) {
    if (!box) return false;
    const node = box.bodyNode || box.node || box._node || box.contentNode;
    return !!(node && node.querySelector && node.querySelector('.audio_add_box'));
  }

  function patchBoxQueue() {
    const bq = window.boxQueue;
    if (!bq || bq.__vmuPatched) return;
    const origHide = bq._hide.bind(bq);
    bq._hide = function (id) {
      if (__vmuBlockAudioBoxHide) {
        const b = window._message_boxes && window._message_boxes[id];
        if (boxHasAudioAddBox(b)) return;
      }
      return origHide(id);
    };
    bq.__vmuPatched = true;
  }

  // Patch the instance methods of any audio-upload box (each box is built from
  // a factory and has its own .content / .hide copies — so we patch per instance
  // when we see one appear).
  function patchAudioBoxInstance(box) {
    if (!box || box.__vmuPatched) return;
    box.__vmuPatched = true;
    const origContent = box.content && box.content.bind(box);
    if (origContent) {
      box.content = function (e) {
        if (__vmuBlockAudioBoxHide) return;
        return origContent(e);
      };
    }
    for (const m of ['hide', '_hide', '_hideForce', '_hideInternal', 'destroy']) {
      const orig = box[m] && box[m].bind(box);
      if (!orig) continue;
      box[m] = function (...args) {
        if (__vmuBlockAudioBoxHide) return m === 'hide' ? false : undefined;
        return orig(...args);
      };
    }
    // NB: setOptions/setButtons/setControlsText/setBackTitle are NOT blocked —
    // VK uses them to update dialog state after each successful upload, and that
    // update is part of the chain that actually attaches the track to the user's
    // library (save_audio). Blocking them caused uploads to "succeed" on the
    // upload server but never get committed to the audio list.
  }

  // Re-enable the audio popup so the user can keep dropping files after an
  // upload completes. VK's close anim may have set opacity 0 / pointer-events
  // none on the layer that wraps the box — clear them.
  function armBoxVisibility() {
    const box = document.querySelector('.audio_add_box');
    if (!box) return;
    const layers = [box, box.closest('.popup_box_container'), box.closest('#box_layer'), box.closest('.box_body'), box.closest('.box_layout')].filter(Boolean);
    for (const el of layers) {
      const cs = getComputedStyle(el);
      if (cs.pointerEvents === 'none') el.style.pointerEvents = 'auto';
      if (parseFloat(cs.opacity || '1') < 0.99) el.style.opacity = '1';
      if (cs.display === 'none') el.style.display = '';
      if (cs.visibility === 'hidden') el.style.visibility = 'visible';
    }
  }

  // When the "hold open" flag flips off (queue finished or user cancelled),
  // remove all inline overrides we may have set. Otherwise opacity:1 and
  // pointer-events:auto stick on the popup container and VK's natural
  // fade-out close animation can't run — the user can't close the panel.
  function clearArmedStyles() {
    const box = document.querySelector('.audio_add_box');
    if (!box) return;
    const layers = [box, box.closest('.popup_box_container'), box.closest('#box_layer'), box.closest('.box_body'), box.closest('.box_layout')].filter(Boolean);
    for (const el of layers) {
      el.style.removeProperty('pointer-events');
      el.style.removeProperty('opacity');
      el.style.removeProperty('display');
      el.style.removeProperty('visibility');
    }
  }

  function scanAndPatchAudioBoxes() {
    const mb = window._message_boxes;
    if (!mb) return;
    for (const id in mb) {
      if (boxHasAudioAddBox(mb[id])) patchAudioBoxInstance(mb[id]);
    }
  }

  const __vmuBoxQueuePoll = setInterval(() => {
    patchBoxQueue();
    scanAndPatchAudioBoxes();
    patchUploadManager();
    // Take a snapshot whenever VK has a populated slot — overwrites any stale
    // snapshot if VK silently re-initialized between operations.
    if (window.Upload?.options?.[0]) snapshotUploadSlot();
  }, 300);

  window.addEventListener('message', e => {
    if (e.source !== window || !e.data) return;
    if (e.data.type === 'VMU_BLOCK_AUDIO_HIDE') {
      const wasBlocking = __vmuBlockAudioBoxHide;
      __vmuBlockAudioBoxHide = !!e.data.block;
      if (__vmuBlockAudioBoxHide) {
        // entering "hold open" — re-arm in case VK started fading already
        armBoxVisibility();
      } else if (wasBlocking) {
        // leaving "hold open" — strip our inline overrides so VK's natural
        // close animation can run when the user clicks the X.
        clearArmedStyles();
      }
    }
  });

  // Periodic visibility re-arm: between consecutive uploads VK can rev up its
  // close animation between two postMessage frames, leaving the popup faded
  // when we finally block .hide(). The cheap interval below keeps the box
  // clickable for as long as the queue is still working.
  setInterval(() => {
    if (__vmuBlockAudioBoxHide) armBoxVisibility();
  }, 300);

  // ── Capture VK's own al_audio.php hashes for reuse ───────────────────────────
  window.__vmuHashes = {};
  // Callback for one-shot response capture
  let __savePlaylistCapture = null;
  // Capture audio.editPlaylist params for reorder
  let __editPlaylistCapture = null;

  // ── Pending upload → done_add bridge ────────────────────────────────────────
  // VK's upload chain is: pu.vk.com/...upload (file transfer) → al_audio.php?act=done_add
  // (commit). Only done_add tells us whether the track was actually attached to
  // the user's audio (it returns payload[0]=0 on success, error code otherwise —
  // e.g. "9|Вы не можете загружать так много аудиозаписей" on rate limit).
  // We defer VK_UPLOAD_DONE until done_add resolves; if done_add doesn't come
  // within a few seconds (older VK flows without it), we fall back to the raw
  // upload response so the queue doesn't hang.
  let __vmuPendingUpload = null;
  let __vmuPendingTimer = null;
  function emitDoneFromDoneAdd(text) {
    if (!__vmuPendingUpload) return false;
    clearTimeout(__vmuPendingTimer);
    __vmuPendingTimer = null;
    __vmuPendingUpload = null;
    let code, errArr;
    try {
      const d = JSON.parse(text);
      code = d?.payload?.[0];
      errArr = d?.payload?.[1];
    } catch {
      window.postMessage({ type: 'VK_UPLOAD_DONE', error: true, errorMsg: 'Ошибка ответа done_add' }, '*');
      return true;
    }
    if (code === 0 || code === '0') {
      window.postMessage({ type: 'VK_UPLOAD_DONE', response: text }, '*');
    } else {
      let msg = 'Ошибка загрузки';
      if (Array.isArray(errArr) && errArr[0]) {
        let raw = String(errArr[0]).replace(/^"+|"+$/g, '');
        const pipe = raw.indexOf('|');
        if (pipe >= 0) raw = raw.slice(pipe + 1);
        msg = raw.replace(/<br\s*\/?>/gi, ' ').replace(/\\"/g, '"').trim() || msg;
      }
      console.log('[VMU UPLOAD] done_add error code=', code, 'msg=', msg);
      window.postMessage({ type: 'VK_UPLOAD_DONE', error: true, errorMsg: msg, errorCode: code }, '*');
    }
    return true;
  }
  function deferUploadDone(uploadResponseText) {
    __vmuPendingUpload = { response: uploadResponseText };
    clearTimeout(__vmuPendingTimer);
    __vmuPendingTimer = setTimeout(() => {
      if (!__vmuPendingUpload) return;
      console.log('[VMU UPLOAD] done_add timeout — flushing raw upload response');
      const pending = __vmuPendingUpload;
      __vmuPendingUpload = null;
      window.postMessage({ type: 'VK_UPLOAD_DONE', response: pending.response }, '*');
    }, 8000);
  }

  function parseBodyStr(body) {
    if (body instanceof URLSearchParams) return body.toString();
    if (typeof body === 'string') return body;
    if (body instanceof FormData) return [...body.entries()].map(([k,v]) => `${k}=${v}`).join('&');
    return '';
  }

  function extractAct(bodyStr, url) {
    return bodyStr.match(/(?:^|&)act=([^&]+)/)?.[1]
      || url.match(/[?&]act=([^&]+)/)?.[1]
      || '';
  }

  function extractHash(bodyStr) {
    return bodyStr.match(/(?:^|&)hash=([^&]+)/)?.[1] || '';
  }

  // ── XHR interceptor ──────────────────────────────────────────────────────────
  const origOpen = XMLHttpRequest.prototype.open;
  const origSend = XMLHttpRequest.prototype.send;

  XMLHttpRequest.prototype.open = function (method, url, ...rest) {
    this._vkUrl = url;
    return origOpen.call(this, method, url, ...rest);
  };

  XMLHttpRequest.prototype.send = function (body) {
    if (!this._vkUrl) return origSend.call(this, body);
    const url = this._vkUrl;

    // Audio file upload — match pu.vk.com, vkontakte.ru, or any upload.php endpoint
    const isUploadUrl = url.includes('pu.vk.com') || url.includes('vkontakte.ru') || url.includes('upload.php');
    if (isUploadUrl) {
      const xhr = this;
      window.__vmuCurrentUpload = xhr;
      console.log('[VMU XHR] upload start', url.slice(0, 80));
      if (xhr.upload) {
        xhr.upload.addEventListener('progress', (e) => {
          if (e.lengthComputable) {
            window.postMessage({ type: 'VK_UPLOAD_PROGRESS', loaded: e.loaded, total: e.total }, '*');
          }
        });
      }
      this.addEventListener('load', () => {
        console.log('[VMU XHR] load', xhr.status, (xhr.responseText || '').slice(0, 200));
        window.__vmuCurrentUpload = null;
        deferUploadDone(xhr.responseText);
      });
      this.addEventListener('error', () => {
        console.log('[VMU XHR] error');
        window.__vmuCurrentUpload = null;
        window.postMessage({ type: 'VK_UPLOAD_DONE', error: true }, '*');
      });
      this.addEventListener('abort', () => {
        console.log('[VMU XHR] abort');
        window.__vmuCurrentUpload = null;
        window.postMessage({ type: 'VK_UPLOAD_DONE', error: true, aborted: true }, '*');
      });
    }

    // Capture hashes from VK's own al_audio.php calls
    if (url.includes('al_audio.php')) {
      const bStr = parseBodyStr(body);
      const act = extractAct(bStr, url);
      const hash = extractHash(bStr);
      if (hash) { window.__vmuHashes[act || '_page'] = hash; window.__vmuHashes._page = hash; }

      const xhr = this;
      this.addEventListener('load', () => {
        // VK uses save_playlist on personal pages, playlists_edit_data on group pages
        if ((act === 'save_playlist' || act === 'playlists_edit_data') && __savePlaylistCapture) {
          const cb = __savePlaylistCapture;
          __savePlaylistCapture = null;
          cb(xhr.responseText);
        }
        if (act === 'done_add') emitDoneFromDoneAdd(xhr.responseText);
        try {
          const d = JSON.parse(xhr.responseText);
          if (d?.payload) walkForHashes(d.payload);
        } catch {}
      });
    }

    // Capture audio.editPlaylist API calls (VK's new API for saving playlists)
    if (url.includes('audio.editPlaylist')) {
      const bStr = parseBodyStr(body);
      if (__editPlaylistCapture) {
        const cb = __editPlaylistCapture;
        __editPlaylistCapture = null;
        cb(bStr);
      }
    }

    // Capture audio data for playlist download (fires on any al_audio.php response)
    {
      const _xhrDl = this, _urlDl = url;
      _xhrDl.addEventListener('load', () => { try { processForAudioData(_urlDl, _xhrDl.responseText); } catch {} });
    }

    return origSend.call(this, body);
  };

  // ── Fetch interceptor ────────────────────────────────────────────────────────
  const origFetch = window.fetch;
  window.fetch = async function (...args) {
    const url = typeof args[0] === 'string' ? args[0] : (args[0]?.url || '');

    // Audio file upload — match pu.vk.com, vkontakte.ru, or any upload.php endpoint
    const isUploadUrl = url.includes('pu.vk.com') || url.includes('vkontakte.ru') || url.includes('upload.php');
    if (isUploadUrl) {
      return origFetch.apply(this, args).then(result => {
        result.clone().text().then(text => {
          deferUploadDone(text);
        }).catch(() => window.postMessage({ type: 'VK_UPLOAD_DONE', error: true }, '*'));
        return result;
      }, err => {
        window.postMessage({ type: 'VK_UPLOAD_DONE', error: true }, '*');
        throw err;
      });
    }

    const result = await origFetch.apply(this, args);

    if (url.includes('al_audio.php')) {
      const bStr = parseBodyStr(args[1]?.body);
      const act = extractAct(bStr, url);
      const hash = extractHash(bStr);
      if (hash) { window.__vmuHashes[act || '_page'] = hash; window.__vmuHashes._page = hash; }

      result.clone().text().then(text => {
        if ((act === 'save_playlist' || act === 'playlists_edit_data') && __savePlaylistCapture) {
          const cb = __savePlaylistCapture;
          __savePlaylistCapture = null;
          cb(text);
        }
        if (act === 'done_add') emitDoneFromDoneAdd(text);
        try {
          const d = JSON.parse(text);
          if (d?.payload) walkForHashes(d.payload);
        } catch {}
      }).catch(() => {});
    }

    // Capture audio.editPlaylist API calls (VK's new API for saving playlists)
    if (url.includes('audio.editPlaylist')) {
      const bStr = parseBodyStr(args[1]?.body);
      if (__editPlaylistCapture) {
        const cb = __editPlaylistCapture;
        __editPlaylistCapture = null;
        cb(bStr);
      }
    }

    // Capture audio data for playlist download
    if (url.includes('al_audio') || url.includes('api.vk.com')) {
      result.clone().text().then(t => { try { processForAudioData(url, t); } catch {} }).catch(() => {});
    }

    return result;
  };

  function walkForHashes(obj, depth = 0) {
    if (!obj || typeof obj !== 'object' || depth > 4) return;
    for (const [k, v] of Object.entries(obj)) {
      if (typeof v === 'string' && v.length >= 25 && /^[a-zA-Z0-9_\-]+$/.test(v)) {
        if (k.includes('hash') || k.includes('Hash')) window.__vmuHashes[k] = v;
      }
      walkForHashes(v, depth + 1);
    }
  }

  // ── Audio capture for playlist download ──────────────────────────────────────

  const _dlSeen = new Set();

  // ── Capture playlist access_hash from execute responses ────────────────────
  function capturePlaylistMeta(obj, depth) {
    if (!obj || depth > 10) return;
    if (typeof obj === 'object' && !Array.isArray(obj)) {
      if (typeof obj.id === 'number' && typeof obj.owner_id === 'number') {
        const h = obj.access_hash || obj.access_key;
        if (typeof h === 'string' && h.length >= 6) {
          window.__vmuHashes['ph_' + obj.owner_id + '_' + obj.id] = h;
        }
      }
      for (const v of Object.values(obj)) capturePlaylistMeta(v, depth + 1);
    } else if (Array.isArray(obj)) {
      for (const v of obj) capturePlaylistMeta(v, depth + 1);
    }
  }

  function processForAudioData(url, text) {
    if (!text || text.length < 20) return;
    if (!url.includes('al_audio') && !url.includes('/method/') && !url.includes('api.vk.com')) return;
    try {
      const stripped = text.replace(/^<!--[\s\S]*?-->/, '').trim();
      const parsed = JSON.parse(stripped);
      capturePlaylistMeta(parsed, 0);
      walkForAudio(parsed, 0);
    } catch {
      try {
        const parsed = JSON.parse(text);
        capturePlaylistMeta(parsed, 0);
        walkForAudio(parsed, 0);
      } catch {}
    }
  }

  function walkForAudio(obj, depth) {
    if (!obj || depth > 12) return;
    if (Array.isArray(obj)) {
      if (!tryEmitTrack(obj)) obj.forEach(v => walkForAudio(v, depth + 1));
    } else if (typeof obj === 'object') {
      if (!tryEmitTrackObj(obj)) {
        for (const v of Object.values(obj)) walkForAudio(v, depth + 1);
      }
    }
  }

  function tryEmitTrackObj(obj) {
    const id = obj.id, ownerId = obj.owner_id, title = obj.title, artist = obj.artist;
    if (typeof id !== 'number' || typeof ownerId !== 'number') return false;
    if (typeof title !== 'string' || typeof artist !== 'string') return false;
    const trackId = `${ownerId}_${id}`;
    if (_dlSeen.has(trackId)) return true;
    _dlSeen.add(trackId);
    const cs = s => typeof s === 'string' ? s.replace(/<[^>]+>/g, '').trim() : String(s || '').trim();
    window.postMessage({ type: 'VKD_TRACK', track: { id: trackId, title: cs(title), artist: cs(artist), duration: obj.duration || 0, url: obj.url || null } }, '*');
    return true;
  }

  function tryEmitTrack(arr) {
    if (!Array.isArray(arr) || arr.length < 5) return false;
    const [id, ownerId, url, title, artist, duration] = arr;
    if (typeof id !== 'number' || typeof ownerId !== 'number') return false;
    if (typeof title !== 'string' || typeof artist !== 'string') return false;
    const trackId = `${ownerId}_${id}`;
    if (_dlSeen.has(trackId)) return true;
    _dlSeen.add(trackId);
    const cs = s => typeof s === 'string' ? s.replace(/<[^>]+>/g, '').trim() : String(s || '').trim();
    const realUrl = typeof url === 'string' ? vkAudioDecode(url) : url;
    window.postMessage({
      type: 'VKD_TRACK',
      track: { id: trackId, title: cs(title), artist: cs(artist), duration: duration || 0, url: realUrl || null }
    }, '*');
    return true;
  }

  // ── Message dispatcher ────────────────────────────────────────────────────────
  window.addEventListener('message', function (e) {
    if (!e.data || e.source !== window) return;

    switch (e.data.type) {

      case 'VMU_CANCEL_UPLOAD': {
        if (window.__vmuCurrentUpload) {
          try { window.__vmuCurrentUpload.abort(); } catch {}
          window.__vmuCurrentUpload = null;
        }
        break;
      }

      case 'VK_INJECT_FILE': {
        const { name, mimeType, buffer } = e.data;
        const file = new File([buffer], name, { type: mimeType || 'audio/mpeg' });
        console.log('[VMU INJECT] received file', name, file.size, 'bytes');

        // VK clears Upload.options[0] etc. after the first completed upload
        // (deinit / cleanup path). Restore the saved snapshot so subsequent
        // calls have the slot they need to drive the upload.
        if (!window.Upload?.options?.[0]) {
          const restored = restoreUploadSlot();
          console.log('[VMU INJECT] slot was empty, restored=', restored);
        } else {
          // Slot present — reset per-upload counters so VK starts fresh
          const o = window.Upload.options[0];
          o.uploading = false;
          o.filesQueue = [];
          o.filesTotalSize = 0;
          o.filesTotalCount = 0;
          o.filesLoadedSize = 0;
          o.filesLoadedCount = 0;
          delete window.cur?.multiProgressIndex;
          delete window.cur?.fileApiUploadStarted;
        }

        try {
          const o = window.Upload?.options?.[0];
          console.log('[VMU INJECT] Upload.options[0]', o ? {
            uploading: o.uploading,
            filesQueueLen: o.filesQueue?.length,
            filesTotalCount: o.filesTotalCount,
            filesLoadedCount: o.filesLoadedCount,
            uploadUrl: window.Upload.uploadUrls?.[0]?.slice(0, 50),
          } : 'missing');
        } catch (err) {}

        try {
          if (window.Upload && typeof window.Upload.onFileApiSend === 'function' && window.Upload.options?.[0]) {
            console.log('[VMU INJECT] using Upload.onFileApiSend(0, [file])');
            Promise.resolve(window.Upload.onFileApiSend(0, [file])).catch(err => {
              console.log('[VMU INJECT] onFileApiSend rejected:', err?.message || err);
            });
            window.postMessage({ type: 'VK_FILE_INJECTED', ok: true }, '*');
            break;
          }
          console.log('[VMU INJECT] Upload manager unavailable, falling back to DOM input');
        } catch (err) { console.log('[VMU INJECT] direct call failed:', err.message); }

        let tries = 0;
        (function tryInject() {
          const input = document.querySelector('[data-vmu-vk="1"]') || document.querySelector('input[type="file"]');
          if (input) {
            console.log('[VMU INJECT] DOM input found, mark=', !!input.dataset.vmuVk);
            const dt = new DataTransfer();
            dt.items.add(file);
            input.files = dt.files;
            input.dispatchEvent(new Event('change', { bubbles: true }));
            window.postMessage({ type: 'VK_FILE_INJECTED', ok: true }, '*');
          } else if (tries++ < 100) {
            setTimeout(tryInject, 200);
          } else {
            console.log('[VMU INJECT] no input found');
            window.postMessage({ type: 'VK_FILE_INJECTED', ok: false, error: 'input not found' }, '*');
          }
        })();
        break;
      }

      case 'VK_CREATE_PLAYLIST': {
        const { title, description, trackNames, coverBuf } = e.data;
        // Store cover as a File object so the dialog can inject it
        window.__vmuPendingCover = coverBuf
          ? new File([coverBuf], 'cover.jpg', { type: 'image/jpeg' })
          : null;
        createPlaylistViaUI(title, description, trackNames || [])
          .then(r => window.postMessage({ type: 'VK_PLAYLIST_CREATED', ok: true, ...r }, '*'))
          .catch(err => { window.__vmuPendingCover = null; window.postMessage({ type: 'VK_PLAYLIST_CREATED', ok: false, error: err.message }, '*'); });
        break;
      }

      case 'VK_GET_RECENT_AUDIOS': {
        window.postMessage({ type: 'VK_RECENT_AUDIOS', ids: getRecentAudioIdsFromDOM(e.data.count) }, '*');
        break;
      }

      // Fallback: add via al_audio.php (for editing existing playlists)
      case 'VK_ADD_TO_PLAYLIST': {
        const { ownerId, playlistId, audioIds } = e.data;
        callAlAudio('add_to_playlist', {
          owner_id: ownerId, playlist_id: playlistId, audio_ids: audioIds.join(',')
        }).then(d => window.postMessage({ type: 'VK_ADD_PLAYLIST_DONE', ok: d?.payload?.[0] === 0, data: JSON.stringify(d?.payload?.[1]).substring(0,100) }, '*'))
          .catch(err => window.postMessage({ type: 'VK_ADD_PLAYLIST_DONE', ok: false, error: err.message }, '*'));
        break;
      }

      case 'VK_REMOVE_FROM_PLAYLIST': {
        const { ownerId, playlistId, audioId } = e.data;
        callAlAudio('remove_from_playlist', {
          owner_id: ownerId, playlist_id: playlistId, audio_ids: audioId
        }).then(d => window.postMessage({ type: 'VK_REMOVE_PLAYLIST_DONE', audioId, ok: d?.payload?.[0] === 0 }, '*'))
          .catch(err => window.postMessage({ type: 'VK_REMOVE_PLAYLIST_DONE', audioId, ok: false, error: err.message }, '*'));
        break;
      }

      case 'VK_UPLOAD_COVER': {
        injectCoverFile(new File([e.data.buffer], 'cover.jpg', { type: 'image/jpeg' }));
        break;
      }

      case 'VKD_RESET_DL': {
        _dlSeen.clear();
        break;
      }

      case 'VKD_LOAD_SECTIONS': {
        const { ownerId, playlistId, accessHash } = e.data;
        loadPlaylistSections(ownerId, playlistId, accessHash);
        break;
      }

      case 'VKD_EXTRACT_DOM': {
        const tracks = extractTracksFromFiber();
        window.postMessage({ type: 'VKD_EXTRACT_DOM_DONE', ok: true, tracks }, '*');
        break;
      }

      case 'VKD_MARK_ROWS': {
        const marked = markRowTrackData();
        window.postMessage({ type: 'VKD_MARK_ROWS_DONE', ok: true, marked }, '*');
        break;
      }

      case 'VKD_HLS_DOWNLOAD': {
        const { url, trackId, returnBuffer } = e.data;
        const onProgress = (done, total) => {
          window.postMessage({ type: 'VKD_HLS_PROGRESS', trackId, done, total }, '*');
        };
        downloadHlsAsBlob(url, onProgress).then(async result => {
          const msg = { type: 'VKD_HLS_DOWNLOAD_DONE', ok: true, trackId, ext: result.ext };
          const transfers = [];
          if (returnBuffer && result.blob) {
            const buf = await result.blob.arrayBuffer();
            msg.buffer = buf;
            transfers.push(buf);
          } else {
            msg.blobUrl = result.blobUrl;
          }
          window.postMessage(msg, '*', transfers);
        }).catch(err => {
          window.postMessage({ type: 'VKD_HLS_DOWNLOAD_DONE', ok: false, trackId, error: err.message }, '*');
        });
        break;
      }

      case 'VKD_FETCH_BLOB': {
        const { url, trackId, returnBuffer } = e.data;
        origFetch(url, { headers: { Referer: 'https://vk.com/' } })
          .then(resp => {
            if (!resp.ok) throw new Error('fetch failed: ' + resp.status);
            return resp.blob();
          })
          .then(async blob => {
            const msg = { type: 'VKD_FETCH_BLOB_DONE', ok: true, trackId, size: blob.size };
            const transfers = [];
            if (returnBuffer) {
              const buf = await blob.arrayBuffer();
              msg.buffer = buf;
              transfers.push(buf);
            } else {
              msg.blobUrl = URL.createObjectURL(blob);
            }
            window.postMessage(msg, '*', transfers);
          })
          .catch(err => {
            window.postMessage({ type: 'VKD_FETCH_BLOB_DONE', ok: false, trackId, error: err.message }, '*');
          });
        break;
      }

      case 'VKD_RELOAD_AUDIO': {
        const { ids } = e.data;
        reloadAudioUrls(ids).then(resolved => {
          window.postMessage({ type: 'VKD_RELOAD_AUDIO_DONE', ok: true, resolved }, '*');
        }).catch(err => {
          window.postMessage({ type: 'VKD_RELOAD_AUDIO_DONE', ok: false, error: err.message, resolved: {} }, '*');
        });
        break;
      }

      case 'VKD_SCROLL_LOAD': {
        // Fallback scroll (legacy path)
        const sc = document.querySelector('.audio_page, .AudioPageDynamic, [class*="AudioList"]')
          || document.scrollingElement || document.body;
        sc.scrollTop = sc.scrollHeight;
        if (document.scrollingElement) document.scrollingElement.scrollTop = document.scrollingElement.scrollHeight;
        break;
      }
    }
  });

  // ── HLS download: fetch m3u8 → download .ts segments → decrypt AES-128 → concat → blob URL ──

  function resolveUrl(uri, baseUrl) {
    if (uri.startsWith('http')) return uri;
    try { return new URL(uri, baseUrl).href; } catch {}
    return baseUrl + uri;
  }

  function parseM3u8(text, baseUrl) {
    const segments = [];
    let currentKey = null;
    let mediaSequence = 0;
    const lines = text.split('\n');
    for (let i = 0; i < lines.length; i++) {
      const line = lines[i].trim();
      if (line.startsWith('#EXT-X-MEDIA-SEQUENCE:')) {
        mediaSequence = parseInt(line.split(':')[1], 10) || 0;
      }
      if (line.startsWith('#EXT-X-KEY:')) {
        const methodMatch = line.match(/METHOD=([^,]+)/);
        const uriMatch = line.match(/URI="([^"]+)"/);
        const ivMatch = line.match(/IV=0x([0-9a-fA-F]+)/);
        const method = methodMatch?.[1] || 'NONE';
        if (method === 'AES-128' && uriMatch) {
          currentKey = { method, uri: resolveUrl(uriMatch[1], baseUrl), iv: ivMatch?.[1] || null };
        } else {
          currentKey = null;
        }
      }
      if (line && !line.startsWith('#')) {
        segments.push({ url: resolveUrl(line, baseUrl), key: currentKey, seqNum: mediaSequence + segments.length });
      }
    }
    return segments;
  }

  async function downloadHlsAsBlob(m3u8Url, onProgress) {
    // Fix doubled /index.m3u8 (content.js may append it when URL already has .m3u8?query)
    m3u8Url = m3u8Url.replace(/(\.m3u8[^/]*?)\/index\.m3u8$/, '$1');
    const hlsFetch = (url) => origFetch(url, { headers: { Referer: 'https://vk.com/' } });

    const m3u8Resp = await hlsFetch(m3u8Url);
    if (!m3u8Resp.ok) throw new Error('m3u8 fetch failed: ' + m3u8Resp.status);
    let m3u8Text = await m3u8Resp.text();
    let baseUrl = m3u8Url.substring(0, m3u8Url.lastIndexOf('/') + 1);

    // Handle master playlist: pick highest bandwidth stream
    if (m3u8Text.includes('#EXT-X-STREAM-INF')) {
      const lines = m3u8Text.split('\n');
      let bestBw = -1, bestUri = null;
      for (let i = 0; i < lines.length; i++) {
        const m = lines[i].match(/#EXT-X-STREAM-INF.*?BANDWIDTH=(\d+)/);
        if (m) {
          const bw = parseInt(m[1], 10);
          const uri = (lines[i + 1] || '').trim();
          if (uri && !uri.startsWith('#') && bw > bestBw) { bestBw = bw; bestUri = uri; }
        }
      }
      if (!bestUri) throw new Error('no streams in master m3u8');
      const mediaUrl = resolveUrl(bestUri, baseUrl);
      console.log('[vmu] HLS master playlist, selected stream:', bestBw, 'bps');
      const mediaResp = await hlsFetch(mediaUrl);
      if (!mediaResp.ok) throw new Error('media m3u8 fetch failed: ' + mediaResp.status);
      m3u8Text = await mediaResp.text();
      baseUrl = mediaUrl.substring(0, mediaUrl.lastIndexOf('/') + 1);
    }

    console.log('[vmu] m3u8 content:\n', m3u8Text.substring(0, 500));
    const segments = parseM3u8(m3u8Text, baseUrl);
    if (segments.length === 0) throw new Error('no segments in m3u8');

    const encrypted = segments.some(s => s.key);
    console.log('[vmu] HLS:', segments.length, 'segments,', encrypted ? 'AES-128 encrypted' : 'no encryption');
    if (encrypted) console.log('[vmu] key URI:', segments[0].key?.uri?.substring(0, 120));

    // Cache decryption keys (usually one key per playlist)
    const keyCache = {};
    let decryptionAvailable = true;

    async function getKey(uri) {
      if (keyCache[uri]) return keyCache[uri];
      try {
        const resp = await hlsFetch(uri);
        if (!resp.ok) { console.warn('[vmu] key fetch HTTP', resp.status); return null; }
        const rawKey = await resp.arrayBuffer();
        if (rawKey.byteLength !== 16 && rawKey.byteLength !== 32) {
          console.warn('[vmu] bad key size:', rawKey.byteLength);
          return null;
        }
        const cryptoKey = await crypto.subtle.importKey('raw', rawKey, { name: 'AES-CBC' }, false, ['decrypt']);
        keyCache[uri] = cryptoKey;
        return cryptoKey;
      } catch (e) {
        console.warn('[vmu] getKey error:', e.message);
        return null;
      }
    }

    function hexToIv(hex) {
      const bytes = new Uint8Array(16);
      const h = hex.padStart(32, '0');
      for (let i = 0; i < 32; i += 2) bytes[i / 2] = parseInt(h.substr(i, 2), 16);
      return bytes;
    }

    function seqNumToIv(n) {
      const iv = new Uint8Array(16);
      iv[15] = n & 0xff; iv[14] = (n >> 8) & 0xff;
      iv[13] = (n >> 16) & 0xff; iv[12] = (n >> 24) & 0xff;
      return iv;
    }

    // Download and optionally decrypt segments
    const chunks = [];
    for (let i = 0; i < segments.length; i++) {
      const seg = segments[i];
      const resp = await hlsFetch(seg.url);
      if (!resp.ok) throw new Error(`segment ${i} failed: ${resp.status}`);
      let data = await resp.arrayBuffer();

      if (seg.key && decryptionAvailable) {
        const cryptoKey = await getKey(seg.key.uri);
        if (cryptoKey) {
          const standardIv = seg.key.iv ? hexToIv(seg.key.iv) : seqNumToIv(seg.seqNum);
          try {
            data = await crypto.subtle.decrypt({ name: 'AES-CBC', iv: standardIv }, cryptoKey, data);
          } catch {
            try {
              const vkIv = new Uint8Array(data.slice(0, 16));
              data = await crypto.subtle.decrypt({ name: 'AES-CBC', iv: vkIv }, cryptoKey, data.slice(16));
            } catch (e2) {
              console.warn('[vmu] decrypt failed seg', i, e2.message);
            }
          }
        } else if (i === 0) {
          console.warn('[vmu] key not available, downloading without decryption');
          decryptionAvailable = false;
        }
      }

      chunks.push(data);
      if (typeof onProgress === 'function') {
        try { onProgress(i + 1, segments.length); } catch {}
      }
      if (i % 5 === 4) await pause(50);
    }

    // Concatenate all segments
    const totalSize = chunks.reduce((s, c) => s + c.byteLength, 0);
    const combined = new Uint8Array(totalSize);
    let offset = 0;
    for (const chunk of chunks) {
      combined.set(new Uint8Array(chunk), offset);
      offset += chunk.byteLength;
    }
    console.log('[vmu] HLS raw TS size:', (totalSize / 1024 / 1024).toFixed(1), 'MB');

    // Check if data is already raw audio (not TS container)
    if (combined[0] !== 0x47) {
      if (combined[0] === 0xFF && (combined[1] & 0xE0) === 0xE0) {
        const blob = new Blob([combined], { type: 'audio/mpeg' });
        return { blobUrl: URL.createObjectURL(blob), ext: 'mp3', blob };
      }
      if (combined[0] === 0xFF && (combined[1] & 0xF0) === 0xF0) {
        const blob = new Blob([combined], { type: 'audio/aac' });
        return { blobUrl: URL.createObjectURL(blob), ext: 'aac', blob };
      }
    }

    // Extract raw audio frames from MPEG-TS container
    const audioResult = extractAacFromTs(combined);
    if (audioResult) {
      const { data: audio, codec } = audioResult;
      const ext = codec === 'mp3' ? 'mp3' : 'aac';
      const mime = codec === 'mp3' ? 'audio/mpeg' : 'audio/aac';
      console.log('[vmu] extracted', ext + ':', (audio.byteLength / 1024 / 1024).toFixed(1), 'MB');
      const blob = new Blob([audio], { type: mime });
      return { blobUrl: URL.createObjectURL(blob), ext, blob };
    }
    // Fallback: return raw TS
    console.warn('[vmu] audio extraction failed, returning raw TS');
    const blob = new Blob([combined], { type: 'video/mp2t' });
    return { blobUrl: URL.createObjectURL(blob), ext: 'ts', blob };
  }

  // Extract audio frames from MPEG-TS data. Returns { data: Uint8Array, codec: 'mp3'|'aac' } or null
  function extractAacFromTs(tsData) {
    const bytes = tsData instanceof Uint8Array ? tsData : new Uint8Array(tsData);
    if (bytes.length < 188) return null;

    // Step 1: find audio PID from PAT → PMT
    let audioPid = -1;
    const TS = 188;
    const findSync = (start) => {
      for (let i = start; i < bytes.length - TS; i++) {
        if (bytes[i] === 0x47 && bytes[i + TS] === 0x47) return i;
      }
      return -1;
    };

    let syncOff = findSync(0);
    if (syncOff < 0) return null;

    // Parse PAT to get PMT PID
    let pmtPid = -1;
    for (let pos = syncOff; pos + TS <= bytes.length; pos += TS) {
      if (bytes[pos] !== 0x47) { syncOff = findSync(pos); if (syncOff < 0) break; pos = syncOff; }
      const pid = ((bytes[pos + 1] & 0x1F) << 8) | bytes[pos + 2];
      if (pid !== 0) continue; // PAT is PID 0
      const pusi = !!(bytes[pos + 1] & 0x40);
      const afc = (bytes[pos + 3] >> 4) & 0x03;
      let payOff = pos + 4;
      if (afc === 3) payOff += 1 + bytes[pos + 4];
      if (pusi) payOff += 1 + bytes[payOff]; // pointer field
      // PAT: skip 8 bytes header, then 4-byte entries
      payOff += 8;
      if (payOff + 4 <= pos + TS) {
        pmtPid = ((bytes[payOff + 2] & 0x1F) << 8) | bytes[payOff + 3];
      }
      break;
    }
    if (pmtPid < 0) return null;

    // Parse PMT to find audio PID (stream_type 0x0F=AAC, 0x11=AAC-LATM, 0x03/0x04=MP3)
    let audioStreamType = 0;
    for (let pos = syncOff; pos + TS <= bytes.length; pos += TS) {
      if (bytes[pos] !== 0x47) continue;
      const pid = ((bytes[pos + 1] & 0x1F) << 8) | bytes[pos + 2];
      if (pid !== pmtPid) continue;
      const pusi = !!(bytes[pos + 1] & 0x40);
      const afc = (bytes[pos + 3] >> 4) & 0x03;
      let payOff = pos + 4;
      if (afc === 3) payOff += 1 + bytes[pos + 4];
      if (pusi) payOff += 1 + bytes[payOff];
      // PMT header
      const secLen = ((bytes[payOff + 1] & 0x0F) << 8) | bytes[payOff + 2];
      const progInfoLen = ((bytes[payOff + 10] & 0x0F) << 8) | bytes[payOff + 11];
      let p = payOff + 12 + progInfoLen;
      const secEnd = payOff + 3 + secLen - 4; // minus CRC
      while (p + 5 <= secEnd && p < pos + TS) {
        const sType = bytes[p];
        const sPid = ((bytes[p + 1] & 0x1F) << 8) | bytes[p + 2];
        const esLen = ((bytes[p + 3] & 0x0F) << 8) | bytes[p + 4];
        if (sType === 0x0F || sType === 0x11 || sType === 0x03 || sType === 0x04) {
          audioPid = sPid;
          audioStreamType = sType;
          console.log('[vmu] TS audio PID:', audioPid, 'type:', sType === 0x0F ? 'AAC' : sType === 0x03 ? 'MP3' : '0x' + sType.toString(16));
          break;
        }
        p += 5 + esLen;
      }
      break;
    }
    if (audioPid < 0) return null;

    // Step 2: collect PES payloads from audio PID
    const pesChunks = [];
    for (let pos = syncOff; pos + TS <= bytes.length; pos += TS) {
      if (bytes[pos] !== 0x47) continue;
      const pid = ((bytes[pos + 1] & 0x1F) << 8) | bytes[pos + 2];
      if (pid !== audioPid) continue;
      const pusi = !!(bytes[pos + 1] & 0x40);
      const afc = (bytes[pos + 3] >> 4) & 0x03;
      let payOff = pos + 4;
      if (afc === 3) payOff += 1 + bytes[pos + 4];
      if (afc === 2) continue; // no payload
      if (pusi) {
        // PES header: skip start code (3) + stream_id (1) + PES_length (2) + flags (3) + header_data_length
        const pesHdrLen = bytes[payOff + 8];
        payOff += 9 + pesHdrLen;
      }
      if (payOff < pos + TS) {
        pesChunks.push(bytes.subarray(payOff, pos + TS));
      }
    }

    if (pesChunks.length === 0) return null;
    const audioTotal = pesChunks.reduce((s, c) => s + c.length, 0);
    const audioData = new Uint8Array(audioTotal);
    let off = 0;
    for (const c of pesChunks) { audioData.set(c, off); off += c.length; }
    const codec = (audioStreamType === 0x03 || audioStreamType === 0x04) ? 'mp3' : 'aac';
    return { data: audioData, codec };
  }

  // Walk up the React fiber tree from a DOM element looking for a node whose
  // props expose `track.entity`. New VK wraps each row in 7+ levels of HOCs,
  // so the entity does not live on the row element itself.
  function findTrackEntityFromFiber(el, maxDepth) {
    const fiberKey = Object.keys(el).find(k => k.startsWith('__reactFiber$') || k.startsWith('__reactInternalInstance$'));
    if (!fiberKey) return null;
    let cur = el[fiberKey];
    const limit = maxDepth || 20;
    for (let i = 0; i < limit && cur; i++) {
      const props = cur.memoizedProps || cur.pendingProps;
      const entity = props?.track?.entity || props?.audio?.entity || props?.entity;
      if (entity && entity.data?.identity) return entity;
      cur = cur.return;
    }
    return null;
  }

  // ── Stamp data-vmu-track on audio rows so the content script (isolated world,
  // no access to React fiber expandos) can read track data from the DOM ──
  function markRowTrackData() {
    let marked = 0;
    for (const row of document.querySelectorAll('[class*="vkitAudioRow__root"], .AudioRow')) {
      try {
        const entity = findTrackEntityFromFiber(row);
        if (!entity) continue;
        const identity = entity.data?.identity;
        const ownerId = identity?.ownerId;
        const audioId = identity?.id;
        if (!ownerId || !audioId) continue;
        // Always overwrite — virtualized lists recycle row elements for other tracks
        row.dataset.vmuTrack = JSON.stringify({
          id: `${ownerId}_${audioId}`,
          title: entity.title || '',
          artist: entity.authors?.main?.[0]?.name || entity.subtitle || '',
          url: entity.url || null,
          isBlocked: !!(entity.data?.isBlocked) || entity.data?.url === null,
        });
        marked++;
      } catch {}
    }
    return marked;
  }

  // ── Extract tracks from popup DOM via React fiber (runs in page context) ──
  function extractTracksFromFiber() {
    const tracks = [];
    const seen = new Set();

    const modal = [...document.querySelectorAll('[class*="vkitInternalModalBox"]')]
      .find(m => m.getBoundingClientRect().width > 0);
    const container = modal || document;
    const rows = container.querySelectorAll('[class*="vkitAudioRow__root"], .AudioRow, [data-full-id]');

    for (const row of rows) {
      try {
        const entity = findTrackEntityFromFiber(row);
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

        // Fallback: data-full-id (old VK)
        const fullId = row.dataset?.fullId;
        if (!fullId || seen.has(fullId)) continue;
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

  // ── Resolve direct audio URLs via al_audio.php?act=reload_audio ──────────
  async function reloadAudioUrls(ids) {
    // No page-level hash: reload_audio authorizes via per-track hashes inside ids
    // (owner_audio_actionHash_urlHash) — sending a page hash yields "bad_hash"
    const resolved = {};
    // Batch in groups of 10
    for (let i = 0; i < ids.length; i += 10) {
      const batch = ids.slice(i, i + 10);
      try {
        const body = new URLSearchParams({
          act: 'reload_audio',
          al: '1',
          ids: batch.join(','),
        });
        const res = await origFetch('/al_audio.php', {
          method: 'POST',
          body,
          headers: { 'X-Requested-With': 'XMLHttpRequest' },
        });
        const text = await res.text();
        console.log('[vmu] reload_audio response:', text.substring(0, 150));

        // Parse response — VK returns track arrays
        const stripped = text.replace(/^<!--[\s\S]*?-->/, '').trim();
        let data;
        try { data = JSON.parse(stripped); } catch { data = JSON.parse(text); }

        // Walk through response to find track arrays and extract URLs
        const walkReload = (obj, depth) => {
          if (!obj || depth > 8) return;
          if (Array.isArray(obj)) {
            if (obj.length >= 5 && typeof obj[0] === 'number' && typeof obj[1] === 'number') {
              const trackId = `${obj[1]}_${obj[0]}`;
              const rawUrl = obj[2];
              if (typeof rawUrl === 'string' && rawUrl) {
                // Deobfuscate VK's audio_api_unavailable stub URLs into real CDN URLs.
                // Reload_audio returns a placeholder mp3 that decodes (via window.vk.id +
                // op chain in the URL fragment) into the actual /a2/...m3u8 stream.
                const realUrl = vkAudioDecode(rawUrl);
                const put = (u) => {
                  if (!u || !u.startsWith('http')) return;
                  if (u.includes('audio_api_unavailable')) return; // never store stubs
                  const isHls = u.includes('/a2/') || u.includes('.m3u8');
                  const cur = resolved[trackId];
                  const curIsHls = cur && (cur.includes('/a2/') || cur.includes('.m3u8'));
                  if (!cur || (curIsHls && !isHls)) resolved[trackId] = u;
                };
                put(realUrl);
              }
            } else {
              for (const v of obj) walkReload(v, depth + 1);
            }
          } else if (typeof obj === 'object') {
            for (const v of Object.values(obj)) walkReload(v, depth + 1);
          }
        };
        walkReload(data, 0);
      } catch (e) {
        console.warn('[vmu] reload_audio batch error:', e.message);
      }
      if (i + 10 < ids.length) await pause(300);
    }
    console.log('[vmu] reload_audio resolved:', Object.keys(resolved).length, 'of', ids.length);
    return resolved;
  }

  async function loadPlaylistSections(ownerId, playlistId, accessHash) {
    const BATCH = 50;
    const hash = window.__vmuHashes.load_section || window.__vmuHashes._page || getPageHashFromDOM();
    if (!hash) {
      console.warn('[vmu] loadPlaylistSections: no CSRF hash found');
      window.postMessage({ type: 'VKD_SECTIONS_DONE' }, '*'); return;
    }

    // access_hash: from caller, or captured from execute response, or from URL hash
    const aHash = accessHash || window.__vmuHashes['ph_' + ownerId + '_' + playlistId] || null;
    console.log('[vmu] loadPlaylistSections', ownerId, playlistId, 'access_hash:', aHash ? aHash.substring(0, 10) + '…' : 'none', 'csrf:', hash.substring(0, 12) + '…');

    // Try playlist-specific type values — do NOT fall back to generic types (0/1/2 load wrong sections)
    const TYPES = ['playlist', 'album'];
    let workingType = null;
    for (const type of TYPES) {
      try {
        const params = { act: 'load_section', al: '1', hash, id: playlistId, owner_id: ownerId, type, offset: 0, utf8: '1' };
        if (aHash) params.access_hash = aHash;
        const body = new URLSearchParams(params);
        const res = await origFetch('/al_audio.php', { method: 'POST', body, headers: { 'X-Requested-With': 'XMLHttpRequest' } });
        const text = await res.text();
        console.log('[vmu] load_section type=' + type, 'resp:', text.substring(0, 120));
        if (text.includes('"ERR_') || text.includes('ERR_1')) continue;
        const before = _dlSeen.size;
        processForAudioData('/al_audio.php', text);
        if (_dlSeen.size > before) { workingType = type; break; }
      } catch (e) { console.warn('[vmu] load_section type=' + type + ' error:', e); continue; }
    }

    if (!workingType) {
      console.warn('[vmu] loadPlaylistSections: all types failed, falling back to preloaded');
      window.postMessage({ type: 'VKD_SECTIONS_DONE' }, '*'); return;
    }

    // Paginate remaining batches
    for (let offset = BATCH; ; offset += BATCH) {
      try {
        const params = { act: 'load_section', al: '1', hash, id: playlistId, owner_id: ownerId, type: workingType, offset, utf8: '1' };
        if (aHash) params.access_hash = aHash;
        const body = new URLSearchParams(params);
        const res = await origFetch('/al_audio.php', { method: 'POST', body, headers: { 'X-Requested-With': 'XMLHttpRequest' } });
        const text = await res.text();
        const before = _dlSeen.size;
        processForAudioData('/al_audio.php', text);
        const added = _dlSeen.size - before;
        if (added < BATCH) break;
        await pause(250);
      } catch { break; }
    }
    window.postMessage({ type: 'VKD_SECTIONS_DONE' }, '*');
  }

  // ── Create playlist via VK UI automation ─────────────────────────────────────
  // Pure ASCII/unicode-escape strings — no Cyrillic source bytes
  const LBL_CREATE  = '\u0421\u043e\u0437\u0434\u0430\u0442\u044c \u043f\u043b\u0435\u0439\u043b\u0438\u0441\u0442'; // \u0421\u043e\u0437\u0434\u0430\u0442\u044c \u043f\u043b\u0435\u0439\u043b\u0438\u0441\u0442
  const LBL_SAVE    = '\u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c'; // \u0421\u043e\u0445\u0440\u0430\u043d\u0438\u0442\u044c
  const LBL_ADD_AUD = 'ape_add_audios_btn';

  function findCreateBtn() {
    // Try by exact aria-label
    let btn = document.querySelector('[aria-label="' + LBL_CREATE + '"]');
    if (btn) return btn;
    // Try by button text or partial aria-label match
    for (const b of document.querySelectorAll('button')) {
      const lbl = b.getAttribute('aria-label') || '';
      const txt = b.textContent || '';
      if (lbl === LBL_CREATE || txt.trim() === LBL_CREATE) return b;
      if (lbl.indexOf('плейлист') !== -1 &&
          lbl.indexOf('Созд') !== -1) return b;
    }
    return null;
  }

  // Wait up to 8s for the button to appear (VK SPA may re-render after upload)
  function waitForCreateBtn(timeout) {
    return new Promise((resolve, reject) => {
      const btn = findCreateBtn();
      if (btn) { resolve(btn); return; }
      const deadline = Date.now() + (timeout || 8000);
      const obs = new MutationObserver(() => {
        const found = findCreateBtn();
        if (found) { obs.disconnect(); resolve(found); }
      });
      obs.observe(document.body, { childList: true, subtree: true, attributes: true, attributeFilter: ['aria-label'] });
      setTimeout(() => {
        obs.disconnect();
        const found = findCreateBtn();
        found ? resolve(found) : reject(new Error('btn_create_playlist_not_found'));
      }, deadline - Date.now());
    });
  }

  function findCropSaveBtn() {
    // Try known class names
    for (const sel of ['.photo_crop_button_save', '[class*="cropSave"]', '[class*="crop_save"]']) {
      const btn = document.querySelector(sel);
      if (btn) return btn;
    }
    // Fallback: find button with "Сохранить" text inside crop/photo dialogs
    for (const btn of document.querySelectorAll('button, .FlatButton')) {
      const txt = (btn.textContent || '').trim();
      if (txt === 'Сохранить' && btn.closest('[class*="crop"], [class*="photo_editor"], .pv_save_btn, .popup_box_container')) {
        return btn;
      }
    }
    // Last resort: any "Сохранить и продолжить" or "Сохранить" inside a popup that appeared after cover
    for (const btn of document.querySelectorAll('.popup_box_container button, .box_layout button')) {
      const txt = (btn.textContent || '').trim();
      if (txt.includes('Сохран') && btn.offsetParent !== null) return btn;
    }
    return null;
  }

  // PHASE 1: Create empty playlist (title + description + cover, no tracks)
  // PHASE 2: Open edit dialog on new playlist — ._ape_audio_item items are directly visible there
  async function createPlaylistViaUI(title, description, trackNames) {
    const createBtn = await waitForCreateBtn(8000);

    const responseText = await new Promise((resolve, reject) => {
      const timeout = setTimeout(() => { __savePlaylistCapture = null; __editPlaylistCapture = null; reject(new Error('save_playlist_timeout')); }, 30000);
      const onCapture = (text) => { clearTimeout(timeout); __savePlaylistCapture = null; __editPlaylistCapture = null; resolve(text); };
      __savePlaylistCapture = onCapture;
      __editPlaylistCapture = onCapture;

      (async () => {
        createBtn.click();
        const dialog = await waitForEl('.audio_pl_edit_box', 4000);
        if (!dialog) { clearTimeout(timeout); __savePlaylistCapture = null; reject(new Error('dialog_not_opened')); return; }

        await pause(400);

        // Fill title
        const titleEl = dialog.querySelector('#ape_pl_name') || dialog.querySelector('input.ape_pl_input');
        if (titleEl) setVal(titleEl, title);

        // Fill description — use execCommand to preserve \n as real newline
        const descEl = dialog.querySelector('#ape_pl_description') || dialog.querySelector('textarea.ape_pl_input');
        if (descEl) {
          descEl.focus();
          descEl.select();
          const inserted = document.execCommand('insertText', false, description);
          if (!inserted || !descEl.value.includes('\n')) setVal(descEl, description);
          descEl.blur();
        }

        // Inject cover via .ape_cover click-intercept
        if (window.__vmuPendingCover) {
          const coverEl = dialog.querySelector('.ape_cover, ._ape_cover');
          if (coverEl) {
            const coverFile = window.__vmuPendingCover;
            window.__vmuPendingCover = null;
            await new Promise((res) => {
              let intercepted = false;
              const origClick = HTMLInputElement.prototype.click;
              HTMLInputElement.prototype.click = function () {
                if (!intercepted && this.type === 'file') {
                  intercepted = true;
                  HTMLInputElement.prototype.click = origClick;
                  const dt = new DataTransfer();
                  dt.items.add(coverFile);
                  this.files = dt.files;
                  this.dispatchEvent(new Event('change', { bubbles: true }));
                  // Wait for crop dialog and confirm it
                  let n = 0;
                  const t = setInterval(() => {
                    const cropBtn = findCropSaveBtn();
                    if (cropBtn) {
                      cropBtn.click();
                      clearInterval(t);
                      // Wait for crop to apply and cover thumbnail to update
                      let waitN = 0;
                      const waitT = setInterval(() => {
                        const coverImg = dialog.querySelector('.ape_cover img, .ape_cover [style*="background"]');
                        if (coverImg || ++waitN >= 10) { clearInterval(waitT); res(); }
                      }, 500);
                      return;
                    }
                    if (++n >= 30) { clearInterval(t); res(); }
                  }, 400);
                  return;
                }
                return origClick.call(this);
              };
              coverEl.click();
              setTimeout(() => {
                if (!intercepted) { HTMLInputElement.prototype.click = origClick; res(); }
              }, 8000);
            });
            await pause(1000);
          } else {
            window.__vmuPendingCover = null;
          }
        }

        // Save playlist (no tracks yet)
        await pause(300);
        const boxLayout = dialog.closest('.box_layout') || dialog.closest('.box_body')?.parentElement;
        const saveBtn = (boxLayout || document).querySelector('.FlatButton--primary') || document.querySelector('.FlatButton--primary');
        if (!saveBtn) { clearTimeout(timeout); __savePlaylistCapture = null; reject(new Error('save_btn_not_found')); return; }
        saveBtn.click();
      })();
    });

    // Get playlist ID: primary = find by title in DOM (data-id attr),
    // fallback = parse from network response.
    let result = null;
    for (let i = 0; i < 12; i++) {
      result = findPlaylistInDOMByTitle(title) || parsePlaylistIdFromResponse(responseText);
      if (result) break;
      await pause(500);
    }
    if (!result) throw new Error('playlist_id_not_found');
    console.log('[vmu] Playlist created:', result.ownerId, result.playlistId);

    // PHASE 2: Add tracks via UI.
    if (trackNames.length > 0) {
      if (typeof window.nav?.reload === 'function') {
        console.log('[vmu] SPA reload to reset APE state...');
        window.nav.reload();
        await pause(3000);
      }
      await addTracksViaEditDialog(title, result.playlistId, trackNames);
    }

    return result;
  }

  // Find edit button near a playlist cell element
  function findEditBtnInCell(cell) {
    if (!cell) return null;
    // data-testid variant (new VK)
    let btn = cell.querySelector('[data-testid="MusicPlaylistItem_OpenEditing"]');
    if (btn) return btn;
    // Class-based variants
    btn = cell.querySelector('.audio_pl__edit, .audio_pl_actions__edit, [class*="edit"]');
    if (btn) return btn;
    // aria-label containing "редакт" (case-insensitive)
    for (const b of cell.querySelectorAll('button, [role="button"]')) {
      const lbl = (b.getAttribute('aria-label') || b.title || b.textContent || '').toLowerCase();
      if (lbl.includes('редакт')) return b; // "редакт"
    }
    return null;
  }

  // Open edit dialog for the newly created playlist and select tracks by name
  async function addTracksViaEditDialog(playlistTitle, playlistId, trackNames) {
    // Wait for new playlist to appear in DOM (VK SPA updates after save)
    let editBtn = null;
    for (let i = 0; i < 20; i++) {
      await pause(600);

      // Find by title text
      for (const el of document.querySelectorAll('[data-testid="MusicPlaylistItem_Title"], .audio_pl__title, .audio_pl_item__title')) {
        if (el.textContent?.trim().includes(playlistTitle.substring(0, 8))) {
          const cell = el.closest('[data-testid="MusicPlaylistItem_Cell"], .audio_pl_item, .audio_pl') || el.parentElement?.parentElement;
          const btn = findEditBtnInCell(cell);
          if (btn) { editBtn = btn; break; }
        }
      }
      if (editBtn) break;

      // Fallback: find by playlist ID in any link/href
      for (const link of document.querySelectorAll('[href*="_' + playlistId + '"]')) {
        const cell = link.closest('[data-testid="MusicPlaylistItem_Cell"], .audio_pl_item, .audio_pl') || link.parentElement?.parentElement;
        const btn = findEditBtnInCell(cell);
        if (btn) { editBtn = btn; break; }
      }
      if (editBtn) break;
    }

    if (!editBtn) {
      console.warn('[vmu] addTracksViaEditDialog: edit button not found for playlist', playlistId, playlistTitle);
      return;
    }

    console.log('[vmu] Opening edit dialog for playlist', playlistId);
    editBtn.click();
    const dialog = await waitForEl('.audio_pl_edit_box', 4000);
    if (!dialog) { console.warn('[vmu] Edit dialog not opened'); return; }

    // Give VK time to attach event handlers to the dialog
    await pause(1000);

    // Must click ape_add_audios_btn to reveal ._ape_audio_item list.
    const addAudiosBtn = dialog.querySelector('.ape_add_audios_btn');
    if (!addAudiosBtn) { console.warn('[vmu] ape_add_audios_btn not found in edit dialog'); return; }

    let items = [];
    for (let attempt = 1; attempt <= 3; attempt++) {
      console.log('[vmu] ape_add_audios_btn click attempt', attempt);
      addAudiosBtn.click();
      items = await waitForItems(dialog, 1, 3000);
      if (items.length > 0) { console.log('[vmu] Items appeared after attempt', attempt); break; }
      if (attempt < 3) await pause(1000);
    }

    if (!items.length) { console.warn('[vmu] No ._ape_audio_item after 3 attempts'); return; }
    console.log('[vmu] Found', items.length, '._ape_audio_item items, matching by name...');

    // Match tracks by name instead of just taking first N
    await selectTracksByName(items, trackNames);

    await pause(300);

    // Set up capture for the audio.editPlaylist call that fires on save
    const editPlaylistBody = await new Promise((resolve) => {
      __editPlaylistCapture = (body) => resolve(body);
      // Also set a timeout in case VK uses the old al_audio.php path
      setTimeout(() => resolve(null), 10000);

      // Must click Сохранить — without it tracks are NOT persisted
      const boxLayout = dialog.closest('.box_layout') || dialog.closest('.box_body')?.parentElement;
      const saveBtn = (boxLayout || document).querySelector('.FlatButton--primary') || document.querySelector('.FlatButton--primary');
      if (!saveBtn) { console.warn('[vmu] Save button not found in edit dialog'); __editPlaylistCapture = null; resolve(null); return; }
      console.log('[vmu] Clicking Save');
      saveBtn.click();
    });

    await pause(1500);

    // Reorder tracks to match album order
    if (editPlaylistBody && trackNames.length > 1) {
      await reorderPlaylistTracks(editPlaylistBody, trackNames);
      await pause(1000);
    }
  }

  // Wait for ._ape_audio_item elements to appear in dialog
  function waitForItems(dialog, minCount, timeout) {
    return new Promise(resolve => {
      const check = () => {
        const items = [...dialog.querySelectorAll('._ape_audio_item')];
        if (items.length >= minCount || items.length > 0) { resolve(items); return; }
        const obs = new MutationObserver(() => {
          const found = [...dialog.querySelectorAll('._ape_audio_item')];
          if (found.length > 0) { obs.disconnect(); clearTimeout(t); resolve(found); }
        });
        obs.observe(dialog, { childList: true, subtree: true });
        const t = setTimeout(() => { obs.disconnect(); resolve([...dialog.querySelectorAll('._ape_audio_item')]); }, timeout);
      };
      check();
    });
  }

  function normalizeStr(s) {
    return (s || '').toLowerCase().replace(/[\s\-–—_.,:;!?'"()[\]{}]+/g, ' ').trim();
  }

  function getItemText(item) {
    const titleEl = item.querySelector('.audio_row__title_inner, .ai_title, [class*="audio_title"]');
    const artistEl = item.querySelector('.audio_row__performers, .ai_artist, [class*="audio_artist"]');
    const title = normalizeStr(titleEl?.textContent || '');
    const artist = normalizeStr(artistEl?.textContent || '');
    // Also try full text if selectors don't match
    const fullText = normalizeStr(item.textContent || '');
    return { title, artist, fullText };
  }

  function trackMatchesItem(trackName, itemText) {
    const tTitle = normalizeStr(trackName.title);
    const tArtist = normalizeStr(trackName.artist);

    // Exact match on title
    if (tTitle && itemText.title && itemText.title.includes(tTitle)) return true;
    if (tTitle && itemText.fullText.includes(tTitle)) {
      // If we have artist, verify it too
      if (tArtist) {
        return itemText.artist.includes(tArtist) || itemText.fullText.includes(tArtist);
      }
      return true;
    }
    // Partial: check if significant portion of the title is in the item
    if (tTitle.length > 4) {
      const words = tTitle.split(' ').filter(w => w.length > 2);
      const matchedWords = words.filter(w => itemText.fullText.includes(w));
      if (matchedWords.length >= Math.ceil(words.length * 0.7)) return true;
    }
    return false;
  }

  // Reorder audio_ids to match trackNames order, then send audio.editPlaylist
  async function reorderPlaylistTracks(capturedBody, trackNames) {
    if (!capturedBody || !trackNames.length) return;

    // Parse captured params
    const params = {};
    capturedBody.split('&').forEach(pair => {
      const eqIdx = pair.indexOf('=');
      if (eqIdx > 0) {
        params[decodeURIComponent(pair.substring(0, eqIdx))] = decodeURIComponent(pair.substring(eqIdx + 1));
      }
    });

    const audioIdsStr = params.audio_ids;
    if (!audioIdsStr) { console.warn('[vmu] No audio_ids in captured editPlaylist call'); return; }

    // Parse audio_ids: "ownerID_audioID_hash,ownerID_audioID_hash,..."
    const audioEntries = audioIdsStr.split(',').map(entry => {
      const parts = entry.split('_');
      // Format: ownerID_audioID_hash (e.g. -206614096_456239638_b332ffe4664ae5ed82)
      return { full: entry, ownerId: parts[0], audioId: parts[1], hash: parts.slice(2).join('_') };
    });

    // We need to find the title/artist for each audio ID to match with trackNames
    // Look them up in the DOM - they were just visible in the edit dialog
    const audioIdToName = {};
    document.querySelectorAll('._audio_row').forEach(row => {
      const cls = row.className;
      const match = cls.match(/_audio_row_(-?\d+_\d+)/);
      if (match) {
        const titleEl = row.querySelector('.audio_row__title_inner, .ai_title');
        const artistEl = row.querySelector('.audio_row__performers, .ai_artist');
        audioIdToName[match[1]] = {
          title: normalizeStr(titleEl?.textContent || ''),
          artist: normalizeStr(artistEl?.textContent || ''),
          fullText: normalizeStr(row.textContent || '')
        };
      }
    });

    // Build ordered audio_ids based on trackNames
    const orderedEntries = [];
    const used = new Set();

    for (const trackName of trackNames) {
      let bestEntry = null;
      for (const entry of audioEntries) {
        const key = entry.ownerId + '_' + entry.audioId;
        if (used.has(key)) continue;
        const nameInfo = audioIdToName[key];
        if (nameInfo && trackMatchesItem(trackName, nameInfo)) {
          bestEntry = entry;
          used.add(key);
          break;
        }
      }
      if (bestEntry) orderedEntries.push(bestEntry);
    }

    // Add any remaining entries not matched (shouldn't happen, but just in case)
    for (const entry of audioEntries) {
      const key = entry.ownerId + '_' + entry.audioId;
      if (!used.has(key)) orderedEntries.push(entry);
    }

    const newAudioIds = orderedEntries.map(e => e.full).join(',');
    if (newAudioIds === audioIdsStr) {
      console.log('[vmu] Track order already correct, no reorder needed');
      return;
    }

    console.log('[vmu] Reordering tracks via audio.editPlaylist...');

    // Send the reorder request
    const formData = new URLSearchParams();
    for (const [k, v] of Object.entries(params)) {
      if (k === 'audio_ids') formData.append(k, newAudioIds);
      else formData.append(k, v);
    }

    try {
      const resp = await origFetch('https://api.vk.com/method/audio.editPlaylist?' + new URLSearchParams({v: '5.280', client_id: '6287487'}).toString(), {
        method: 'POST',
        body: formData,
      });
      const data = await resp.json();
      console.log('[vmu] Reorder result:', data.response ? 'success' : 'error', data);
    } catch (err) {
      console.error('[vmu] Reorder failed:', err);
    }
  }

  // Select tracks by matching names in album order (first track in trackNames = first clicked)
  async function selectTracksByName(items, trackNames) {
    // Build text map for all items
    const itemTexts = items.map(item => ({ item, text: getItemText(item) }));

    let selectedCount = 0;
    // Click in album order (trackNames is already in correct order)
    for (const trackName of trackNames) {
      let matched = null;
      for (const { item, text } of itemTexts) {
        if (item.classList.contains('ape_selected')) continue;
        if (trackMatchesItem(trackName, text)) {
          matched = item;
          break;
        }
      }
      if (matched) {
        const chk = matched.querySelector('.ape_check') || matched;
        chk.dispatchEvent(new MouseEvent('click', { bubbles: true, cancelable: true }));
        selectedCount++;
        await pause(100);
      } else {
        console.warn('[vmu] Track not found in list:', trackName.artist, '-', trackName.title);
      }
    }
    console.log('[vmu] Selected', selectedCount, 'of', trackNames.length, 'tracks by name');
  }

  // Primary: find newly created playlist by title in DOM.
  // MusicPlaylistItem_Cell has data-id="ownerId_playlistId" (e.g. "-206614096_32").
  function findPlaylistInDOMByTitle(title) {
    if (!title) return null;
    const prefix = title.substring(0, 12).toLowerCase();
    const ownerMatch = location.href.match(/audios(-?\d+)/);
    const ownerId = ownerMatch ? ownerMatch[1] : String(window.vk?.id || '');
    for (const cell of document.querySelectorAll('[data-testid="MusicPlaylistItem_Cell"][data-id]')) {
      const cellTitle = cell.querySelector('[data-testid="MusicPlaylistItem_Title"]')?.textContent?.trim().toLowerCase() || '';
      if (cellTitle.includes(prefix)) {
        const m = cell.getAttribute('data-id')?.match(/^([-\d]+)_(\d+)$/);
        if (m) return { ownerId: m[1], playlistId: parseInt(m[2]) };
      }
    }
    return null;
  }

  // Fallback: parse playlist ID from network response text.
  function parsePlaylistIdFromResponse(responseText) {
    const ownerMatch = location.href.match(/audios(-?\d+)/);
    const ownerId = ownerMatch ? ownerMatch[1] : String(window.vk?.id || '');
    try {
      const data = JSON.parse(responseText);
      const p1 = data?.payload?.[1];
      // Look for a key named "id", "playlist_id", or similar first
      const findById = (v, depth = 0) => {
        if (depth > 5 || !v || typeof v !== 'object') return null;
        for (const [k, val] of Object.entries(v)) {
          if ((k === 'id' || k === 'playlist_id') && typeof val === 'number' && val > 0 && val < 10_000_000) return val;
          const found = findById(val, depth + 1);
          if (found) return found;
        }
        return null;
      };
      const playlistId = findById(Array.isArray(p1) ? p1[0] : p1);
      if (playlistId) return { playlistId, ownerId };
    } catch {}
    // Last resort: find newest cell in DOM
    const first = document.querySelector('[data-testid="MusicPlaylistItem_Cell"][data-id]');
    const m = first?.getAttribute('data-id')?.match(/^([-\d]+)_(\d+)$/);
    if (m) return { ownerId: m[1], playlistId: parseInt(m[2]) };
    return null;
  }

  // ── Get recent audio IDs from page DOM ────────────────────────────────────────
  function getRecentAudioIdsFromDOM(count) {
    const ids = [];
    const seen = new Set();

    // VK audio rows: .audio_row[data-full-id="ownerId_audioId"]
    for (const el of document.querySelectorAll('[data-full-id]')) {
      const id = el.dataset.fullId;
      if (!id || seen.has(id)) continue;
      // Skip: system/fake IDs starting with -200
      const [ownerId] = id.split('_');
      if (ownerId.startsWith('-200')) continue;
      seen.add(id);
      ids.push(id);
      if (ids.length >= count) break;
    }

    // Fallback: audio links /audio{owner}_{id}
    if (!ids.length) {
      for (const link of document.querySelectorAll('a[href^="/audio"]')) {
        const m = link.href.match(/\/audio(-?\d+)_(\d+)/);
        if (!m || m[1].startsWith('-200')) continue;
        const id = `${m[1]}_${m[2]}`;
        if (!seen.has(id)) { seen.add(id); ids.push(id); }
        if (ids.length >= count) break;
      }
    }

    return ids;
  }

  // ── al_audio.php direct call (uses captured hashes) ──────────────────────────
  async function callAlAudio(act, params) {
    const hash = window.__vmuHashes[act] || window.__vmuHashes._page || getPageHashFromDOM();
    if (!hash) throw new Error('Хэш не найден');
    const body = new URLSearchParams({ act, al: '1', hash, ...params });
    const res = await origFetch('/al_audio.php', { method: 'POST', body, headers: { 'X-Requested-With': 'XMLHttpRequest' } });
    const text = await res.text();
    return JSON.parse(text);
  }

  function getPageHashFromDOM() {
    for (const s of document.querySelectorAll('script:not([src])')) {
      const m = s.textContent.match(/"hash"\s*:\s*"([a-zA-Z0-9_\-]{30,70})"/);
      if (m) return m[1];
    }
    return null;
  }

  // ── Helpers ───────────────────────────────────────────────────────────────────
  function setVal(el, value) {
    // Works for both React and old VK inputs
    const proto = el.tagName === 'TEXTAREA' ? HTMLTextAreaElement.prototype : HTMLInputElement.prototype;
    const setter = Object.getOwnPropertyDescriptor(proto, 'value')?.set;
    if (setter) setter.call(el, value); else el.value = value;
    el.dispatchEvent(new Event('input', { bubbles: true }));
    el.dispatchEvent(new Event('change', { bubbles: true }));
  }

  function waitForEl(selector, timeout) {
    return new Promise(resolve => {
      const el = document.querySelector(selector);
      if (el) { resolve(el); return; }
      const obs = new MutationObserver(() => {
        const found = document.querySelector(selector);
        if (found) { obs.disconnect(); resolve(found); }
      });
      obs.observe(document.body, { childList: true, subtree: true });
      setTimeout(() => { obs.disconnect(); resolve(null); }, timeout);
    });
  }

  // ── Cover upload via file input intercept ─────────────────────────────────────
  function injectCoverFile(coverFile) {
    const coverEl = document.querySelector('.ape_cover');
    if (!coverEl) { window.postMessage({ type: 'VK_COVER_DONE', ok: false, error: 'ape_cover не найден' }, '*'); return; }

    let intercepted = false;
    const origClick = HTMLInputElement.prototype.click;
    HTMLInputElement.prototype.click = function () {
      if (!intercepted && this.type === 'file') {
        intercepted = true;
        HTMLInputElement.prototype.click = origClick;
        const dt = new DataTransfer();
        dt.items.add(coverFile);
        this.files = dt.files;
        this.dispatchEvent(new Event('change', { bubbles: true }));
        watchCropAndConfirm();
        return;
      }
      return origClick.call(this);
    };

    coverEl.click();
    setTimeout(() => {
      if (!intercepted) { HTMLInputElement.prototype.click = origClick; window.postMessage({ type: 'VK_COVER_DONE', ok: false, error: 'input не появился' }, '*'); }
    }, 4000);
  }

  function watchCropAndConfirm() {
    let n = 0;
    const t = setInterval(() => {
      const btn = findCropSaveBtn();
      if (btn) { btn.click(); clearInterval(t); setTimeout(() => window.postMessage({ type: 'VK_COVER_DONE', ok: true }, '*'), 2000); return; }
      if (++n >= 25) { clearInterval(t); window.postMessage({ type: 'VK_COVER_DONE', ok: true }, '*'); }
    }, 400);
  }

  // ── Monitor <audio> element src for resolved CDN URLs ────────────────────────
  (function () {
    function watchAudio(el) {
      new MutationObserver(() => {
        const src = el.src;
        if (src && !src.includes('audio_api_unavailable') && src.startsWith('http')) {
          window.postMessage({ type: 'VKD_AUDIO_SRC', src }, '*');
        }
      }).observe(el, { attributes: true, attributeFilter: ['src'] });
    }
    const dObs = new MutationObserver(muts => {
      for (const m of muts) for (const n of m.addedNodes) {
        if (n.nodeName === 'AUDIO') watchAudio(n);
        else if (n.querySelectorAll) n.querySelectorAll('audio').forEach(watchAudio);
      }
    });
    const start = () => {
      document.querySelectorAll('audio').forEach(watchAudio);
      dObs.observe(document.documentElement, { childList: true, subtree: true });
    };
    document.readyState === 'loading' ? document.addEventListener('DOMContentLoaded', start) : start();
  })();
})();