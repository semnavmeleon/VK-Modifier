// Runs at document_start to patch fetch/XHR before VK scripts fire
(function () {
  if (window.__vkMultiUploadInjected) return;
  const s = document.createElement('script');
  s.src = chrome.runtime.getURL('injected.js');
  (document.documentElement || document.head).appendChild(s);

  // ─── global drag & drop interceptor ────────────────────────────────────────
  // Registered here at document_start so our window-capture listeners run
  // before VK's own bundle attaches its window-level drag/drop handlers
  // (registration order matters for multiple listeners on the same node/phase —
  // stopPropagation alone can't stop a same-node listener registered earlier).
  // Forwards matched drops to content.js via a custom event, since content.js
  // (document_idle) owns the upload queue.
  //
  // While VK's upload dialog (.audio_add_box) is open, swallow ALL drag events
  // unconditionally — not just ones over #vmu-embedded. VK's own dragenter
  // handler reacts to the very first dragenter fired for the window (often
  // before the cursor is over our panel) by re-rendering the box back to its
  // native dropzone, destroying #vmu-embedded before 'drop' ever fires. If we
  // let that first event through, the coordinate check on 'drop' then fails
  // because #vmu-embedded no longer exists.
  let dndCounter = 0;
  for (const evt of ['dragenter', 'dragover', 'dragleave', 'drop']) {
    window.addEventListener(evt, e => {
      const box = document.querySelector('.audio_add_box') ||
        [...document.querySelectorAll('[class*="vkitInternalModalBox"]')]
          .find(m => m.getBoundingClientRect().width > 0 &&
                     (m.querySelector('input[accept*="audio"], input[accept*="mp3"]') ||
                      m.querySelector('[data-testid="UploadAudio_SelectFileButton"]')));
      if (!box) return;
      e.preventDefault();
      e.stopPropagation();
      e.stopImmediatePropagation();
      const dz = document.getElementById('vmu-dropzone');
      if (evt === 'dragenter') {
        dndCounter++;
        dz?.classList.add('vmu-over');
      } else if (evt === 'dragleave') {
        dndCounter--;
        if (dndCounter <= 0) { dndCounter = 0; dz?.classList.remove('vmu-over'); }
      } else if (evt === 'drop') {
        dndCounter = 0;
        dz?.classList.remove('vmu-over');
        window.dispatchEvent(new CustomEvent('vmu-files-dropped', { detail: { files: [...e.dataTransfer.files] } }));
      }
    }, true);
  }
})();
