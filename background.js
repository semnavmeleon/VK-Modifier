// Service worker — handles chrome.downloads calls for playlist download feature.

chrome.runtime.onMessage.addListener((msg, _sender, sendResponse) => {
  if (msg.type !== 'VKD_DOWNLOAD') return false;
  let { url, filename } = msg;

  // Normalize protocol-relative URLs (//cs*.vkuserdata.com/...)
  if (url?.startsWith('//')) url = 'https:' + url;

  if (!url?.startsWith('http') && !url?.startsWith('blob:')) {
    console.warn('[vmu] bad url:', url?.substring(0, 80));
    sendResponse({ ok: false, error: 'bad url: ' + (url?.substring(0, 60) || 'null') });
    return false;
  }

  console.log('[vmu] downloading:', url.substring(0, 100));

  chrome.downloads.download(
    {
      url,
      filename: `VK Music/${filename}`,
      conflictAction: 'uniquify',
    },
    id => {
      if (chrome.runtime.lastError) {
        const err = chrome.runtime.lastError.message;
        console.warn('[vmu] download error:', err, url.substring(0, 80));
        sendResponse({ ok: false, error: err });
      } else {
        sendResponse({ ok: true, id });
      }
    }
  );
  return true; // keep message channel open for async callback
});
