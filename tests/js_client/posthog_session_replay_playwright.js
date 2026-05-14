import { createRequire } from 'module';
import path from 'path';
import { chromium } from 'playwright';

const require = createRequire(import.meta.url);

const apiHost = process.env.HOGFLARE_HOST;
if (!apiHost) {
  throw new Error('HOGFLARE_HOST must be provided');
}

const apiKey = process.env.HOGFLARE_API_KEY || 'phc_test_replay_key';
const distinctId = process.env.HOGFLARE_DISTINCT_ID || 'js-replay-user';
const posthogPackage = path.dirname(require.resolve('posthog-js/package.json'));
const posthogBundle = path.join(posthogPackage, 'dist', 'array.full.no-external.js');

const browser = await chromium.launch({ headless: true });
const context = await browser.newContext({
  viewport: { width: 900, height: 600 },
});
const page = await context.newPage();

const consoleMessages = [];
const requests = [];
page.on('console', (message) => {
  consoleMessages.push(`${message.type()}: ${message.text()}`);
});
page.on('pageerror', (error) => {
  consoleMessages.push(`pageerror: ${error.message}`);
});
page.on('request', (request) => {
  requests.push(`${request.method()} ${request.url()}`);
});
page.on('requestfailed', (request) => {
  requests.push(`FAILED ${request.method()} ${request.url()} ${request.failure()?.errorText || ''}`);
});

let replayStatus = null;
const replayResponse = new Promise((resolve) => {
  page.on('response', (response) => {
    if (response.url().includes('/s') && response.request().method() === 'POST') {
      replayStatus = response.status();
      resolve(response.status());
    }
  });
});

await page.goto(`${apiHost}/healthz`);
await page.setContent(`
  <!doctype html>
  <html>
    <head>
      <title>Hogflare replay verification</title>
      <style>
        body { font-family: system-ui, sans-serif; margin: 32px; }
        main { width: 480px; border: 1px solid #ccd0d5; padding: 24px; }
        button { margin-top: 16px; padding: 8px 12px; }
      </style>
    </head>
    <body>
      <main id="app">
        <h1>Replay Ready</h1>
        <label>
          Name
          <input id="name" value="Initial value" />
        </label>
        <button id="record-button">Record interaction</button>
        <p id="status">Waiting for interaction</p>
      </main>
      <script>
        document.getElementById('record-button').addEventListener('click', () => {
          document.getElementById('status').textContent = 'Clicked and recorded';
          document.body.dataset.replayState = 'clicked';
        });
      </script>
    </body>
  </html>
`);
await page.addScriptTag({ path: posthogBundle });

await page.evaluate(
  ({ apiHost, apiKey, distinctId }) =>
    new Promise((resolve) => {
      window.posthog.init(apiKey, {
        api_host: apiHost,
        capture_pageview: false,
        autocapture: false,
        request_batching: false,
        disable_session_recording: false,
        disable_surveys: true,
        opt_out_useragent_filter: true,
        before_send: (event) => event,
        bootstrap: {
          distinctID: distinctId,
          isIdentifiedID: true,
        },
        loaded: () => resolve(true),
        on_request_error: (error) => {
          console.error('PostHog request error', error);
        },
      });
    }),
  { apiHost, apiKey, distinctId },
);

await page.evaluate(() => {
  window.__hogflareReplayDebug = {
    captureCalls: [],
    sendRequests: [],
  };

  const originalCapture = window.posthog.capture.bind(window.posthog);
  window.posthog.capture = (...args) => {
    const call = {
      event: args[0],
      optionKeys: Object.keys(args[2] || {}),
      url: args[2]?._url || null,
      batchKey: args[2]?._batchKey || null,
      isCapturing: window.posthog.is_capturing?.(),
      requestBatching: window.posthog.config?.request_batching,
    };
    window.__hogflareReplayDebug.captureCalls.push(call);
    const result = originalCapture(...args);
    call.returned = !!result;
    call.resultEvent = result?.event || null;
    call.resultKeys = result ? Object.keys(result) : [];
    return result;
  };

  const originalSendRequest = window.posthog._send_request.bind(window.posthog);
  window.posthog._send_request = (options) => {
    window.__hogflareReplayDebug.sendRequests.push({
      method: options?.method,
      url: options?.url,
      batchKey: options?.batchKey || null,
      compression: options?.compression || null,
      event: Array.isArray(options?.data) ? 'array' : options?.data?.event || null,
    });
    return originalSendRequest(options);
  };
});

await page.waitForFunction(
  () => window.posthog?.sessionRecording && ['active', 'buffering'].includes(window.posthog.sessionRecording.status),
  null,
  { timeout: 15000 },
);

await page.fill('#name', 'Replay input value');
await page.click('#record-button');
await page.evaluate(() => {
  const el = document.createElement('div');
  el.id = 'late-mutation';
  el.textContent = 'Late mutation marker';
  document.getElementById('app').appendChild(el);
});

await page.waitForFunction(
  () => {
    const lazyRecorder = window.posthog?.sessionRecording?._lazyLoadedSessionRecording;
    return (lazyRecorder?._buffer?.data?.length || 0) > 0;
  },
  null,
  { timeout: 10000 },
);
await page.waitForTimeout(500);

const flushResult = await page.evaluate(() => {
  const posthog = window.posthog;
  const lazyRecorder = posthog?.sessionRecording?._lazyLoadedSessionRecording;

  if (lazyRecorder && typeof lazyRecorder._flushBuffer === 'function') {
    const bufferLengthBeforeFlush = lazyRecorder._buffer?.data?.length || 0;
    lazyRecorder._flushBuffer();
    return {
      status: posthog.sessionRecording.status,
      flushedVia: '_flushBuffer',
      started: posthog.sessionRecordingStarted?.(),
      bufferLengthBeforeFlush,
      bufferLengthAfterFlush: lazyRecorder._buffer?.data?.length || 0,
      debug: window.__hogflareReplayDebug,
    };
  }

  posthog?.sessionRecording?.stopRecording?.();
  return {
    status: posthog?.sessionRecording?.status,
    flushedVia: 'stopRecording',
    started: posthog?.sessionRecordingStarted?.(),
    debug: window.__hogflareReplayDebug,
  };
});

const status = await Promise.race([
  replayResponse,
  page.waitForTimeout(15000).then(() => null),
]);

await browser.close();

if (status === null || replayStatus === null || replayStatus >= 400) {
  throw new Error(
    `Replay upload did not complete successfully. replayStatus=${replayStatus}, flush=${JSON.stringify(
      flushResult,
    )}, requests=${JSON.stringify(requests)}, console=${JSON.stringify(consoleMessages)}`,
  );
}

console.log(
  JSON.stringify({
    replayStatus,
    flushResult,
    requests,
    consoleMessages,
  }),
);
