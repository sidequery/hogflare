import { JSDOM } from 'jsdom';
import { waitForFlush } from './setup.js';

const apiHost = process.env.HOGFLARE_HOST;
if (!apiHost) {
  throw new Error('HOGFLARE_HOST must be provided');
}

const apiKey = process.env.HOGFLARE_API_KEY || 'phc_test_integration_key';
const distinctId = process.env.HOGFLARE_DISTINCT_ID || 'js-auto-exception-user';

const dom = new JSDOM('', {
  url: apiHost,
  runScripts: 'dangerously',
  resources: 'usable',
});
global.window = dom.window;
global.document = dom.window.document;
global.navigator = dom.window.navigator;
global.self = dom.window;
global.localStorage = dom.window.localStorage;
global.sessionStorage = dom.window.sessionStorage;
global.location = dom.window.location;

const { posthog } = await import('posthog-js');

let loadedResolve;
const loadedPromise = new Promise((resolve) => {
  loadedResolve = resolve;
});

posthog.init(apiKey, {
  api_host: apiHost,
  capture_pageview: false,
  autocapture: false,
  disable_persistence: true,
  request_batching: false,
  disable_compression: true,
  disable_session_recording: true,
  disable_surveys: true,
  disable_external_dependency_loading: false,
  bootstrap: {
    distinctID: distinctId,
  },
  loaded: () => loadedResolve(),
});

await loadedPromise;

const instrumentationDeadline = Date.now() + 5000;
while (!dom.window.onerror?.__POSTHOG_INSTRUMENTED__ && Date.now() < instrumentationDeadline) {
  await new Promise((resolve) => setTimeout(resolve, 20));
}
if (!dom.window.onerror?.__POSTHOG_INSTRUMENTED__) {
  throw new Error('posthog-js did not load and install the exception autocapture helper');
}

const error = new dom.window.RangeError('auto captured checkout failure');
dom.window.onerror('auto captured checkout failure', 'https://example.test/checkout.js', 12, 4, error);

await waitForFlush();
posthog._handle_unload?.();
posthog.reset?.();
process.exit(0);
