import { JSDOM } from 'jsdom';

export async function setupPosthog(options = {}) {
  const apiHost = process.env.HOGFLARE_HOST;
  if (!apiHost) {
    throw new Error('HOGFLARE_HOST must be provided');
  }

  const apiKey = process.env.HOGFLARE_API_KEY || 'phc_test_integration_key';
  const distinctId = process.env.HOGFLARE_DISTINCT_ID || 'js-integration-user';

  // Set up browser globals for posthog-js
  const dom = new JSDOM('', { url: apiHost });
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
    bootstrap: {
      distinctID: distinctId,
    },
    on_request_error: (err) => {
      console.error('PostHog request error:', err);
    },
    loaded: () => loadedResolve(),
    ...options,
  });

  await loadedPromise;

  return { posthog, apiHost, apiKey, distinctId };
}

export function waitForFlush(ms = 500) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}
