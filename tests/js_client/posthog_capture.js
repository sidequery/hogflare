const { JSDOM } = require('jsdom');

async function main() {
  const apiHost = process.env.HOGFLARE_HOST;
  if (!apiHost) {
    throw new Error('HOGFLARE_HOST must be provided');
  }

  const apiKey = process.env.HOGFLARE_API_KEY || 'phc_test_integration_key';
  const distinctId = process.env.HOGFLARE_DISTINCT_ID || 'js-integration-user';

  const dom = new JSDOM('', { url: apiHost });
  global.window = dom.window;
  global.document = dom.window.document;
  global.navigator = dom.window.navigator;
  global.self = dom.window;
  global.localStorage = dom.window.localStorage;
  global.sessionStorage = dom.window.sessionStorage;
  global.location = dom.window.location;

  if (typeof global.fetch === 'undefined') {
    global.fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));
  }
  const nativeFetch = global.fetch.bind(global);
  let lastRequestPromise = null;
  global.fetch = (input, init = {}) => {
    const url = typeof input === 'string' ? input : input.url;

    if (url.includes('/flags/')) {
      lastRequestPromise = Promise.resolve(
        new Response(
          JSON.stringify({ featureFlags: {}, errorsWhileComputingFlags: false }),
          {
            status: 200,
            headers: { 'Content-Type': 'application/json' },
          }
        )
      );
      return lastRequestPromise;
    }

    if (url.includes('/e/')) {
      const rawBody = typeof init.body === 'string' ? init.body : init.body?.toString?.();
      const eventPayload = rawBody ? JSON.parse(rawBody) : {};
      const capturePayload = {
        api_key: eventPayload.token,
        event: eventPayload.event,
        distinct_id:
          eventPayload.properties?.distinct_id ?? eventPayload.distinct_id ?? distinctId,
        properties: eventPayload.properties,
        timestamp: eventPayload.timestamp,
      };

      lastRequestPromise = nativeFetch(`${apiHost}/capture`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(capturePayload),
      });

      return lastRequestPromise;
    }

    lastRequestPromise = nativeFetch(input, init);
    return lastRequestPromise;
  };

  const { posthog } = require('posthog-js');

  posthog.init(apiKey, {
    api_host: apiHost,
    capture_pageview: false,
    autocapture: false,
    disable_persistence: true,
    request_batching: false,
    disable_compression: true,
    advanced_disable_feature_flags: true,
    disable_session_recording: true,
    disable_surveys: true,
    bootstrap: {
      distinctID: distinctId,
    },
  });

  posthog.capture('js-integration-test', {
    framework: 'integration',
    client: 'posthog-js',
  });

  if (lastRequestPromise) {
    await lastRequestPromise;
  }

  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
