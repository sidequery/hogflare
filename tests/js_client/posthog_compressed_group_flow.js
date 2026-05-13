import { setupPosthog, waitForFlush } from './setup.js';

async function main() {
  const { posthog } = await setupPosthog({
    disable_compression: false,
    request_batching: false,
  });

  posthog.group('company', 'sdk-acme', {
    plan: 'enterprise',
    seats: 42,
  });

  await waitForFlush(750);

  posthog.capture('js-grouped-capture', {
    client: 'posthog-js',
    action: 'grouped',
  });

  await waitForFlush(1500);
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
