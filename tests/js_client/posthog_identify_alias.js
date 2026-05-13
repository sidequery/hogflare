import { setupPosthog, waitForFlush } from './setup.js';

async function main() {
  const anonId = process.env.HOGFLARE_ANON_DISTINCT_ID || 'anonymous-sdk-user';
  const identifiedId = process.env.HOGFLARE_IDENTIFIED_ID || 'identified-sdk-user';

  const { posthog } = await setupPosthog({
    disable_compression: false,
    request_batching: false,
    disable_persistence: false,
    bootstrap: {
      distinctID: anonId,
      isIdentifiedID: false,
    },
  });

  posthog.identify(identifiedId, {
    email: 'sdk-identify@example.com',
    plan: 'enterprise',
  });

  await waitForFlush(1500);
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
