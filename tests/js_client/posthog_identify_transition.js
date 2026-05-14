import { setupPosthog, waitForFlush } from './setup.js';

async function main() {
  const identifiedId = process.env.HOGFLARE_IDENTIFIED_ID || 'js-identified-transition-user';
  const { posthog } = await setupPosthog();

  posthog.capture('js-anon-pageview', {
    client: 'posthog-js',
    phase: 'anonymous',
    $set: {
      initial_referrer: 'adwords',
      anon_trait: 'curious',
    },
    $set_once: {
      first_seen_source: 'landing-page',
    },
  });

  await waitForFlush();

  posthog.identify(
    identifiedId,
    {
      email: 'js-transition@example.com',
      plan: 'pro',
    },
    {
      signup_source: 'product',
    },
  );

  await waitForFlush();

  posthog.capture('js-identified-action', {
    client: 'posthog-js',
    phase: 'identified',
    button: 'checkout',
  });

  await waitForFlush();
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
