import { PostHog } from 'posthog-node';

async function main() {
  const apiHost = process.env.HOGFLARE_HOST;
  if (!apiHost) {
    throw new Error('HOGFLARE_HOST must be provided');
  }

  const apiKey = process.env.HOGFLARE_API_KEY || 'phc_test_integration_key';
  const distinctId = process.env.HOGFLARE_DISTINCT_ID || 'node-flag-user';

  const identifyClient = new PostHog(apiKey, { host: apiHost });
  identifyClient.identify({
    distinctId,
    properties: {
      plan: 'pro',
    },
  });
  await identifyClient.shutdown();

  const flagClient = new PostHog(apiKey, { host: apiHost });
  const value = await flagClient.getFeatureFlag('pro-flag', distinctId);
  const payload = await flagClient.getFeatureFlagPayload('pro-flag', distinctId, value);
  await flagClient.shutdown();

  console.log(JSON.stringify({ value, payload }));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
