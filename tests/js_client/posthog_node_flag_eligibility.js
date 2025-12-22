import { PostHog } from 'posthog-node';

async function main() {
  const apiHost = process.env.HOGFLARE_HOST;
  if (!apiHost) {
    throw new Error('HOGFLARE_HOST must be provided');
  }

  const apiKey = process.env.HOGFLARE_API_KEY || 'phc_test_integration_key';
  const distinctId = process.env.HOGFLARE_DISTINCT_ID || 'node-eligibility-user';

  const preClient = new PostHog(apiKey, { host: apiHost });
  const before = await preClient.getFeatureFlag('eligible-flag', distinctId);
  await preClient.shutdown();

  const captureClient = new PostHog(apiKey, { host: apiHost });
  captureClient.capture({
    distinctId,
    event: 'eligibility-event',
    properties: {
      $set: {
        plan: 'pro',
      },
    },
  });
  await captureClient.shutdown();

  const postClient = new PostHog(apiKey, { host: apiHost });
  const after = await postClient.getFeatureFlag('eligible-flag', distinctId);
  const payload = await postClient.getFeatureFlagPayload('eligible-flag', distinctId, after);
  await postClient.shutdown();

  console.log(JSON.stringify({ before, after, payload }));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
