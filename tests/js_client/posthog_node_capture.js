import { PostHog } from 'posthog-node';

async function main() {
  const apiHost = process.env.HOGFLARE_HOST;
  if (!apiHost) {
    throw new Error('HOGFLARE_HOST must be provided');
  }

  const apiKey = process.env.HOGFLARE_API_KEY || 'phc_test_integration_key';
  const distinctId = process.env.HOGFLARE_DISTINCT_ID || 'node-integration-user';

  const client = new PostHog(apiKey, { host: apiHost });

  client.capture({
    distinctId,
    event: 'node-integration-test',
    properties: {
      framework: 'integration',
      client: 'posthog-node',
    },
  });

  await client.shutdown();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
