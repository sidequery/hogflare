import { PostHog } from 'posthog-node';

async function main() {
  const apiHost = process.env.HOGFLARE_HOST;
  if (!apiHost) {
    throw new Error('HOGFLARE_HOST must be provided');
  }

  const apiKey = process.env.HOGFLARE_API_KEY || 'phc_test_node_key';
  const distinctId = process.env.HOGFLARE_DISTINCT_ID || 'node-group-user';
  const client = new PostHog(apiKey, { host: apiHost });

  client.groupIdentify({
    groupType: 'company',
    groupKey: 'node-acme',
    distinctId,
    properties: {
      plan: 'enterprise',
      seats: 12,
    },
  });

  client.capture({
    distinctId,
    event: 'node-grouped-capture',
    groups: { company: 'node-acme' },
    properties: {
      client: 'posthog-node',
    },
  });

  await client.shutdown();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
