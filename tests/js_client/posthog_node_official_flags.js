import { PostHog } from 'posthog-node';

async function main() {
  const apiHost = process.env.HOGFLARE_HOST;
  if (!apiHost) {
    throw new Error('HOGFLARE_HOST must be provided');
  }

  const apiKey = process.env.HOGFLARE_API_KEY || 'phc_test_node_key';
  const distinctId = process.env.HOGFLARE_DISTINCT_ID || 'node-official-flag-user';
  const groupKey = process.env.HOGFLARE_GROUP_KEY || 'node-flags-company';

  const ingestClient = new PostHog(apiKey, { host: apiHost });
  ingestClient.groupIdentify({
    groupType: 'company',
    groupKey,
    distinctId,
    properties: {
      plan: 'enterprise',
    },
  });
  await ingestClient.shutdown();

  const flagClient = new PostHog(apiKey, { host: apiHost });
  const groups = { company: groupKey };
  const distinct = await flagClient.getFeatureFlag('sdk-distinct-flag', distinctId);
  const groupKeyFlag = await flagClient.getFeatureFlag('sdk-group-key-flag', distinctId, { groups });
  const groupPlan = await flagClient.getFeatureFlag('sdk-group-plan-flag', distinctId, { groups });
  const variant = await flagClient.getFeatureFlag('sdk-variant-flag', distinctId);
  const payload = await flagClient.getFeatureFlagPayload('sdk-variant-flag', distinctId, variant);
  await flagClient.shutdown();

  console.log(JSON.stringify({ distinct, groupKeyFlag, groupPlan, variant, payload }));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
