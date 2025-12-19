import { setupPosthog, waitForFlush } from './setup.js';

async function main() {
  const { posthog } = await setupPosthog();

  posthog.group('company', 'acme-corp', {
    name: 'Acme Corporation',
    industry: 'Technology',
    employee_count: 500,
  });

  await waitForFlush();
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
