import { setupPosthog, waitForFlush } from './setup.js';

async function main() {
  const { posthog } = await setupPosthog();

  posthog.identify('identified-user-123', {
    email: 'test@example.com',
    name: 'Test User',
    plan: 'enterprise',
  });

  await waitForFlush();
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
