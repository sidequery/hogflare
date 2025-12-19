import { setupPosthog, waitForFlush } from './setup.js';

async function main() {
  const { posthog } = await setupPosthog();

  // Simulate a user journey
  posthog.capture('page_view', { path: '/home' });
  posthog.capture('button_click', { button: 'signup' });
  posthog.capture('form_submit', { form: 'registration' });
  posthog.capture('signup_complete', { method: 'email' });

  await waitForFlush(1000); // Wait a bit longer for multiple events
  process.exit(0);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
