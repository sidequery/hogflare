import { setupPosthog, waitForFlush } from './setup.js';

const { posthog } = await setupPosthog({
  advanced_disable_feature_flags: true,
  disable_external_dependency_loading: true,
  capture_exceptions: false,
  error_tracking: {
    exception_steps: { enabled: true },
  },
});

posthog.exceptions.addExceptionStep('opened failing checkout', {
  route: '/checkout',
});

try {
  throw new TypeError('checkout total was NaN');
} catch (error) {
  posthog.captureException(error, {
    component: 'checkout',
    severity: 'high',
  });
}

await waitForFlush();
posthog._handle_unload?.();
posthog.reset?.();
process.exit(0);
