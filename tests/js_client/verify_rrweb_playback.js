import { createRequire } from 'module';
import { gunzipSync, strFromU8, strToU8 } from 'fflate';
import fs from 'fs';
import path from 'path';
import { chromium } from 'playwright';

const require = createRequire(import.meta.url);

const replayEventsPath = process.env.HOGFLARE_REPLAY_EVENTS_FILE;
if (!replayEventsPath) {
  throw new Error('HOGFLARE_REPLAY_EVENTS_FILE must be provided');
}

const events = JSON.parse(fs.readFileSync(replayEventsPath, 'utf8'));
if (!Array.isArray(events) || events.length === 0) {
  throw new Error('Replay events file must contain a non-empty rrweb event array');
}

const gunzipJsonField = (value) => JSON.parse(strFromU8(gunzipSync(strToU8(value, true))));

const decodePostHogCompressedEvent = (event) => {
  const decoded = structuredClone(event);
  if (decoded?.cv !== '2024-10') {
    return decoded;
  }

  if (typeof decoded.data === 'string') {
    decoded.data = gunzipJsonField(decoded.data);
  } else if (decoded.data && typeof decoded.data === 'object') {
    for (const field of ['adds', 'attributes', 'removes', 'texts']) {
      if (typeof decoded.data[field] === 'string') {
        decoded.data[field] = gunzipJsonField(decoded.data[field]);
      }
    }
  }

  delete decoded.cv;
  return decoded;
};

const playableEvents = events.map(decodePostHogCompressedEvent);
const eventTypes = new Set(playableEvents.map((event) => event?.type));
if (!eventTypes.has(2)) {
  throw new Error(`Replay events are missing an rrweb FullSnapshot event. types=${[...eventTypes].join(',')}`);
}

const rrwebPackage = path.dirname(require.resolve('rrweb/package.json'));
const rrwebPlayerPackage = path.dirname(require.resolve('rrweb-player/package.json'));

const browser = await chromium.launch({ headless: true });
const page = await browser.newPage({ viewport: { width: 1000, height: 720 } });
const errors = [];
page.on('console', (message) => {
  if (message.type() === 'error') {
    errors.push(message.text());
  }
});
page.on('pageerror', (error) => errors.push(error.message));

await page.setContent('<!doctype html><html><body><div id="player"></div></body></html>');
await page.addStyleTag({ path: path.join(rrwebPackage, 'dist', 'rrweb.min.css') });
await page.addStyleTag({ path: path.join(rrwebPlayerPackage, 'dist', 'style.css') });
await page.addScriptTag({ path: path.join(rrwebPackage, 'dist', 'rrweb-all.min.js') });
await page.addScriptTag({ path: path.join(rrwebPlayerPackage, 'dist', 'index.js') });

await page.evaluate((replayEvents) => {
  window.__hogflarePlayer = new window.rrwebPlayer({
    target: document.getElementById('player'),
    props: {
      events: replayEvents,
      autoPlay: false,
      showController: true,
      width: 900,
      height: 560,
    },
  });
}, playableEvents);

await page.waitForSelector('.rr-player', { timeout: 10000 });
await page.waitForTimeout(500);

const duration = playableEvents[playableEvents.length - 1].timestamp - playableEvents[0].timestamp;
await page.evaluate((timeOffset) => {
  window.__hogflarePlayer.goto(Math.max(0, timeOffset), false);
}, duration);
await page.waitForTimeout(500);

const result = await page.evaluate(() => {
  const iframe = document.querySelector('iframe');
  const replayText = iframe?.contentDocument?.body?.innerText || '';
  const playerBox = document.querySelector('.rr-player')?.getBoundingClientRect();

  return {
    replayText,
    playerWidth: playerBox?.width || 0,
    playerHeight: playerBox?.height || 0,
    iframeCount: document.querySelectorAll('iframe').length,
  };
});

await browser.close();

if (errors.length > 0) {
  throw new Error(`rrweb-player emitted errors: ${JSON.stringify(errors)}`);
}

if (!result.replayText.includes('Replay Ready')) {
  throw new Error(`Replay frame did not render the recorded DOM. text=${JSON.stringify(result.replayText)}`);
}

if (result.playerWidth <= 0 || result.playerHeight <= 0 || result.iframeCount === 0) {
  throw new Error(`rrweb-player did not render a visible replay surface: ${JSON.stringify(result)}`);
}

console.log(
  JSON.stringify({
    eventCount: playableEvents.length,
    compressedEventCount: events.filter((event) => event?.cv === '2024-10').length,
    eventTypes: [...eventTypes].sort(),
    replayText: result.replayText,
    playerWidth: result.playerWidth,
    playerHeight: result.playerHeight,
  }),
);
