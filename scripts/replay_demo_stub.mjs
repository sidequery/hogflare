const port = Number(process.env.REPLAY_DEMO_PORT || 4666);

let nextNodeId = 1;

function id() {
  return nextNodeId++;
}

function text(textContent) {
  return {
    type: 3,
    textContent,
    id: id(),
  };
}

function textWithId(nodeId, textContent) {
  return {
    type: 3,
    textContent,
    id: nodeId,
  };
}

function element(tagName, attributes = {}, childNodes = []) {
  return {
    type: 2,
    tagName,
    attributes,
    childNodes,
    id: id(),
  };
}

function elementWithId(nodeId, tagName, attributes = {}, childNodes = []) {
  return {
    type: 2,
    tagName,
    attributes,
    childNodes,
    id: nodeId,
  };
}

const css = `
  * { box-sizing: border-box; }
  body {
    margin: 0;
    min-height: 100vh;
    background: #f4f7fb;
    color: #101418;
    font-family: Inter, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
  }
  .shell {
    display: grid;
    min-height: 100vh;
    grid-template-rows: 64px 1fr;
  }
  .top {
    display: flex;
    align-items: center;
    justify-content: space-between;
    border-bottom: 1px solid #d9e1ea;
    background: #ffffff;
    padding: 0 32px;
  }
  .brand {
    font-size: 18px;
    font-weight: 760;
  }
  .nav {
    display: flex;
    gap: 24px;
    color: #627081;
    font-size: 14px;
    font-weight: 650;
  }
  .content {
    display: grid;
    grid-template-columns: minmax(420px, 0.95fr) minmax(430px, 0.75fr);
    gap: 28px;
    padding: 32px;
  }
  .hero, .checkout {
    border: 1px solid #d9e1ea;
    border-radius: 8px;
    background: #ffffff;
    box-shadow: 0 20px 60px rgba(30, 44, 60, 0.08);
  }
  .hero {
    padding: 34px;
  }
  .eyebrow {
    color: #087a55;
    font-size: 12px;
    font-weight: 760;
    letter-spacing: 0.04em;
    text-transform: uppercase;
  }
  h1 {
    max-width: 700px;
    margin: 14px 0 14px;
    font-size: 44px;
    line-height: 1.02;
    letter-spacing: 0;
  }
  .lede {
    max-width: 640px;
    color: #5f6d7a;
    font-size: 17px;
    line-height: 1.5;
  }
  .plans {
    display: grid;
    grid-template-columns: repeat(3, minmax(0, 1fr));
    gap: 14px;
    margin-top: 28px;
  }
  .plan {
    min-height: 164px;
    border: 1px solid #d9e1ea;
    border-radius: 8px;
    padding: 18px;
  }
  .plan.active {
    border-color: #087a55;
    background: #e8f6ef;
  }
  .plan strong {
    display: block;
    font-size: 15px;
  }
  .price {
    margin: 12px 0;
    font-size: 28px;
    font-weight: 760;
  }
  .plan span {
    color: #627081;
    font-size: 13px;
    line-height: 1.45;
  }
  .checkout {
    align-self: start;
    padding: 24px;
  }
  .checkout h2 {
    margin: 0 0 6px;
    font-size: 22px;
  }
  .checkout p {
    margin: 0 0 22px;
    color: #627081;
  }
  .notice {
    margin: 0 0 16px;
    border: 1px solid #c7d5e4;
    border-radius: 6px;
    background: #f4f8fc;
    color: #4d5965;
    padding: 10px 12px;
    font-size: 13px;
    font-weight: 650;
  }
  .notice.active {
    border-color: #d68b21;
    background: #fff5e6;
    color: #804d08;
  }
  .field {
    display: grid;
    gap: 6px;
    margin-bottom: 14px;
  }
  label {
    color: #4d5965;
    font-size: 12px;
    font-weight: 720;
  }
  input {
    height: 42px;
    border: 1px solid #ccd6e0;
    border-radius: 6px;
    padding: 0 12px;
    color: #101418;
    font: inherit;
  }
  .cta {
    width: 100%;
    height: 44px;
    border: 0;
    border-radius: 6px;
    background: #111827;
    color: #ffffff;
    font-size: 14px;
    font-weight: 760;
  }
  .cta[disabled] {
    background: #6b7280;
  }
  .summary {
    display: grid;
    gap: 9px;
    margin-top: 18px;
    border-top: 1px solid #d9e1ea;
    padding-top: 18px;
    color: #627081;
    font-size: 13px;
  }
  .summary div {
    display: flex;
    justify-content: space-between;
  }
  .summary strong {
    color: #101418;
  }
`;

const emailInputId = id();
const companyInputId = id();
const noticeId = id();
const noticeTextId = id();
const ctaButtonId = id();
const ctaTextId = id();

function snapshotNode() {
  return {
    type: 0,
    childNodes: [
      {
        type: 1,
        name: "html",
        publicId: "",
        systemId: "",
        id: id(),
      },
      element("html", { lang: "en" }, [
        element("head", {}, [
          element("title", {}, [text("Acme Analytics Checkout")]),
          element("style", {}, [text(css)]),
        ]),
        element("body", {}, [
          element("div", { class: "shell" }, [
            element("header", { class: "top" }, [
              element("div", { class: "brand" }, [text("Acme Analytics")]),
              element("nav", { class: "nav" }, [
                element("span", {}, [text("Pricing")]),
                element("span", {}, [text("Docs")]),
                element("span", {}, [text("Support")]),
              ]),
            ]),
            element("main", { class: "content" }, [
              element("section", { class: "hero" }, [
                element("div", { class: "eyebrow" }, [text("Replay demo account")]),
                element("h1", {}, [text("Understand every product moment without guessing.")]),
                element("p", { class: "lede" }, [
                  text(
                    "This is a real rrweb snapshot used by the Hogflare demo, with pricing cards, a checkout form, and recorded interactions."
                  ),
                ]),
                element("div", { class: "plans" }, [
                  element("article", { class: "plan" }, [
                    element("strong", {}, [text("Starter")]),
                    element("div", { class: "price" }, [text("$49")]),
                    element("span", {}, [text("Basic ingestion and replay for smaller teams.")]),
                  ]),
                  element("article", { class: "plan active" }, [
                    element("strong", {}, [text("Pro")]),
                    element("div", { class: "price" }, [text("$149")]),
                    element("span", {}, [text("Event search, funnels, replay, and friction signals.")]),
                  ]),
                  element("article", { class: "plan" }, [
                    element("strong", {}, [text("Scale")]),
                    element("div", { class: "price" }, [text("Custom")]),
                    element("span", {}, [text("Warehouse-first analytics for high-volume teams.")]),
                  ]),
                ]),
              ]),
              element("aside", { class: "checkout" }, [
                element("h2", {}, [text("Start Pro trial")]),
                element("p", {}, [text("Recorded checkout path for replay verification.")]),
                elementWithId(noticeId, "div", { class: "notice" }, [
                  textWithId(noticeTextId, "Ready. Fill the form to continue."),
                ]),
                element("div", { class: "field" }, [
                  element("label", { for: "email" }, [text("Work email")]),
                  {
                    type: 2,
                    tagName: "input",
                    attributes: {
                      id: "email",
                      type: "email",
                      value: "",
                      placeholder: "you@company.com",
                    },
                    childNodes: [],
                    id: emailInputId,
                  },
                ]),
                element("div", { class: "field" }, [
                  element("label", { for: "company" }, [text("Company")]),
                  {
                    type: 2,
                    tagName: "input",
                    attributes: {
                      id: "company",
                      type: "text",
                      value: "",
                      placeholder: "Company name",
                    },
                    childNodes: [],
                    id: companyInputId,
                  },
                ]),
                elementWithId(ctaButtonId, "button", { class: "cta" }, [
                  textWithId(ctaTextId, "Continue to payment"),
                ]),
                element("div", { class: "summary" }, [
                  element("div", {}, [element("span", {}, [text("Plan")]), element("strong", {}, [text("Pro")])]),
                  element("div", {}, [element("span", {}, [text("Seats")]), element("strong", {}, [text("8")])]),
                  element("div", {}, [element("span", {}, [text("Due today")]), element("strong", {}, [text("$0")])]),
                ]),
              ]),
            ]),
          ]),
        ]),
      ]),
    ],
    id: id(),
  };
}

const demoEvents = [
  {
    type: 4,
    timestamp: 1_000,
    data: {
      href: "https://app.test/pricing",
      width: 1365,
      height: 768,
    },
  },
  {
    type: 2,
    timestamp: 1_100,
    data: {
      node: snapshotNode(),
      initialOffset: { left: 0, top: 0 },
    },
  },
  { type: 3, timestamp: 1_700, data: { source: 2, type: 5, x: 815, y: 255 } },
  { type: 3, timestamp: 2_100, data: { source: 5, id: emailInputId, text: "nico@atm.com", isChecked: false } },
  { type: 3, timestamp: 2_700, data: { source: 5, id: companyInputId, text: "ATM.COM", isChecked: false } },
  { type: 3, timestamp: 3_400, data: { source: 2, type: 2, x: 948, y: 517 } },
  {
    type: 3,
    timestamp: 3_650,
    data: {
      source: 0,
      texts: [{ id: ctaTextId, value: "Checking workspace..." }],
      attributes: [{ id: ctaButtonId, attributes: { class: "cta", disabled: "true" } }],
      removes: [],
      adds: [],
    },
  },
  { type: 3, timestamp: 4_050, data: { source: 2, type: 2, x: 950, y: 518 } },
  { type: 3, timestamp: 4_450, data: { source: 2, type: 2, x: 951, y: 516 } },
  {
    type: 3,
    timestamp: 4_950,
    data: {
      source: 0,
      texts: [
        { id: noticeTextId, value: "Payment method missing. Add card details to continue." },
        { id: ctaTextId, value: "Continue to payment" },
      ],
      attributes: [
        { id: noticeId, attributes: { class: "notice active" } },
        { id: ctaButtonId, attributes: { class: "cta" } },
      ],
      removes: [],
      adds: [],
    },
  },
  { type: 3, timestamp: 6_200, data: { source: 3, x: 0, y: 720 } },
];

const journeyEvents = [
  { type: 4, timestamp: 1_000, data: { href: "https://app.test/home", width: 1365, height: 768 } },
  {
    type: 2,
    timestamp: 1_100,
    data: {
      node: snapshotNode(),
      initialOffset: { left: 0, top: 0 },
    },
  },
  { type: 3, timestamp: 1_700, data: { source: 3, x: 0, y: 420 } },
];

const rows = [
  {
    uuid: "demo-chunk-1",
    event: "$snapshot",
    distinct_id: "replay-user",
    created_at: "2026-05-22T10:00:01Z",
    api_key: "phc_demo",
    properties: {
      session_id: "demo-session-1",
      events: demoEvents,
    },
  },
  {
    uuid: "demo-chunk-2",
    event: "$snapshot",
    distinct_id: "journey-user",
    created_at: "2026-05-22T10:03:00Z",
    api_key: "phc_demo",
    properties: {
      session_id: "journey-session",
      events: journeyEvents,
    },
  },
  {
    uuid: "event-pricing",
    event: "Viewed Pricing",
    distinct_id: "replay-user",
    created_at: "2026-05-22T10:00:02Z",
    api_key: "phc_demo",
    properties: {
      "$session_id": "demo-session-1",
      "$current_url": "https://app.test/pricing",
      plan: "pro",
    },
  },
  {
    uuid: "event-checkout",
    event: "Checkout Started",
    distinct_id: "replay-user",
    created_at: "2026-05-22T10:00:05Z",
    api_key: "phc_demo",
    properties: {
      "$session_id": "demo-session-1",
      "$current_url": "https://app.test/checkout",
      plan: "pro",
    },
  },
  {
    uuid: "event-stuck-1",
    event: "Viewed Pricing",
    distinct_id: "stuck-user",
    created_at: "2026-05-22T10:01:00Z",
    api_key: "phc_demo",
    properties: {
      "$session_id": "stuck-session",
      "$current_url": "https://app.test/pricing",
    },
  },
  {
    uuid: "event-stuck-2",
    event: "Viewed Pricing",
    distinct_id: "stuck-user",
    created_at: "2026-05-22T10:01:03Z",
    api_key: "phc_demo",
    properties: {
      "$session_id": "stuck-session",
      "$current_url": "https://app.test/pricing",
    },
  },
  {
    uuid: "event-journey",
    event: "Product Viewed",
    distinct_id: "journey-user",
    created_at: "2026-05-22T10:03:05Z",
    api_key: "phc_demo",
    properties: {
      "$session_id": "journey-session",
      "$current_url": "https://app.test/product",
      sku: "sku_123",
    },
  },
];

Bun.serve({
  port,
  async fetch(request) {
    let payload = {};
    try {
      payload = await request.json();
    } catch {}
    console.log(payload.query || "no query");
    return Response.json({ result: { rows } });
  },
});

console.log(`replay sql demo stub listening on ${port}`);
await new Promise(() => {});
