/**
 * CHG-002: the browser-side cleaner behaves like the ingestion cleaner.
 *
 * Loads web/vendor/purify.min.js and web/html_allowlist.js into a jsdom
 * window, then the CHG-002 block of web/app.js (purifier, cleanStoredHtml,
 * descriptionFrame), and runs the same removal / retention cases as
 * tests/test_html_clean.py plus the sandbox attributes of the frame.
 *
 *   cd tests/js && npm install && npm test
 */
"use strict";
const test = require("node:test");
const assert = require("node:assert/strict");
const fs = require("node:fs");
const path = require("node:path");
const vm = require("node:vm");
const { JSDOM } = require("jsdom");

const ROOT = path.resolve(__dirname, "..", "..");
const read = (p) => fs.readFileSync(path.join(ROOT, p), "utf8");

function makeWindow() {
  const dom = new JSDOM("<!doctype html><html><body><div id='descBox' class='desc-box'></div></body></html>",
                        { runScripts: "outside-only" });
  const w = dom.window;
  w.eval(read("web/vendor/purify.min.js"));
  w.eval(read("web/html_allowlist.js"));
  // the CHG-002 block of app.js, and the one helper it uses
  const app = read("web/app.js");
  const start = app.indexOf("const HTML_DISPLAY_LIMIT");
  const end = app.indexOf("const DISPOSITIONS");
  assert.ok(start > 0 && end > start, "CHG-002 block not found in app.js");
  w.eval('function esc(s){return String(s==null?"":s).replace(/[&<>"\']/g,c=>({"&":"&amp;","<":"&lt;",">":"&gt;",\'"\':"&quot;","\'":"&#39;"}[c]));}');
  w.eval("const $ = (sel, root) => (root || document).querySelector(sel);");
  w.eval(app.slice(start, end));
  return w;
}

const W = makeWindow();
const clean = (h) => W.cleanStoredHtml(h).html;

test("sanitiser and allowlist are loaded", () => {
  assert.ok(W.DOMPurify && W.DOMPurify.isSupported);
  assert.ok(Array.isArray(W.HTML_ALLOWLIST.tags));
  assert.ok(W.purifier(), "purifier() must return an instance");
});

const REMOVED = [
  ['<img src=x onerror=alert(1)>', "onerror"],
  ['<svg onload=alert(1)><circle/></svg>', "svg"],
  ['<a href="&#106;avascript:alert(1)">x</a>', "href"],
  ['<a href="javascript:alert(1)">x</a>', "href"],
  ['<a href="data:text/html,hi">x</a>', "href"],
  ['<a href="data:image/png;base64,AAAA">x</a>', "href"],
  ['<img src="data:text/html,hi">', "src"],
  ['<img src="data:image/svg+xml;base64,PHN2Zz4=">', "src"],
  ['<a href="https://x" target="_top">x</a>', "_top"],
  ['<iframe src="https://x"></iframe>', "iframe"],
  ['<form action="/p"><input name=q></form>', "form"],
  ['<form action="/p"><input name=q></form>', "input"],
  ['<button onclick="go()">Go</button>', "button"],
  ['<object data="x.swf"></object>', "object"],
  ['<embed src="x.swf">', "embed"],
  ['<meta http-equiv="refresh" content="0;url=x">', "meta"],
  ['<link rel="stylesheet" href="x.css">', "link"],
  ['<base href="https://evil/">', "base"],
  ['<div onmouseover="x()">t</div>', "onmouseover"],
  ['<p>a<!-- hidden <script>x</script> -->b</p>', "<!--"],
  ['<script>bad()</script><p>t</p>', "bad()"],
  ['<style>p{display:none}</style><p>t</p>', "display:none"],
  ['<noscript>fallback</noscript><p>t</p>', "fallback"],
  ['<h3 onclick="toggle()">Article One</h3>', "onclick"],
  ['<div data-bs-toggle="collapse" aria-expanded="true" role="tab">t</div>', "data-bs-toggle"],
  ['<div data-bs-toggle="collapse" aria-expanded="true" role="tab">t</div>', "aria-expanded"],
  ['<img src="x.png" onerror="alert(1)" alt="a">', "onerror"],
  ['<td colspan="2" onclick="x()">c</td>', "onclick"],
  ['<p id="a" colspan="2">t</p>', "colspan"],        // allowed on td, not on p
];
for (const [fragment, gone] of REMOVED) {
  test(`removes ${gone} from ${fragment.slice(0, 40)}`, () => {
    assert.ok(!clean(fragment).includes(gone), clean(fragment));
  });
}

const KEPT = [
  ['<a href="https://a.b/c.pdf">pdf</a>', 'href="https://a.b/c.pdf"'],
  ['<a href="/sites/f.pdf">rel</a>', 'href="/sites/f.pdf"'],
  ['<a href="mailto:x@y.z">m</a>', 'href="mailto:x@y.z"'],
  ['<a href="#s2">anchor</a>', 'href="#s2"'],
  ['<img src="https://a/b.png" alt="fig" width="3">', 'alt="fig"'],
  ['<img src="data:image/png;base64,iVBORw0KGgo=">', 'src="data:image/png;base64,iVBORw0KGgo="'],
  ['<img src="data:image/jpeg;base64,/9j/4AAQ">', 'src="data:image/jpeg;base64,/9j/4AAQ"'],
  ['<table border="1"><tr><td colspan="2">c</td></tr></table>', 'colspan="2"'],
  ['<table bordercolor="#ccc"><tr><td>c</td></tr></table>', 'bordercolor="#ccc"'],
  ['<ul><li>i</li></ul><ol start="3"><li>j</li></ol>', 'start="3"'],
  ['<h2 id="s1">H</h2>', 'id="s1"'],
  ['<p style="color:red" class="note">t</p>', 'style="color:red"'],
  ['<p style="color:red" class="note">t</p>', 'class="note"'],
  ['<span dir="rtl" lang="ar">t</span>', 'dir="rtl"'],
  ['<strong>b</strong><em>i</em><sup>2</sup>', "<sup>2</sup>"],
  ['<blockquote cite="https://a">q</blockquote>', 'cite="https://a"'],
  ['<form action="/p"><input name=q>This notice applies to all licensees.</form>', "This notice applies to all licensees."],
];
for (const [fragment, kept] of KEPT) {
  test(`keeps ${kept.slice(0, 40)}`, () => {
    assert.ok(clean(fragment).includes(kept), clean(fragment));
  });
}

test("every link opens in a new tab with noopener noreferrer", () => {
  const out = clean('<a href="https://x">x</a><a href="/y">y</a>');
  const links = out.match(/<a [^>]*>/g);
  assert.equal(links.length, 2);
  for (const a of links) {
    assert.ok(a.includes('target="_blank"'), a);
    assert.ok(a.includes('rel="noopener noreferrer"'), a);
  }
});

test("idempotent on dirty input", () => {
  const once = clean('<p>x</p><script>bad()</script><img src=x onerror=alert(1)>');
  assert.equal(clean(once), once);
});

test("fixtures keep their words", () => {
  const dir = path.join(ROOT, "tests", "fixtures");
  // whole document, not body: a fixture's <title> text is kept as words by
  // both cleaners (the tag goes, the words stay), same as the Python test
  const words = (h) => new JSDOM(h).window.document.documentElement.textContent.replace(/\s+/g, "");
  for (const f of fs.readdirSync(dir).filter((x) => x.endsWith(".html"))) {
    const raw = fs.readFileSync(path.join(dir, f), "utf8");
    // script/style/noscript bodies are removed on purpose (same as the fingerprint)
    const expect = words(raw.replace(/<(script|style|noscript)[\s\S]*?<\/\1>/gi, ""));
    assert.equal(words(clean(raw)), expect, f);
  }
});

test("truncation happens after cleaning and closes open tags", () => {
  const big = "<div>" + "<p>word word word</p>".repeat(20000) + "</div>";   // ~440k chars
  const r = W.cleanStoredHtml(big);
  assert.ok(r.truncated);
  const limit = 200000;
  assert.ok(read("web/app.js").includes("const HTML_DISPLAY_LIMIT = 200000;"), "limit changed: update this test");
  assert.ok(r.html.length <= limit + 50, String(r.html.length));
  assert.ok(r.html.endsWith("</div>") || r.html.endsWith("</p>"), r.html.slice(-30));
  assert.ok(!/<p>word word word$/.test(r.html), "cut mid element");
});

test("the frame is sandboxed with no scripts, no same-origin, a CSP and a base target", () => {
  const f = W.descriptionFrame("<p>hi</p>");
  const sb = f.getAttribute("sandbox");
  assert.equal(sb, "allow-popups allow-popups-to-escape-sandbox");
  assert.ok(!sb.includes("allow-scripts") && !sb.includes("allow-same-origin") && !sb.includes("allow-forms"));
  assert.equal(f.getAttribute("referrerpolicy"), "no-referrer");
  assert.ok(f.srcdoc.includes("Content-Security-Policy"));
  assert.ok(f.srcdoc.includes("default-src 'none'"));
  assert.ok(f.srcdoc.includes("img-src https: http: data:"));
  assert.ok(f.srcdoc.includes('<base target="_blank">'));
  assert.ok(f.srcdoc.includes("<p>hi</p>"));
});

test("mountDescription: empty row, frame, and text-only fallback without a sanitiser", () => {
  const box = W.document.getElementById("descBox");
  W.mountDescription(box, { document_html: "" });
  assert.ok(box.textContent.includes("No stored document content"));

  W.mountDescription(box, { document_html: '<p>ok</p><script>x()</script>' });
  const frame = box.querySelector("iframe.desc-frame");
  assert.ok(frame && box.classList.contains("has-frame"));
  assert.ok(frame.srcdoc.includes("<p>ok</p>") && !frame.srcdoc.includes("x()"));

  const W2 = makeWindow();
  W2.eval("window.DOMPurify = undefined;");
  const box2 = W2.document.getElementById("descBox");
  W2.mountDescription(box2, { document_html: '<p>words</p><img src=x onerror=alert(1)>' });
  assert.ok(!box2.querySelector("iframe") && !box2.querySelector("img"));
  assert.ok(box2.textContent.includes("words") && box2.textContent.includes("Sanitiser not loaded"));
});
