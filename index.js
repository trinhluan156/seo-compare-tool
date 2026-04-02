const express = require("express");
const cors = require("cors");
const multer = require("multer");
const Diff = require("diff");
const axios = require("axios");

const app = express();
const PORT = process.env.PORT || 3000;

process.env.PLAYWRIGHT_BROWSERS_PATH =
  process.env.PLAYWRIGHT_BROWSERS_PATH || "0";

app.use(cors());
app.use(express.json({ limit: "20mb" }));
app.use(express.urlencoded({ extended: true }));
app.use(express.static("public"));

const upload = multer({
  storage: multer.memoryStorage(),
  limits: { fileSize: 20 * 1024 * 1024 }
});

const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

const URL_CACHE_TTL_MS = 3 * 60 * 1000;
const URL_CACHE_MAX_ITEMS = 10;
const urlCache = new Map();

let sharedBrowser = null;
let browserInitPromise = null;

/* =========================
   LAZY LOAD HEAVY LIBS
========================= */

let mammothLib = null;
let playwrightLib = null;
let jsdomLib = null;
let readabilityLib = null;

function getMammoth() {
  if (!mammothLib) mammothLib = require("mammoth");
  return mammothLib;
}

function getPlaywright() {
  if (!playwrightLib) playwrightLib = require("playwright");
  return playwrightLib;
}

function getJSDOM() {
  if (!jsdomLib) jsdomLib = require("jsdom");
  return jsdomLib;
}

function getReadability() {
  if (!readabilityLib) readabilityLib = require("@mozilla/readability");
  return readabilityLib;
}

/* =========================
   BASIC HELPERS
========================= */

function isValidHttpUrl(value) {
  try {
    const url = new URL(value);
    return url.protocol === "http:" || url.protocol === "https:";
  } catch {
    return false;
  }
}

function escapeHtml(str) {
  return String(str)
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

function normalizeVietnameseText(text) {
  if (!text) return "";

  return text
    .normalize("NFC")
    .replace(/\p{Cf}/gu, "")
    .replace(/\u00AD/g, "")
    .replace(/\u00A0/g, " ")
    .replace(/[“”„‟]/g, '"')
    .replace(/[‘’‚‛]/g, "'")
    .replace(/[‐-‒–—―]/g, "-")
    .replace(/…/g, "...")
    .replace(/\r/g, "")
    .replace(/\t/g, " ")
    .replace(/\s+/g, " ")
    .trim();
}

function normalizeForStrictCompare(text) {
  return normalizeVietnameseText(text)
    .replace(/\s*([:;,.!?|/-])\s*/g, " $1 ")
    .replace(/\s+/g, " ")
    .trim();
}

const viWordSegmenter =
  typeof Intl !== "undefined" && Intl.Segmenter
    ? new Intl.Segmenter("vi", { granularity: "word" })
    : null;

const viCollator = new Intl.Collator("vi", {
  usage: "search",
  sensitivity: "variant",
  ignorePunctuation: true
});

function tokenizeVietnameseWords(text) {
  const normalized = normalizeVietnameseText(text);
  if (!normalized) return [];

  if (viWordSegmenter) {
    const segments = [];
    for (const part of viWordSegmenter.segment(normalized)) {
      if (part.isWordLike) segments.push(part.segment);
    }
    return segments;
  }

  return normalized.split(/\s+/).filter(Boolean);
}

function normalizeCompareTokenVi(word) {
  return normalizeVietnameseText(word)
    .replace(/^[^\p{L}\p{N}]+|[^\p{L}\p{N}]+$/gu, "")
    .trim();
}

function equalViWord(a, b) {
  const aa = normalizeCompareTokenVi(a);
  const bb = normalizeCompareTokenVi(b);

  if (!aa && !bb) return true;
  if (!aa || !bb) return false;

  return viCollator.compare(aa, bb) === 0;
}

function similarityScore(a, b) {
  const wordsA = tokenizeVietnameseWords(a);
  const wordsB = tokenizeVietnameseWords(b);

  if (!wordsA.length && !wordsB.length) return 1;
  if (!wordsA.length || !wordsB.length) return 0;

  const matchedB = new Array(wordsB.length).fill(false);
  let common = 0;

  for (const wa of wordsA) {
    for (let i = 0; i < wordsB.length; i++) {
      if (matchedB[i]) continue;
      if (equalViWord(wa, wordsB[i])) {
        matchedB[i] = true;
        common++;
        break;
      }
    }
  }

  return common / Math.max(wordsA.length, wordsB.length);
}

function logMemory(label) {
  const m = process.memoryUsage();
  console.log(label, {
    rssMB: (m.rss / 1024 / 1024).toFixed(1),
    heapUsedMB: (m.heapUsed / 1024 / 1024).toFixed(1),
    heapTotalMB: (m.heapTotal / 1024 / 1024).toFixed(1)
  });
}

function trimCacheIfNeeded() {
  while (urlCache.size > URL_CACHE_MAX_ITEMS) {
    const oldestKey = urlCache.keys().next().value;
    if (!oldestKey) break;
    urlCache.delete(oldestKey);
  }
}

/* =========================
   BROWSER REUSE
========================= */

async function getSharedBrowser() {
  if (sharedBrowser) return sharedBrowser;
  if (browserInitPromise) return browserInitPromise;

  const { chromium } = getPlaywright();

  const launchOptions = {
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-software-rasterizer",
      "--disable-background-networking",
      "--disable-background-timer-throttling",
      "--disable-renderer-backgrounding"
    ]
  };

  if (process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH) {
    launchOptions.executablePath =
      process.env.PLAYWRIGHT_CHROMIUM_EXECUTABLE_PATH;
  }

  browserInitPromise = chromium.launch(launchOptions);

  try {
    sharedBrowser = await browserInitPromise;
    return sharedBrowser;
  } finally {
    browserInitPromise = null;
  }
}

async function closeSharedBrowser() {
  if (sharedBrowser) {
    try {
      await sharedBrowser.close();
    } catch (_) {}
    sharedBrowser = null;
  }
}

async function shutdown(signal) {
  console.log(`Nhận tín hiệu ${signal}, đang tắt browser...`);
  await closeSharedBrowser();
  process.exit(0);
}

process.on("SIGINT", () => shutdown("SIGINT"));
process.on("SIGTERM", () => shutdown("SIGTERM"));

/* =========================
   HTML FETCH
========================= */

async function getHtmlWithAxios(url) {
  const response = await axios.get(url, {
    timeout: 12000,
    maxRedirects: 5,
    responseType: "text",
    transformResponse: [(data) => data],
    headers: {
      "User-Agent": DEFAULT_USER_AGENT,
      "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"
    }
  });

  return typeof response.data === "string"
    ? response.data
    : String(response.data || "");
}

async function looksLikeReadableArticle(html, url) {
  try {
    const { JSDOM } = getJSDOM();
    const { Readability } = getReadability();

    const dom = new JSDOM(html, { url });
    const article = new Readability(dom.window.document).parse();

    return Boolean(
      article &&
        article.content &&
        article.textContent &&
        article.textContent.trim().length > 200
    );
  } catch {
    return false;
  }
}

async function getHtmlWithPlaywright(url) {
  const browser = await getSharedBrowser();

  const page = await browser.newPage({
    userAgent: DEFAULT_USER_AGENT,
    viewport: { width: 1366, height: 768 },
    locale: "vi-VN"
  });

  page.setDefaultNavigationTimeout(15000);
  page.setDefaultTimeout(15000);

  try {
    await page.goto(url, {
      waitUntil: "domcontentloaded",
      timeout: 15000
    });

    await page.waitForTimeout(1200);

    const html = await page.content();
    return html;
  } finally {
    await page.close();
  }
}

async function getBestHtml(url) {
  const cached = urlCache.get(url);
  if (cached && Date.now() - cached.createdAt < URL_CACHE_TTL_MS) {
    return cached.html;
  }

  let html;

  try {
    const axiosHtml = await getHtmlWithAxios(url);

    if (await looksLikeReadableArticle(axiosHtml, url)) {
      html = axiosHtml;
    } else {
      html = await getHtmlWithPlaywright(url);
    }
  } catch (error) {
    console.log("Axios không đủ hoặc lỗi, fallback Playwright:", error.message);
    html = await getHtmlWithPlaywright(url);
  }

  urlCache.set(url, {
    html,
    createdAt: Date.now()
  });

  trimCacheIfNeeded();
  return html;
}

/* =========================
   CONTENT EXTRACTION
========================= */

async function cleanHtmlForComparison(html) {
  const { JSDOM } = getJSDOM();

  const dom = new JSDOM(`<div id="root">${html}</div>`);
  const document = dom.window.document;
  const root = document.querySelector("#root");

  if (!root) return "";

  root
    .querySelectorAll(
      "script, style, noscript, svg, canvas, iframe, figure, figcaption"
    )
    .forEach((el) => el.remove());

  root.querySelectorAll("img, picture, source, video, audio").forEach((el) => {
    el.remove();
  });

  root.querySelectorAll("*").forEach((el) => {
    [...el.attributes].forEach((attr) => {
      if (
        attr.name === "style" ||
        attr.name === "class" ||
        attr.name === "id" ||
        attr.name.startsWith("data-") ||
        attr.name.startsWith("aria-")
      ) {
        el.removeAttribute(attr.name);
      }
    });
  });

  const cleaned = root.innerHTML;

  dom.window.close();
  return cleaned;
}

function headingLevel(tagName) {
  const m = /^h([1-6])$/i.exec(tagName || "");
  return m ? Number(m[1]) : null;
}

function groupForTag(tag) {
  if (/^h[1-6]$/.test(tag)) return "heading";
  if (tag === "li") return "list";
  if (tag === "blockquote") return "blockquote";
  if (tag === "table-row") return "table";
  return "text";
}

function getCellText(cell) {
  return normalizeForStrictCompare(cell.textContent || "");
}

function buildTableRowText(cells) {
  return cells.map((c) => normalizeForStrictCompare(c)).filter(Boolean).join(" | ");
}

async function extractOrderedBlocksFromHtml(html) {
  const { JSDOM } = getJSDOM();

  const dom = new JSDOM(`<div id="root">${html}</div>`);
  const document = dom.window.document;
  const root = document.querySelector("#root");

  const blocks = [];
  let order = 0;

  function pushBlock(tag, text, meta = {}) {
    const normalized = normalizeForStrictCompare(text);
    if (!normalized) return;

    blocks.push({
      tag,
      group: groupForTag(tag),
      level: headingLevel(tag),
      text: normalized,
      order: order++,
      ...meta
    });
  }

  function extractTableRows(tableEl) {
    const rows = [];
    const trs = tableEl.querySelectorAll("tr");

    trs.forEach((tr, rowIndex) => {
      const cells = [...tr.querySelectorAll("th,td")]
        .map((cell) => getCellText(cell))
        .filter(Boolean);

      if (!cells.length) return;

      rows.push({
        tag: "table-row",
        text: buildTableRowText(cells),
        rowIndex,
        cellCount: cells.length
      });
    });

    return rows;
  }

  function walk(node) {
    if (!node || node.nodeType !== 1) return;

    const tag = node.tagName.toLowerCase();

    if (
      ["script", "style", "noscript", "svg", "canvas", "iframe", "figure", "figcaption"].includes(tag)
    ) {
      return;
    }

    if (["img", "picture", "source", "video", "audio"].includes(tag)) {
      return;
    }

    if (tag === "table") {
      const rows = extractTableRows(node);
      rows.forEach((row) =>
        pushBlock(row.tag, row.text, {
          rowIndex: row.rowIndex,
          cellCount: row.cellCount
        })
      );
      return;
    }

    if (["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "blockquote"].includes(tag)) {
      const text = normalizeForStrictCompare(node.textContent || "");
      if (text) pushBlock(tag, text);
      return;
    }

    [...node.children].forEach(walk);
  }

  if (root) {
    [...root.children].forEach(walk);

    if (!blocks.length) {
      const text = normalizeForStrictCompare(root.textContent || "");
      if (text) pushBlock("p", text);
    }
  }

  dom.window.close();
  return blocks;
}

function blocksToFullText(blocks) {
  return blocks.map((b) => b.text).join("\n");
}

async function extractMainContentFromWeb(html, url) {
  const { JSDOM } = getJSDOM();
  const { Readability } = getReadability();

  const dom = new JSDOM(html, { url });
  const reader = new Readability(dom.window.document);
  const article = reader.parse();

  dom.window.close();

  if (!article || !article.content) {
    throw new Error("Không bóc được nội dung chính từ web");
  }

  const cleanHtml = await cleanHtmlForComparison(article.content);
  const blocks = await extractOrderedBlocksFromHtml(cleanHtml);

  return {
    title: article.title || "",
    html: cleanHtml,
    blocks,
    fullText: blocksToFullText(blocks)
  };
}

async function convertDocxToHtml(buffer) {
  const mammoth = getMammoth();

  const result = await mammoth.convertToHtml(
    { buffer },
    { includeDefaultStyleMap: true }
  );

  const cleanHtml = await cleanHtmlForComparison(result.value || "");
  const blocks = await extractOrderedBlocksFromHtml(cleanHtml);

  return {
    html: cleanHtml,
    blocks,
    fullText: blocksToFullText(blocks),
    messages: result.messages || []
  };
}

/* =========================
   ALIGN + DIFF
========================= */

function exactMatchAfterVietnameseNormalization(webFullText, docFullText) {
  const webTokens = tokenizeVietnameseWords(webFullText).map(normalizeCompareTokenVi);
  const docTokens = tokenizeVietnameseWords(docFullText).map(normalizeCompareTokenVi);

  if (webTokens.length !== docTokens.length) return false;

  for (let i = 0; i < webTokens.length; i++) {
    if (!equalViWord(webTokens[i], docTokens[i])) {
      return false;
    }
  }

  return true;
}

function blockThreshold(block) {
  if (block.group === "heading") return 0.82;
  if (block.group === "table") return 0.62;
  if (block.group === "list") return 0.62;
  return 0.58;
}

function blockCompatible(webBlock, docBlock) {
  if (!webBlock || !docBlock) return false;
  if (webBlock.group !== docBlock.group) return false;

  if (webBlock.group === "heading") {
    return webBlock.level === docBlock.level;
  }

  return true;
}

function alignBlocksOrdered(webBlocks, docBlocks, lookAhead = 2) {
  const pairs = [];
  let i = 0;
  let j = 0;

  while (i < webBlocks.length && j < docBlocks.length) {
    const w = webBlocks[i];
    const d = docBlocks[j];

    if (blockCompatible(w, d)) {
      const score = similarityScore(w.text, d.text);
      if (score >= blockThreshold(w)) {
        pairs.push({
          webIndex: i,
          docIndex: j,
          webText: w.text,
          docText: d.text,
          webTag: w.tag,
          docTag: d.tag,
          webGroup: w.group,
          docGroup: d.group,
          score
        });
        i++;
        j++;
        continue;
      }
    }

    let best = null;

    for (let offset = 1; offset <= lookAhead; offset++) {
      const nextWeb = webBlocks[i + offset];
      if (nextWeb && blockCompatible(nextWeb, d)) {
        const score = similarityScore(nextWeb.text, d.text);
        if (score >= blockThreshold(nextWeb)) {
          if (!best || score > best.score) {
            best = { kind: "skip-web", offset, score };
          }
        }
      }
    }

    for (let offset = 1; offset <= lookAhead; offset++) {
      const nextDoc = docBlocks[j + offset];
      if (nextDoc && blockCompatible(w, nextDoc)) {
        const score = similarityScore(w.text, nextDoc.text);
        if (score >= blockThreshold(w)) {
          if (!best || score > best.score) {
            best = { kind: "skip-doc", offset, score };
          }
        }
      }
    }

    if (best?.kind === "skip-web") {
      i += best.offset;
      continue;
    }

    if (best?.kind === "skip-doc") {
      for (let k = 0; k < best.offset; k++) {
        pairs.push({
          webIndex: -1,
          docIndex: j,
          webText: "",
          docText: docBlocks[j].text,
          webTag: "",
          docTag: docBlocks[j].tag,
          webGroup: "",
          docGroup: docBlocks[j].group,
          score: 0
        });
        j++;
      }
      continue;
    }

    pairs.push({
      webIndex: i,
      docIndex: j,
      webText: w.text,
      docText: d.text,
      webTag: w.tag,
      docTag: d.tag,
      webGroup: w.group,
      docGroup: d.group,
      score: blockCompatible(w, d) ? similarityScore(w.text, d.text) : 0
    });
    i++;
    j++;
  }

  while (j < docBlocks.length) {
    pairs.push({
      webIndex: -1,
      docIndex: j,
      webText: "",
      docText: docBlocks[j].text,
      webTag: "",
      docTag: docBlocks[j].tag,
      webGroup: "",
      docGroup: docBlocks[j].group,
      score: 0
    });
    j++;
  }

  return pairs;
}

function getChangedDocWordsOnly(webText, docText) {
  const webWords = tokenizeVietnameseWords(webText);
  const docWords = tokenizeVietnameseWords(docText);

  const parts = Diff.diffArrays(webWords, docWords, {
    comparator: (left, right) => equalViWord(left, right)
  });

  const changed = [];

  for (const part of parts) {
    if (part.added) {
      changed.push(...part.value);
    }
  }

  return changed;
}

function buildHighlightedBlock(docText, webText) {
  const addedDocWords = getChangedDocWordsOnly(webText, docText);

  if (!addedDocWords.length) {
    return {
      changedCount: 0,
      highlightedHtml: escapeHtml(docText)
    };
  }

  const pool = [...addedDocWords];
  const originalPieces = docText.match(/\S+|\s+/g) || [];
  let changedCount = 0;

  const html = originalPieces
    .map((piece) => {
      if (/^\s+$/.test(piece)) {
        return escapeHtml(piece);
      }

      const idx = pool.findIndex((w) => equalViWord(w, piece));
      if (idx !== -1) {
        pool.splice(idx, 1);
        changedCount++;
        return `<mark class="diff-word-doc">${escapeHtml(piece)}</mark>`;
      }

      return escapeHtml(piece);
    })
    .join("");

  return {
    changedCount,
    highlightedHtml: html
  };
}

function buildResultHtmlFromPairs(pairs) {
  const renderedBlocks = [];
  let totalChangedCount = 0;

  for (const pair of pairs) {
    if (pair.docIndex === -1) continue;
    if (!pair.docText) continue;

    const minScore =
      pair.docGroup === "heading" ? 0.5 :
      pair.docGroup === "table" ? 0.35 :
      0.32;

    if (pair.webIndex !== -1 && pair.score < minScore) continue;

    const blockResult = buildHighlightedBlock(pair.docText, pair.webText);

    if (blockResult.changedCount > 0) {
      totalChangedCount += blockResult.changedCount;

      renderedBlocks.push(`
        <div class="diff-block">
          <div class="diff-block-meta">
            Đoạn #${pair.docIndex + 1} | Loại: ${escapeHtml(pair.docTag || "text")} | Độ giống: ${(pair.score * 100).toFixed(1)}%
          </div>
          <div class="diff-block-content">${blockResult.highlightedHtml}</div>
        </div>
      `);
    }
  }

  return {
    totalChangedCount,
    html: renderedBlocks.join("\n")
  };
}

/* =========================
   ROUTES
========================= */

app.get("/", (req, res) => {
  res.send("SEO Compare Tool đang chạy.");
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    uptime: process.uptime(),
    cacheSize: urlCache.size
  });
});

app.post("/compare-docx", upload.single("docxFile"), async (req, res) => {
  try {
    logMemory("before compare");

    const { url } = req.body;
    const file = req.file;

    if (!url || !file) {
      return res.status(400).json({
        error: "Thiếu URL hoặc file DOCX"
      });
    }

    if (!isValidHttpUrl(url)) {
      return res.status(400).json({
        error: "URL không hợp lệ"
      });
    }

    const renderedHtml = await getBestHtml(url);
    logMemory("after getBestHtml");

    const webData = await extractMainContentFromWeb(renderedHtml, url);
    logMemory("after web extract");

    const docData = await convertDocxToHtml(file.buffer);
    logMemory("after docx convert");

    if (
      exactMatchAfterVietnameseNormalization(
        webData.fullText,
        docData.fullText
      )
    ) {
      return res.json({
        success: true,
        articleTitle: webData.title,
        changedCount: 0,
        exactMatchAfterNormalization: true,
        highlightedDocHtml: `
          <div class="preview-box">
            Nội dung DOCX khớp với nội dung chính của bài viết trên web sau khi chuẩn hóa tiếng Việt.
          </div>
        `,
        conversionMessages: docData.messages
      });
    }

    const pairs = alignBlocksOrdered(webData.blocks, docData.blocks, 2);
    const result = buildResultHtmlFromPairs(pairs);

    return res.json({
      success: true,
      articleTitle: webData.title,
      changedCount: result.totalChangedCount,
      exactMatchAfterNormalization: false,
      highlightedDocHtml:
        result.totalChangedCount > 0
          ? result.html
          : `
            <div class="preview-box">
              Không tìm thấy khác biệt đủ tin cậy sau khi đối chiếu tiêu đề, đoạn văn và bảng.
            </div>
          `,
      conversionMessages: docData.messages,
      debug: {
        webBlocks: webData.blocks.length,
        docBlocks: docData.blocks.length,
        alignedPairs: pairs.length,
        cacheSize: urlCache.size
      }
    });
  } catch (error) {
    console.error("COMPARE ERROR:", error);
    return res.status(500).json({
      error: "Không thể xử lý file DOCX",
      details: error.message
    });
  }
});

logMemory("startup");

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Tool đang chạy tại: http://0.0.0.0:${PORT}`);
});