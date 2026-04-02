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
  limits: { fileSize: 10 * 1024 * 1024 }
});

const DEFAULT_USER_AGENT =
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36";

const URL_CACHE_TTL_MS = 3 * 60 * 1000;
const MAX_CACHE_ITEMS = 10;
const urlCache = new Map();

let sharedBrowser = null;
let browserInitPromise = null;

/* =========================
   LAZY LOAD HEAVY LIBS
========================= */

let playwrightLib = null;
let jsdomLib = null;
let readabilityLib = null;

async function getPlaywright() {
  if (!playwrightLib) playwrightLib = require("playwright");
  return playwrightLib;
}

async function getJSDOM() {
  if (!jsdomLib) jsdomLib = require("jsdom");
  return jsdomLib;
}

async function getReadability() {
  if (!readabilityLib) readabilityLib = require("@mozilla/readability");
  return readabilityLib;
}

/* =========================
   DEBUG MEMORY
========================= */

function logMemory(label) {
  const m = process.memoryUsage();
  console.log(label, {
    rssMB: (m.rss / 1024 / 1024).toFixed(1),
    heapUsedMB: (m.heapUsed / 1024 / 1024).toFixed(1),
    heapTotalMB: (m.heapTotal / 1024 / 1024).toFixed(1),
    externalMB: (m.external / 1024 / 1024).toFixed(1)
  });
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

function safeDecodeBufferToString(buffer) {
  if (!buffer) return "";
  return buffer.toString("utf8").replace(/^\uFEFF/, "");
}

function pruneCache() {
  while (urlCache.size > MAX_CACHE_ITEMS) {
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

  const { chromium } = await getPlaywright();

  const launchOptions = {
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-software-rasterizer"
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

process.on("SIGINT", async () => {
  await closeSharedBrowser();
  process.exit(0);
});

process.on("SIGTERM", async () => {
  await closeSharedBrowser();
  process.exit(0);
});

/* =========================
   HTML FETCH
========================= */

async function getHtmlWithAxios(url) {
  const response = await axios.get(url, {
    timeout: 12000,
    maxRedirects: 5,
    responseType: "text",
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
    const { JSDOM } = await getJSDOM();
    const { Readability } = await getReadability();

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
    return await page.content();
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

  pruneCache();
  return html;
}

/* =========================
   CONTENT EXTRACTION
========================= */

async function cleanHtmlForComparison(html) {
  const { JSDOM } = await getJSDOM();

  const dom = new JSDOM(`<div id="root">${html}</div>`);
  const document = dom.window.document;
  const root = document.querySelector("#root");

  if (!root) return "";

  root
    .querySelectorAll(
      "script, style, noscript, svg, canvas, iframe, figure, figcaption, meta, link"
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
        attr.name.startsWith("aria-") ||
        attr.name.startsWith("on")
      ) {
        el.removeAttribute(attr.name);
      }
    });
  });

  return root.innerHTML || "";
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
  return cells
    .map((c) => normalizeForStrictCompare(c))
    .filter(Boolean)
    .join(" | ");
}

async function extractOrderedBlocksFromHtml(html) {
  const { JSDOM } = await getJSDOM();

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
      [
        "script",
        "style",
        "noscript",
        "svg",
        "canvas",
        "iframe",
        "figure",
        "figcaption",
        "meta",
        "link"
      ].includes(tag)
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

    if (
      ["h1", "h2", "h3", "h4", "h5", "h6", "p", "li", "blockquote"].includes(tag)
    ) {
      const text = normalizeForStrictCompare(node.textContent || "");
      if (text) pushBlock(tag, text);
      return;
    }

    [...node.children].forEach(walk);
  }

  if (root) {
    [...root.children].forEach(walk);
  }

  if (!blocks.length && root) {
    const text = normalizeForStrictCompare(root.textContent || "");
    if (text) pushBlock("p", text);
  }

  return blocks;
}

function blocksToFullText(blocks) {
  return blocks.map((b) => b.text).join("\n");
}

async function extractMainContentFromWeb(html, url) {
  const { JSDOM } = await getJSDOM();
  const { Readability } = await getReadability();

  const dom = new JSDOM(html, { url });
  const reader = new Readability(dom.window.document);
  const article = reader.parse();

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

async function extractContentFromUploadedHtml(htmlString) {
  const cleanHtml = await cleanHtmlForComparison(htmlString);
  const blocks = await extractOrderedBlocksFromHtml(cleanHtml);

  return {
    html: cleanHtml,
    blocks,
    fullText: blocksToFullText(blocks)
  };
}

/* =========================
   ALIGN + DIFF
========================= */

function exactMatchAfterVietnameseNormalization(webFullText, fileFullText) {
  const webTokens = tokenizeVietnameseWords(webFullText).map(normalizeCompareTokenVi);
  const fileTokens = tokenizeVietnameseWords(fileFullText).map(normalizeCompareTokenVi);

  if (webTokens.length !== fileTokens.length) return false;

  for (let i = 0; i < webTokens.length; i++) {
    if (!equalViWord(webTokens[i], fileTokens[i])) {
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

function blockCompatible(webBlock, fileBlock) {
  if (!webBlock || !fileBlock) return false;
  if (webBlock.group !== fileBlock.group) return false;

  if (webBlock.group === "heading") {
    return webBlock.level === fileBlock.level;
  }

  return true;
}

function alignBlocksOrdered(webBlocks, fileBlocks, lookAhead = 2) {
  const pairs = [];
  let i = 0;
  let j = 0;

  while (i < webBlocks.length && j < fileBlocks.length) {
    const w = webBlocks[i];
    const f = fileBlocks[j];

    if (blockCompatible(w, f)) {
      const score = similarityScore(w.text, f.text);
      if (score >= blockThreshold(w)) {
        pairs.push({
          webIndex: i,
          fileIndex: j,
          webText: w.text,
          fileText: f.text,
          webTag: w.tag,
          fileTag: f.tag,
          webGroup: w.group,
          fileGroup: f.group,
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
      if (nextWeb && blockCompatible(nextWeb, f)) {
        const score = similarityScore(nextWeb.text, f.text);
        if (score >= blockThreshold(nextWeb)) {
          if (!best || score > best.score) {
            best = { kind: "skip-web", offset, score };
          }
        }
      }
    }

    for (let offset = 1; offset <= lookAhead; offset++) {
      const nextFile = fileBlocks[j + offset];
      if (nextFile && blockCompatible(w, nextFile)) {
        const score = similarityScore(w.text, nextFile.text);
        if (score >= blockThreshold(w)) {
          if (!best || score > best.score) {
            best = { kind: "skip-file", offset, score };
          }
        }
      }
    }

    if (best?.kind === "skip-web") {
      i += best.offset;
      continue;
    }

    if (best?.kind === "skip-file") {
      for (let k = 0; k < best.offset; k++) {
        pairs.push({
          webIndex: -1,
          fileIndex: j,
          webText: "",
          fileText: fileBlocks[j].text,
          webTag: "",
          fileTag: fileBlocks[j].tag,
          webGroup: "",
          fileGroup: fileBlocks[j].group,
          score: 0
        });
        j++;
      }
      continue;
    }

    pairs.push({
      webIndex: i,
      fileIndex: j,
      webText: w.text,
      fileText: f.text,
      webTag: w.tag,
      fileTag: f.tag,
      webGroup: w.group,
      fileGroup: f.group,
      score: blockCompatible(w, f) ? similarityScore(w.text, f.text) : 0
    });

    i++;
    j++;
  }

  while (j < fileBlocks.length) {
    pairs.push({
      webIndex: -1,
      fileIndex: j,
      webText: "",
      fileText: fileBlocks[j].text,
      webTag: "",
      fileTag: fileBlocks[j].tag,
      webGroup: "",
      fileGroup: fileBlocks[j].group,
      score: 0
    });
    j++;
  }

  return pairs;
}

function getChangedFileWordsOnly(webText, fileText) {
  const webWords = tokenizeVietnameseWords(webText);
  const fileWords = tokenizeVietnameseWords(fileText);

  const parts = Diff.diffArrays(webWords, fileWords, {
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

function buildHighlightedBlock(fileText, webText) {
  const addedWords = getChangedFileWordsOnly(webText, fileText);

  if (!addedWords.length) {
    return {
      changedCount: 0,
      highlightedHtml: escapeHtml(fileText)
    };
  }

  const pool = [...addedWords];
  const originalPieces = fileText.match(/\S+|\s+/g) || [];
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
    if (pair.fileIndex === -1) continue;
    if (!pair.fileText) continue;

    const minScore =
      pair.fileGroup === "heading" ? 0.5 :
      pair.fileGroup === "table" ? 0.35 :
      0.32;

    if (pair.webIndex !== -1 && pair.score < minScore) continue;

    const blockResult = buildHighlightedBlock(pair.fileText, pair.webText);

    if (blockResult.changedCount > 0) {
      totalChangedCount += blockResult.changedCount;

      renderedBlocks.push(`
        <div class="diff-block">
          <div class="diff-block-meta">
            Đoạn #${pair.fileIndex + 1} | Loại: ${escapeHtml(
              pair.fileTag || "text"
            )} | Độ giống: ${(pair.score * 100).toFixed(1)}%
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
  res.send("SEO Compare HTML Tool đang chạy.");
});

app.get("/health", (req, res) => {
  res.json({
    ok: true,
    uptime: process.uptime(),
    cacheItems: urlCache.size
  });
});

app.post("/compare-html", upload.single("htmlFile"), async (req, res) => {
  try {
    logMemory("before compare-html");

    const { url } = req.body;
    const file = req.file;

    if (!url || !file) {
      return res.status(400).json({
        error: "Thiếu URL hoặc file HTML"
      });
    }

    if (!isValidHttpUrl(url)) {
      return res.status(400).json({
        error: "URL không hợp lệ"
      });
    }

    const filename = (file.originalname || "").toLowerCase();
    const mimetype = (file.mimetype || "").toLowerCase();

    const isHtmlFile =
      filename.endsWith(".html") ||
      filename.endsWith(".htm") ||
      mimetype.includes("text/html");

    if (!isHtmlFile) {
      return res.status(400).json({
        error: "Chỉ chấp nhận file .html hoặc .htm"
      });
    }

    const uploadedHtmlString = safeDecodeBufferToString(file.buffer);

    if (!uploadedHtmlString.trim()) {
      return res.status(400).json({
        error: "File HTML rỗng hoặc không đọc được"
      });
    }

    const webHtml = await getBestHtml(url);
    logMemory("after get web html");

    const webData = await extractMainContentFromWeb(webHtml, url);
    logMemory("after extract web content");

    const fileData = await extractContentFromUploadedHtml(uploadedHtmlString);
    logMemory("after extract uploaded html");

    if (
      exactMatchAfterVietnameseNormalization(
        webData.fullText,
        fileData.fullText
      )
    ) {
      return res.json({
        success: true,
        articleTitle: webData.title,
        changedCount: 0,
        exactMatchAfterNormalization: true,
        highlightedHtml: `
          <div class="preview-box">
            Nội dung file HTML khớp với nội dung chính của bài viết trên web sau khi chuẩn hóa tiếng Việt.
          </div>
        `,
        debug: {
          webBlocks: webData.blocks.length,
          fileBlocks: fileData.blocks.length,
          alignedPairs: 0
        }
      });
    }

    const pairs = alignBlocksOrdered(webData.blocks, fileData.blocks, 2);
    const result = buildResultHtmlFromPairs(pairs);

    return res.json({
      success: true,
      articleTitle: webData.title,
      changedCount: result.totalChangedCount,
      exactMatchAfterNormalization: false,
      highlightedHtml:
        result.totalChangedCount > 0
          ? result.html
          : `
            <div class="preview-box">
              Không tìm thấy khác biệt đủ tin cậy sau khi đối chiếu nội dung HTML với bài viết trên web.
            </div>
          `,
      debug: {
        webBlocks: webData.blocks.length,
        fileBlocks: fileData.blocks.length,
        alignedPairs: pairs.length
      }
    });
  } catch (error) {
    console.error(error);
    res.status(500).json({
      error: "Không thể xử lý file HTML",
      details: error.message
    });
  }
});

/* =========================
   START SERVER
========================= */

logMemory("startup");

app.listen(PORT, "0.0.0.0", () => {
  console.log(`Tool đang chạy tại: http://0.0.0.0:${PORT}`);
});