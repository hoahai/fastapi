const BLOCK_TAGS = new Set(["P", "DIV", "BLOCKQUOTE", "UL", "OL", "LI", "H1", "H2", "H3", "H4"]);
const INLINE_TAGS = new Set(["SPAN", "STRONG", "B", "EM", "I", "U", "S", "CODE", "A", "BR"]);
const ALLOWED_TAGS = new Set([...BLOCK_TAGS, ...INLINE_TAGS]);

function normalizeLineBreaks(value: string): string {
  return value.replace(/\r\n/g, "\n");
}

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function linkifyPlainText(value: string): string {
  const urlPattern = /(https?:\/\/[^\s<]+|www\.[^\s<]+)/gi;
  let cursor = 0;
  let output = "";
  let match: RegExpExecArray | null;

  while ((match = urlPattern.exec(value)) !== null) {
    const token = match[0] ?? "";
    const index = match.index ?? 0;
    output += escapeHtml(value.slice(cursor, index));

    const href = token.startsWith("www.") ? `https://${token}` : token;
    output += `<a href="${escapeHtml(href)}" target="_blank" rel="noreferrer noopener">${escapeHtml(token)}</a>`;
    cursor = index + token.length;
  }

  output += escapeHtml(value.slice(cursor));
  return output;
}

function plainTextParagraphsToHtml(value: string): string {
  const normalized = normalizeLineBreaks(value).trim();
  if (!normalized) {
    return "";
  }

  return normalized
    .split(/\n{2,}/)
    .map((paragraph) => {
      const text = paragraph.trim();
      if (!text) {
        return "";
      }
      return `<p>${text.split("\n").map((line) => linkifyPlainText(line)).join("<br />")}</p>`;
    })
    .filter(Boolean)
    .join("");
}

function sanitizeAnchorHref(value: string): string {
  const raw = value.trim();
  if (!raw) {
    return "";
  }

  const candidates = raw.includes("://") ? [raw] : [`https://${raw}`];
  for (const candidate of candidates) {
    try {
      const parsed = new URL(candidate);
      if (parsed.protocol === "http:" || parsed.protocol === "https:" || parsed.protocol === "mailto:") {
        return parsed.toString();
      }
    } catch {
      continue;
    }
  }

  return "";
}

function sanitizeNode(node: Node, documentRef: Document): Node | null {
  if (node.nodeType === Node.TEXT_NODE) {
    return documentRef.createTextNode(node.textContent || "");
  }

  if (node.nodeType !== Node.ELEMENT_NODE) {
    return null;
  }

  const element = node as HTMLElement;
  const tagName = element.tagName.toUpperCase();

  if (tagName === "SCRIPT" || tagName === "STYLE" || tagName === "NOSCRIPT") {
    return null;
  }

  if (!ALLOWED_TAGS.has(tagName)) {
    const fragment = documentRef.createDocumentFragment();
    for (const child of Array.from(element.childNodes)) {
      const sanitizedChild = sanitizeNode(child, documentRef);
      if (sanitizedChild) {
        fragment.appendChild(sanitizedChild);
      }
    }
    return fragment;
  }

  if (tagName === "BR") {
    return documentRef.createElement("br");
  }

  const sanitizedElement = documentRef.createElement(tagName.toLowerCase());
  if (tagName === "A") {
    const href = sanitizeAnchorHref(element.getAttribute("href") || "");
    if (href) {
      sanitizedElement.setAttribute("href", href);
      sanitizedElement.setAttribute("target", "_blank");
      sanitizedElement.setAttribute("rel", "noreferrer noopener");
    }
  }

  for (const child of Array.from(element.childNodes)) {
    const sanitizedChild = sanitizeNode(child, documentRef);
    if (sanitizedChild) {
      sanitizedElement.appendChild(sanitizedChild);
    }
  }

  return sanitizedElement;
}

export function isLikelyHtml(value: string): boolean {
  return /<\/?[a-z][\s\S]*>/i.test(value.trim());
}

export function normalizeRichTextHtml(value: string): string {
  const input = normalizeLineBreaks(value || "").trim();
  if (!input) {
    return "";
  }

  if (!isLikelyHtml(input)) {
    return plainTextParagraphsToHtml(input);
  }

  if (typeof document === "undefined") {
    return input;
  }

  const template = document.createElement("template");
  template.innerHTML = input;

  const fragment = document.createDocumentFragment();
  for (const child of Array.from(template.content.childNodes)) {
    const sanitizedChild = sanitizeNode(child, document);
    if (sanitizedChild) {
      fragment.appendChild(sanitizedChild);
    }
  }

  const container = document.createElement("div");
  container.appendChild(fragment);
  const rendered = container.innerHTML.trim();
  const textContent = (container.textContent || "").replace(/\u00A0/g, " ").trim();
  if (!rendered || !textContent) {
    return "";
  }
  return rendered;
}
