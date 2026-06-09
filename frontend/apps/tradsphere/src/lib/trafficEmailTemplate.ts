import { normalizeRichTextHtml } from "@shared/utils/richText";

export type TrafficEmailDownloadLink = {
  flightId: number | null;
  isci: string;
  fileUrl: string;
  scriptUrl: string;
  note: string;
};

export type TrafficEmailWorkspacePayload = {
  bodyContent: string;
  instructionsContent: string;
  downloadLinks: TrafficEmailDownloadLink[];
};

type TrafficEmailTemplateInput = {
  subject: string;
  accountLabel: string;
  campaignLabel: string;
  statusLabel?: string;
  bodyContent: string;
  instructionsContent: string;
  downloadLinks: TrafficEmailDownloadLink[];
};

const META_PREFIX = "<!--TS_TRAFFIC_EMAIL_META:";
const META_SUFFIX = "-->";
const TRAFFIC_EMAIL_HEADER_IMAGE_URL = "https://res.cloudinary.com/dpmjwuqfl/image/upload/v1779469175/traffic_email_header_bg_jr1m9g.png";

function escapeHtml(value: string): string {
  return value
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function normalizeHttpUrl(value: string): string {
  const raw = value.trim();
  if (!raw) {
    return "";
  }
  const candidates = raw.includes("://") ? [raw] : [`https://${raw}`];
  for (const candidate of candidates) {
    try {
      const parsed = new URL(candidate);
      if (parsed.protocol === "http:" || parsed.protocol === "https:") {
        return parsed.toString();
      }
    } catch {
      continue;
    }
  }
  return "";
}

function linkifyText(value: string): string {
  const urlPattern = /(https?:\/\/[^\s<]+|www\.[^\s<]+)/gi;
  let cursor = 0;
  let output = "";
  let match: RegExpExecArray | null;

  while ((match = urlPattern.exec(value)) !== null) {
    const token = match[0] ?? "";
    const index = match.index ?? 0;
    output += escapeHtml(value.slice(cursor, index));

    const normalizedUrl = normalizeHttpUrl(token);
    if (!normalizedUrl) {
      output += escapeHtml(token);
    } else {
      output += `<a href="${escapeHtml(normalizedUrl)}" target="_blank" rel="noreferrer" style="color:#4F46E5;text-decoration:underline;font-weight:600;">${escapeHtml(token)}</a>`;
    }

    cursor = index + token.length;
  }

  output += escapeHtml(value.slice(cursor));
  return output;
}

function multilineTextToHtml(value: string): string {
  const normalized = value.replace(/\r\n/g, "\n");
  const lines = normalized.split("\n");
  if (lines.length === 0) {
    return "";
  }
  return lines
    .map((line) => (line ? linkifyText(line) : "&nbsp;"))
    .join("<br />");
}

function renderRichTextHtml(value: string): string {
  const normalized = normalizeRichTextHtml(value);
  return normalized || "&nbsp;";
}

function renderTrafficEmailSummaryCard(label: string, valueHtml: string): string {
  return `
    <td valign="top" width="50%" style="padding:0 6px;vertical-align:top;">
      <div style="padding:11px 14px;border:1px solid #E2E8F0;border-radius:14px;background:#FBFCFE;box-shadow:0 1px 1px rgba(15,23,42,0.03);">
        <p style="margin:0 0 7px;font-size:9px;line-height:1.35;letter-spacing:0.12em;text-transform:uppercase;color:#64748B;font-weight:700;">${escapeHtml(label)}</p>
        <div style="font-size:13px;line-height:1.45;color:#0F172A;font-weight:600;word-break:break-word;font-variant-numeric:tabular-nums;">
          ${valueHtml}
        </div>
      </div>
    </td>
  `;
}

function renderTrafficEmailLinkPill(href: string, label: string): string {
  const normalizedHref = normalizeHttpUrl(href);
  if (!normalizedHref) {
    return `<span style="color:#94A3B8;font-size:12px;line-height:1.2;font-weight:600;">-</span>`;
  }
  const isDriveLink = /(?:drive|docs)\.google\.com/i.test(normalizedHref);
  const background = isDriveLink ? "#FEF3C7" : "#ECFDF5";
  const border = isDriveLink ? "#F59E0B" : "#A7F3D0";
  const color = isDriveLink ? "#92400E" : "#047857";
  const icon = isDriveLink
    ? `
      <img
        src="https://res.cloudinary.com/dpmjwuqfl/image/upload/v1780583013/GGDrive_Icon_ctgqzm.png"
        alt=""
        aria-hidden="true"
        width="13"
        height="13"
        style="display:block;flex:none;width:13px;height:13px;object-fit:contain;"
      />
    `
    : `
      <svg aria-hidden="true" viewBox="0 0 24 24" width="13" height="13" fill="none" style="display:block;flex:none;">
        <path d="M12 4v9" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M8.5 9.5 12 13l3.5-3.5" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" />
        <path d="M5 16.5h14" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
        <path d="M7 19h10" stroke="currentColor" stroke-width="2" stroke-linecap="round" />
      </svg>
    `;
  return `
    <a href="${escapeHtml(normalizedHref)}" target="_blank" rel="noreferrer" style="display:inline-flex;align-items:center;gap:6px;padding:7px 10px;border:1px solid ${border};border-radius:999px;background:${background};color:${color};font-size:11px;line-height:1.2;font-weight:700;text-decoration:none;">
      <span aria-hidden="true" style="display:inline-flex;align-items:center;justify-content:center;width:13px;height:13px;">${icon}</span>
      <span>${escapeHtml(label)}</span>
    </a>
  `;
}

function extractMonthYearFromSubject(subject: string): string {
  const text = subject.trim();
  if (!text) {
    return "";
  }
  const bracketMatch = text.match(/\[([^\]]+)\]/);
  if (bracketMatch && bracketMatch[1]) {
    return bracketMatch[1].trim();
  }
  const monthYearMatch = text.match(
    /\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{4}\b/i,
  );
  if (monthYearMatch && monthYearMatch[0]) {
    return monthYearMatch[0].trim();
  }
  return "";
}

function encodeMeta(payload: TrafficEmailWorkspacePayload): string {
  try {
    const json = JSON.stringify(payload);
    const bytes = new TextEncoder().encode(json);
    let binary = "";
    for (const byte of bytes) {
      binary += String.fromCharCode(byte);
    }
    return btoa(binary);
  } catch {
    return "";
  }
}

function decodeMeta(encoded: string): TrafficEmailWorkspacePayload | null {
  if (!encoded.trim()) {
    return null;
  }
  try {
    const binary = atob(encoded);
    const bytes = Uint8Array.from(binary, (char) => char.charCodeAt(0));
    const json = new TextDecoder().decode(bytes);
    const parsed = JSON.parse(json) as Partial<TrafficEmailWorkspacePayload>;
    if (!parsed || typeof parsed !== "object") {
      return null;
    }
    const downloadLinks = Array.isArray(parsed.downloadLinks)
      ? parsed.downloadLinks
        .map((item) => ({
          flightId: typeof item?.flightId === "number" && Number.isFinite(item.flightId) ? item.flightId : null,
          isci: typeof item?.isci === "string" ? item.isci : "",
          fileUrl: typeof item?.fileUrl === "string" ? item.fileUrl : "",
          scriptUrl: typeof item?.scriptUrl === "string" ? item.scriptUrl : "",
          note: typeof item?.note === "string" ? item.note : "",
        }))
      : [];

    return {
      bodyContent: typeof parsed.bodyContent === "string" ? parsed.bodyContent : "",
      instructionsContent: typeof parsed.instructionsContent === "string" ? parsed.instructionsContent : "",
      downloadLinks,
    };
  } catch {
    return null;
  }
}

function extractMeta(body: string): { payload: TrafficEmailWorkspacePayload | null; withoutMeta: string } {
  const trimmed = body.trimStart();
  if (!trimmed.startsWith(META_PREFIX)) {
    return { payload: null, withoutMeta: body };
  }

  const suffixIndex = trimmed.indexOf(META_SUFFIX, META_PREFIX.length);
  if (suffixIndex < 0) {
    return { payload: null, withoutMeta: body };
  }

  const encoded = trimmed.slice(META_PREFIX.length, suffixIndex);
  const payload = decodeMeta(encoded);
  const withoutMeta = trimmed.slice(suffixIndex + META_SUFFIX.length).trimStart();
  return {
    payload,
    withoutMeta,
  };
}

function stripHtmlTags(html: string): string {
  const withoutTags = html
    .replace(/<\s*br\s*\/?>/gi, "\n")
    .replace(/<\s*\/p\s*>/gi, "\n")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/gi, " ")
    .replace(/&amp;/gi, "&")
    .replace(/&lt;/gi, "<")
    .replace(/&gt;/gi, ">")
    .replace(/&quot;/gi, '"')
    .replace(/&#39;/gi, "'");
  return withoutTags
    .replace(/\r\n/g, "\n")
    .replace(/[ \t]+\n/g, "\n")
    .replace(/\n{3,}/g, "\n\n")
    .trim();
}

export function parseTrafficEmailWorkspaceFromBody(bodyValue: string): {
  workspace: TrafficEmailWorkspacePayload | null;
  bodyFallbackText: string;
} {
  const body = bodyValue || "";
  const extracted = extractMeta(body);
  if (extracted.payload) {
    return {
      workspace: {
        bodyContent: normalizeRichTextHtml(extracted.payload.bodyContent),
        instructionsContent: normalizeRichTextHtml(extracted.payload.instructionsContent),
        downloadLinks: extracted.payload.downloadLinks,
      },
      bodyFallbackText: extracted.payload.bodyContent,
    };
  }

  const trimmed = extracted.withoutMeta.trim();
  const bodyFallbackText = /<\/?[a-z][\s\S]*>/i.test(trimmed)
    ? stripHtmlTags(trimmed)
    : trimmed;

  return {
    workspace: null,
    bodyFallbackText,
  };
}

export function buildTrafficEmailHtmlDocument({
  subject,
  accountLabel,
  campaignLabel,
  statusLabel,
  bodyContent,
  instructionsContent,
  downloadLinks,
}: TrafficEmailTemplateInput): string {
  void statusLabel;
  const normalizedSubject = subject.trim() || "Traffic Update";
  const normalizedAccount = accountLabel.trim() || "Tradsphere";
  const normalizedCampaign = campaignLabel.trim() || "Campaign";
  const safeBodyHtml = renderRichTextHtml(bodyContent);
  const safeInstructionsHtml = renderRichTextHtml(instructionsContent);
  const headerTitle = normalizedCampaign;
  const subtitleMonthYear = extractMonthYearFromSubject(normalizedSubject);
  const subtitleText = `${subtitleMonthYear ? `${subtitleMonthYear} • ` : ""}${normalizedAccount} traffic`;
  const preheaderText = `${normalizedCampaign} traffic update for ${normalizedAccount}. ${subtitleMonthYear ? `${subtitleMonthYear}. ` : ""}Review the latest body copy, download links, and traffic instructions.`;
  const downloadRowsHtml = downloadLinks.length > 0
    ? downloadLinks.map((item, index) => {
      const isci = item.isci.trim() || `Flight ${index + 1}`;
      const noteValue = item.note.trim()
        ? multilineTextToHtml(item.note)
        : `<span style="color:#94A3B8;">-</span>`;

      return `
        <tr style="background:${index % 2 === 0 ? "#FFFFFF" : "#F8FAFC"};">
          <td style="padding:14px 16px;border-bottom:1px solid #E2E8F0;font-size:13px;line-height:1.4;color:#0F172A;font-weight:700;vertical-align:middle;font-variant-numeric:tabular-nums;">${escapeHtml(isci)}</td>
          <td align="center" style="padding:14px 16px;border-bottom:1px solid #E2E8F0;font-size:12px;line-height:1.45;color:#334155;vertical-align:middle;text-align:center;">${renderTrafficEmailLinkPill(item.fileUrl, "Download")}</td>
          <td align="center" style="padding:14px 16px;border-bottom:1px solid #E2E8F0;font-size:12px;line-height:1.45;color:#334155;vertical-align:middle;text-align:center;">${renderTrafficEmailLinkPill(item.scriptUrl, "Download")}</td>
          <td style="padding:14px 16px;border-bottom:1px solid #E2E8F0;font-size:12px;line-height:1.5;color:#475569;vertical-align:middle;">
            <div style="max-width:240px;word-break:break-word;">${noteValue}</div>
          </td>
        </tr>
      `;
    }).join("")
    : `
      <tr>
        <td colspan="4" style="padding:18px 16px;border-bottom:1px solid #E2E8F0;font-size:13px;line-height:1.5;color:#64748B;background:#FBFCFE;">No flight download rows are available yet.</td>
      </tr>
    `;

  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <meta name="color-scheme" content="light" />
    <meta name="supported-color-schemes" content="light" />
    <title>${escapeHtml(normalizedSubject)}</title>
  </head>
  <body style="margin:0;padding:0;background:#F4F1EC;font-family:'Segoe UI',Arial,Helvetica,sans-serif;color:#1F2937;">
    <div style="display:none;max-height:0;overflow:hidden;opacity:0;color:transparent;mso-hide:all;">${escapeHtml(preheaderText)}</div>
    <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#F4F1EC;">
      <tr>
        <td align="center" style="padding:32px 16px;">
          <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="max-width:680px;background:#FFFFFF;border:1px solid #D9DEE6;border-radius:18px;border-collapse:separate;overflow:hidden;box-shadow:0 18px 40px -30px rgba(15,23,42,0.35);">
            <tr>
              <td
                background="${TRAFFIC_EMAIL_HEADER_IMAGE_URL}"
                style="padding:0;background-color:#0F172A;background-image:url('${TRAFFIC_EMAIL_HEADER_IMAGE_URL}');background-position:center;background-size:cover;background-repeat:no-repeat;"
              >
                <div style="height:5px;background:#B08A5A;line-height:0;font-size:0;">&nbsp;</div>
                <div style="padding:34px 32px 28px;background:rgba(15,23,42,0.60);">
                  <p style="margin:0 0 8px;font-size:10px;line-height:1.35;letter-spacing:0.14em;color:#E2E8F0;text-transform:uppercase;font-weight:700;">TRADSPHERE TRAFFIC DELIVERY</p>
                  <h1 style="margin:0;font-size:26px;line-height:1.16;color:#FFFFFF;font-weight:700;font-family:Georgia,'Times New Roman',Times,serif;letter-spacing:-0.02em;">${escapeHtml(headerTitle)}</h1>
                  <p style="margin:10px 0 0;max-width:40em;font-size:14px;line-height:1.55;color:#E2E8F0;font-weight:500;">${escapeHtml(subtitleText)}</p>
                </div>
              </td>
            </tr>
            <tr>
              <td style="padding:24px 32px 0;background:#FFFFFF;">
                <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:separate;border-spacing:0 0;">
                  <tr>
                    ${renderTrafficEmailSummaryCard("Account", escapeHtml(normalizedAccount))}
                    ${renderTrafficEmailSummaryCard("Campaign", escapeHtml(normalizedCampaign))}
                  </tr>
                </table>
              </td>
            </tr>
            <tr>
              <td style="padding:24px 32px 0;background:#FFFFFF;">
                <div style="margin:0;font-size:14px;line-height:1.75;color:#1F2937;max-width:44em;">
                  ${safeBodyHtml}
                </div>
              </td>
            </tr>

            <tr>
              <td style="padding:20px 32px 0;background:#FFFFFF;">
                <div style="margin:0;">
                  <p style="margin:0 0 10px;font-size:10px;line-height:1.35;letter-spacing:0.12em;text-transform:uppercase;color:#64748B;font-weight:700;">Download links</p>
                  <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:separate;border-spacing:0;border:1px solid #D9DEE6;border-radius:16px;background:#FFFFFF;overflow:hidden;">
                    <thead>
                      <tr>
                        <th align="left" style="padding:12px 16px;background:#F8FAFC;border-bottom:1px solid #D9DEE6;font-size:9px;line-height:1.35;letter-spacing:0.12em;text-transform:uppercase;color:#64748B;font-weight:700;">ISCI</th>
                        <th align="center" style="padding:12px 16px;background:#F8FAFC;border-bottom:1px solid #D9DEE6;font-size:9px;line-height:1.35;letter-spacing:0.12em;text-transform:uppercase;color:#64748B;font-weight:700;">File</th>
                        <th align="center" style="padding:12px 16px;background:#F8FAFC;border-bottom:1px solid #D9DEE6;font-size:9px;line-height:1.35;letter-spacing:0.12em;text-transform:uppercase;color:#64748B;font-weight:700;">Script</th>
                        <th align="left" style="padding:12px 16px;background:#F8FAFC;border-bottom:1px solid #D9DEE6;font-size:9px;line-height:1.35;letter-spacing:0.12em;text-transform:uppercase;color:#64748B;font-weight:700;">Note</th>
                      </tr>
                    </thead>
                    <tbody>
                      ${downloadRowsHtml}
                    </tbody>
                  </table>
                </div>
              </td>
            </tr>

            <tr>
              <td style="padding:20px 32px 0;background:#FFFFFF;">
                <p style="margin:0 0 10px;font-size:10px;line-height:1.35;letter-spacing:0.12em;text-transform:uppercase;color:#64748B;font-weight:700;">Traffic instructions</p>
                <div style="margin:0;padding:16px 20px;border:1px solid #D9DEE6;border-radius:16px;background:#FFFFFF;">
                  <div style="font-size:14px;line-height:1.75;color:#334155;max-width:44em;">
                    ${safeInstructionsHtml}
                  </div>
                </div>
              </td>
            </tr>

            <tr>
              <td style="padding:24px 32px 0;background:#FFFFFF;">
                <div style="padding:20px 18px 18px;border-top:1px solid #E2E8F0;border-radius:0 0 18px 18px;background:#F9FAFC;">
                  <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:collapse;">
                    <tr>
                      <td valign="middle" style="width:128px;padding:0 16px 0 0;vertical-align:middle;">
                        <img alt="The Automotive Advertising Agency" src="https://res.cloudinary.com/dpmjwuqfl/image/upload/v1741870003/TAAA%20Logos/zj4pclsfzfbgc7jt8u5q.png" width="128" style="display:block;width:128px;height:auto;border:0;" />
                      </td>
                      <td valign="middle" style="padding:0;vertical-align:middle;">
                        <p style="margin:0;font-size:13px;line-height:1.35;color:#111827;font-weight:700;">Hai Truong</p>
                        <p style="margin:2px 0 0;font-size:11px;line-height:1.35;color:#4B5563;font-weight:600;">Traffic Manager</p>
                        <p style="margin:3px 0 0;font-size:10px;line-height:1.5;color:#64748B;">The Automotive Advertising Agency</p>
                        <p style="margin:3px 0 0;font-size:10px;line-height:1.5;color:#4B5563;"><span style="font-weight:700;">t.</span> 512.610.7300 | <span style="font-weight:700;">m.</span> 469.810.0266</p>
                        <p style="margin:2px 0 0;font-size:10px;line-height:1.5;color:#4B5563;">6104 Old Fredericksburg Rd, #92767, Austin, TX 78709</p>
                        <p style="margin:5px 0 0;font-size:10px;line-height:1.35;">
                          <a href="https://www.theautoadagency.com/" target="_blank" rel="noreferrer" style="color:#1D4ED8;text-decoration:underline;font-weight:600;">Website</a>
                        </p>
                      </td>
                    </tr>
                  </table>

                  <div style="margin:14px 0 0;padding-top:14px;border-top:1px solid #D9DEE6;">
                    <p style="margin:0;font-size:7px;line-height:1.4;letter-spacing:0.08em;color:#64748B;font-weight:700;text-transform:uppercase;">Greenspaces</p>
                    <p style="margin:2px 0 0;font-size:7px;line-height:1.6;color:#6B7280;">Please consider the environment before printing this email.</p>
                  </div>
                  <div style="margin:10px 0 0;">
                    <p style="margin:0;font-size:7px;line-height:1.4;letter-spacing:0.08em;color:#64748B;font-weight:700;text-transform:uppercase;">Confidentiality</p>
                    <p style="margin:2px 0 0;font-size:7px;line-height:1.6;color:#6B7280;">This message and its attachments are sent from The Automotive Advertising Agency, a full-service advertising agency serving the automotive industry, and may contain information that is confidential and protected by privilege from disclosure. If you are not the intended recipient, you are prohibited from printing, copying, forwarding or saving them. Please delete the message and attachments without printing, copying, forwarding or saving them, and notify the sender immediately.</p>
                  </div>

                  <div style="margin-top:12px;padding-top:12px;border-top:1px solid #D9DEE6;">
                    <p style="margin:0;font-size:9px;line-height:1.35;color:#64748B;text-align:center;">© 2026 Tradsphere Traffic Operations Team. All rights reserved.</p>
                  </div>
                </div>
              </td>
            </tr>
          </table>
        </td>
      </tr>
    </table>
  </body>
</html>`;
}

export function persistTrafficEmailBody(params: {
  workspace: TrafficEmailWorkspacePayload;
  subject: string;
  accountLabel: string;
  campaignLabel: string;
  statusLabel?: string;
}): string {
  const { workspace, subject, accountLabel, campaignLabel, statusLabel } = params;
  const html = buildTrafficEmailHtmlDocument({
    subject,
    accountLabel,
    campaignLabel,
    statusLabel,
    bodyContent: workspace.bodyContent,
    instructionsContent: workspace.instructionsContent,
    downloadLinks: workspace.downloadLinks,
  });
  const encodedMeta = encodeMeta(workspace);
  if (!encodedMeta) {
    return html;
  }
  return `${META_PREFIX}${encodedMeta}${META_SUFFIX}\n${html}`;
}
