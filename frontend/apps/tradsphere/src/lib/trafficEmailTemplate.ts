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
      workspace: extracted.payload,
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
  const safeBodyHtml = multilineTextToHtml(bodyContent);
  const safeInstructionsHtml = multilineTextToHtml(instructionsContent);
  const headerTitle = normalizedCampaign;
  const subtitleMonthYear = extractMonthYearFromSubject(normalizedSubject);

  const subtitleText = `${subtitleMonthYear ? `${subtitleMonthYear} ` : ""}${normalizedAccount} Traffic`;
  const downloadRowsHtml = downloadLinks.length > 0
    ? downloadLinks.map((item, index) => {
      const isci = item.isci.trim() || `Flight ${index + 1}`;
      const fileHref = normalizeHttpUrl(item.fileUrl);
      const scriptHref = normalizeHttpUrl(item.scriptUrl);
      const fileValue = fileHref
        ? `<a href="${escapeHtml(fileHref)}" target="_blank" rel="noreferrer" style="color:#4F46E5;text-decoration:underline;font-weight:600;">Open file</a>`
        : `<span style="color:#64748B;">-</span>`;
      const scriptValue = scriptHref
        ? `<a href="${escapeHtml(scriptHref)}" target="_blank" rel="noreferrer" style="color:#4F46E5;text-decoration:underline;font-weight:600;">Open script</a>`
        : `<span style="color:#64748B;">-</span>`;
      const noteValue = item.note.trim()
        ? multilineTextToHtml(item.note)
        : `<span style="color:#64748B;">-</span>`;

      return `
        <tr>
          <td style="padding:14px 16px;border-bottom:1px solid #E2E8F0;font-size:16px;line-height:1.35;color:#1F2937;font-weight:600;vertical-align:middle;">${escapeHtml(isci)}</td>
          <td style="padding:14px 16px;border-bottom:1px solid #E2E8F0;font-size:13px;line-height:1.45;color:#334155;vertical-align:middle;">${fileValue}</td>
          <td style="padding:14px 16px;border-bottom:1px solid #E2E8F0;font-size:13px;line-height:1.45;color:#334155;vertical-align:middle;">${scriptValue}</td>
          <td style="padding:14px 16px;border-bottom:1px solid #E2E8F0;font-size:13px;line-height:1.55;color:#475569;vertical-align:middle;">${noteValue}</td>
        </tr>
      `;
    }).join("")
    : `
      <tr>
        <td colspan="4" style="padding:16px;border-bottom:1px solid #E2E8F0;font-size:13px;line-height:1.45;color:#64748B;">No flight download rows available.</td>
      </tr>
    `;

  return `<!DOCTYPE html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>${escapeHtml(normalizedSubject)}</title>
  </head>
  <body style="margin:0;padding:0;background:#DCE4F3;font-family:'Segoe UI',Arial,Helvetica,sans-serif;color:#1F2937;">
    <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="background:#DCE4F3;">
      <tr>
        <td align="center" style="padding:32px 16px;">
          <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="max-width:640px;background:#FFFFFF;border:1px solid #E2E8F0;border-radius:12px;border-collapse:separate;overflow:hidden;">
            <tr>
              <td
                background="${TRAFFIC_EMAIL_HEADER_IMAGE_URL}"
                style="padding:0;background-color:#1E1B4B;background-image:url('${TRAFFIC_EMAIL_HEADER_IMAGE_URL}');background-position:center;background-size:cover;background-repeat:no-repeat;"
              >
                <div style="height:4px;background:#6366F1;line-height:0;font-size:0;">&nbsp;</div>
                <div style="padding:34px 32px 30px;background:rgba(15,23,42,0.45);">
                  <p style="margin:0 0 10px;font-size:11px;line-height:1.35;letter-spacing:0.12em;color:#C7D2FE;text-transform:uppercase;font-weight:700;">TRADSPHERE TRAFFIC DELIVERY</p>
                  <h1 style="margin:0;font-size:24px;line-height:1.25;color:#FFFFFF;font-weight:700;font-family:Georgia,'Times New Roman',Times,serif;">${escapeHtml(headerTitle)}</h1>
                  <p style="margin:12px 0 0;font-size:15px;line-height:1.5;color:#E2E8F0;font-weight:500;">${escapeHtml(subtitleText)}</p>
                </div>
              </td>
            </tr>
            <tr>
              <td style="padding:26px 32px;background:#FFFFFF;">
                <div style="margin:0 0 24px;padding:0;">
                  <p style="margin:0;font-size:15px;line-height:1.7;color:#1F2937;">${safeBodyHtml || "&nbsp;"}</p>
                </div>

                <div style="margin:0 0 24px;">
                  <p style="margin:0 0 12px;font-size:11px;line-height:1.35;letter-spacing:0.08em;text-transform:uppercase;color:#4F46E5;font-weight:700;">DOWNLOAD LINKS</p>
                  <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:separate;border-spacing:0;border:1px solid #E2E8F0;border-radius:10px;background:#EFF4FF;overflow:hidden;">
                    <thead>
                      <tr>
                        <th align="left" style="padding:12px 16px;background:#EFF4FF;border-bottom:1px solid #E2E8F0;font-size:11px;line-height:1.35;text-transform:uppercase;color:#64748B;font-weight:600;">ISCI</th>
                        <th align="left" style="padding:12px 16px;background:#EFF4FF;border-bottom:1px solid #E2E8F0;font-size:11px;line-height:1.35;text-transform:uppercase;color:#64748B;font-weight:600;">FILE URL</th>
                        <th align="left" style="padding:12px 16px;background:#EFF4FF;border-bottom:1px solid #E2E8F0;font-size:11px;line-height:1.35;text-transform:uppercase;color:#64748B;font-weight:600;">SCRIPT URL</th>
                        <th align="left" style="padding:12px 16px;background:#EFF4FF;border-bottom:1px solid #E2E8F0;font-size:11px;line-height:1.35;text-transform:uppercase;color:#64748B;font-weight:600;">NOTE</th>
                      </tr>
                    </thead>
                    <tbody>
                      ${downloadRowsHtml}
                    </tbody>
                  </table>
                </div>

                <div style="margin:0 0 24px;">
                  <p style="margin:0 0 12px;font-size:11px;line-height:1.35;letter-spacing:0.08em;text-transform:uppercase;color:#4F46E5;font-weight:700;">TRAFFIC INSTRUCTIONS</p>
                  <div style="padding:18px 16px;border:1px solid #D7DEEE;border-radius:10px;background:#EFF4FF;">
                    <p style="margin:0;font-size:15px;line-height:1.7;color:#46556B;">${safeInstructionsHtml || "&nbsp;"}</p>
                  </div>
                </div>

                <div style="margin:28px -32px -26px;border-top:1px solid #E2E8F0;background:#F8F9FF;">
                  <div style="padding:24px 32px 20px;">
                    <div style="border-bottom:1px solid #C7C4D8;padding-bottom:16px;margin-bottom:16px;">
                      <table role="presentation" cellpadding="0" cellspacing="0" border="0" width="100%" style="border-collapse:collapse;">
                        <tr>
                          <td valign="middle" style="width:128px;padding:0 16px 0 0;vertical-align:middle;">
                            <img alt="The Automotive Advertising Agency" src="https://res.cloudinary.com/dpmjwuqfl/image/upload/v1741870003/TAAA%20Logos/zj4pclsfzfbgc7jt8u5q.png" width="128" style="display:block;width:128px;height:auto;border:0;" />
                          </td>
                          <td valign="middle" style="padding:0;vertical-align:middle;">
                            <p style="margin:0;font-size:13px;line-height:1.35;color:#121C2A;font-weight:700;">Hai Truong</p>
                            <p style="margin:2px 0 0;font-size:12px;line-height:1.35;color:#464555;font-weight:600;">Traffic Manager</p>
                            <p style="margin:2px 0 0;font-size:11px;line-height:1.35;color:#505F76;">The Automotive Advertising Agency</p>
                            <p style="margin:2px 0 0;font-size:11px;line-height:1.35;color:#464555;"><span style="font-weight:700;">t.</span> 512.610.7300 | <span style="font-weight:700;">m.</span> 469.810.0266</p>
                            <p style="margin:2px 0 0;font-size:11px;line-height:1.35;color:#464555;">6104 Old Fredericksburg Rd, #92767, Austin, TX 78709</p>
                            <p style="margin:4px 0 0;font-size:11px;line-height:1.35;">
                              <a href="https://www.theautoadagency.com/" target="_blank" rel="noreferrer" style="color:#3525CD;text-decoration:underline;">Website</a>
                            </p>
                          </td>
                        </tr>
                      </table>
                    </div>

                    <div style="margin:0 0 10px;">
                      <p style="margin:0;font-size:8px;line-height:1.35;color:#505F76;font-weight:700;text-transform:uppercase;">Greenspaces</p>
                      <p style="margin:2px 0 0;font-size:8px;line-height:1.5;color:#64748B;">Please consider the environment before printing this email.</p>
                    </div>
                    <div style="margin:0;">
                      <p style="margin:0;font-size:8px;line-height:1.35;color:#505F76;font-weight:700;text-transform:uppercase;">Confidentiality</p>
                      <p style="margin:2px 0 0;font-size:8px;line-height:1.5;color:#64748B;">This message and its attachments are sent from The Automotive Advertising Agency, a full-service advertising agency serving the automotive industry, and may contain information that is confidential and protected by privilege from disclosure. If you are not the intended recipient, you are prohibited from printing, copying, forwarding or saving them. Please delete the message and attachments without printing, copying, forwarding or saving them, and notify the sender immediately.</p>
                    </div>

                    <div style="margin-top:16px;padding-top:16px;border-top:1px solid #C7C4D8;">
                      <p style="margin:0;font-size:10px;line-height:1.35;color:#505F76;text-align:center;">© 2024 Tradsphere Traffic Operations Team. All rights reserved.</p>
                    </div>
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
