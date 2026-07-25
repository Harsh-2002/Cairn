// A small, SAFE Markdown renderer for the object preview. It emits React elements only — never
// dangerouslySetInnerHTML — so document bytes can never become active content in the console origin
// (audit #13). Raw HTML in the source is shown as literal text; links are restricted to http(s)/
// mailto; images are NOT loaded (rendered as an inert placeholder) so a README can't beacon out.
//
// Scope: a robust common subset (headings, paragraphs, hr, blockquote, fenced code, ordered/unordered
// lists, pipe tables, and inline code/bold/italic/links). The viewer offers a "Source" toggle, so any
// edge case this doesn't cover degrades to readable raw text rather than something wrong.

import { Fragment, type ReactNode } from "react";

const SAFE_LINK = /^(https?:\/\/|mailto:)/i;

// --- inline ---------------------------------------------------------------------------------------

function inline(text: string, kp: string): ReactNode[] {
  const out: ReactNode[] = [];
  let buf = "";
  let k = 0;
  const flush = () => {
    if (buf) {
      out.push(buf);
      buf = "";
    }
  };
  const push = (node: ReactNode) => {
    flush();
    out.push(<Fragment key={`${kp}-${k++}`}>{node}</Fragment>);
  };

  for (let i = 0; i < text.length; ) {
    const rest = text.slice(i);

    // inline code — literal content, highest priority
    const code = /^`([^`]+)`/.exec(rest);
    if (code) {
      push(
        <code className="rounded bg-muted px-1 py-0.5 font-mono text-[0.85em]">
          {code[1]}
        </code>,
      );
      i += code[0].length;
      continue;
    }

    // image — never loaded; inert placeholder
    const img = /^!\[([^\]]*)\]\(([^)]+)\)/.exec(rest);
    if (img) {
      push(
        <span className="text-muted-foreground italic">
          [image: {img[1] || img[2]}]
        </span>,
      );
      i += img[0].length;
      continue;
    }

    // link — safe schemes only; otherwise fall through and render the raw text
    const link = /^\[([^\]]+)\]\(([^)\s]+)(?:\s+"[^"]*")?\)/.exec(rest);
    if (link && SAFE_LINK.test(link[2])) {
      push(
        <a
          href={link[2]}
          target="_blank"
          rel="noopener noreferrer nofollow"
          className="text-primary underline underline-offset-2 hover:opacity-80"
        >
          {inline(link[1], `${kp}-l${k}`)}
        </a>,
      );
      i += link[0].length;
      continue;
    }

    // bold
    const bold = /^(\*\*|__)(.+?)\1/.exec(rest);
    if (bold) {
      push(<strong className="font-semibold">{inline(bold[2], `${kp}-b${k}`)}</strong>);
      i += bold[0].length;
      continue;
    }

    // italic (single * or _, not part of a ** run)
    const em = /^(\*|_)(?!\1)(.+?)\1/.exec(rest);
    if (em) {
      push(<em className="italic">{inline(em[2], `${kp}-i${k}`)}</em>);
      i += em[0].length;
      continue;
    }

    buf += text[i];
    i++;
  }
  flush();
  return out;
}

// --- blocks ---------------------------------------------------------------------------------------

export function renderMarkdown(src: string): ReactNode {
  const lines = src.replace(/\r\n?/g, "\n").split("\n");
  const blocks: ReactNode[] = [];
  let i = 0;
  let key = 0;
  const nk = () => `md-${key++}`;

  while (i < lines.length) {
    let line = lines[i];

    // blank
    if (/^\s*$/.test(line)) {
      i++;
      continue;
    }

    // fenced code
    const fence = /^\s*(```+|~~~+)(.*)$/.exec(line);
    if (fence) {
      const marker = fence[1][0];
      const body: string[] = [];
      i++;
      while (i < lines.length && !new RegExp(`^\\s*${marker === "`" ? "```+" : "~~~+"}\\s*$`).test(lines[i])) {
        body.push(lines[i]);
        i++;
      }
      i++; // closing fence
      blocks.push(
        <pre
          key={nk()}
          className="overflow-x-auto rounded-md border bg-muted/50 p-3 font-mono text-xs leading-relaxed"
        >
          <code>{body.join("\n")}</code>
        </pre>,
      );
      continue;
    }

    // heading
    const h = /^(#{1,6})\s+(.*)$/.exec(line);
    if (h) {
      const level = h[1].length;
      const cls = [
        "mt-6 mb-3 text-2xl font-semibold tracking-tight",
        "mt-6 mb-3 text-xl font-semibold tracking-tight",
        "mt-5 mb-2 text-lg font-semibold",
        "mt-4 mb-2 text-base font-semibold",
        "mt-4 mb-1 text-sm font-semibold",
        "mt-4 mb-1 text-sm font-semibold text-muted-foreground",
      ][level - 1];
      const content = inline(h[2].replace(/\s+#+\s*$/, ""), nk());
      const props = { key: nk(), className: `${cls} first:mt-0` };
      blocks.push(
        level === 1 ? (
          <h1 {...props}>{content}</h1>
        ) : level === 2 ? (
          <h2 {...props}>{content}</h2>
        ) : level === 3 ? (
          <h3 {...props}>{content}</h3>
        ) : level === 4 ? (
          <h4 {...props}>{content}</h4>
        ) : level === 5 ? (
          <h5 {...props}>{content}</h5>
        ) : (
          <h6 {...props}>{content}</h6>
        ),
      );
      i++;
      continue;
    }

    // horizontal rule
    if (/^\s*([-*_])(\s*\1){2,}\s*$/.test(line)) {
      blocks.push(<hr key={nk()} className="my-6 border-border" />);
      i++;
      continue;
    }

    // table: header row + separator row of ---
    if (line.includes("|") && i + 1 < lines.length && /^\s*\|?[\s:|-]+\|?\s*$/.test(lines[i + 1]) && lines[i + 1].includes("-")) {
      const splitRow = (r: string) =>
        r
          .trim()
          .replace(/^\||\|$/g, "")
          .split("|")
          .map((c) => c.trim());
      const header = splitRow(line);
      i += 2;
      const rows: string[][] = [];
      while (i < lines.length && lines[i].includes("|") && !/^\s*$/.test(lines[i])) {
        rows.push(splitRow(lines[i]));
        i++;
      }
      blocks.push(
        <div key={nk()} className="my-4 overflow-x-auto rounded-md border">
          <table className="w-full border-collapse text-sm">
            <thead>
              <tr className="border-b bg-muted/50">
                {header.map((c, ci) => (
                  <th key={ci} className="px-3 py-1.5 text-left font-medium">
                    {inline(c, `${nk()}-th${ci}`)}
                  </th>
                ))}
              </tr>
            </thead>
            <tbody>
              {rows.map((r, ri) => (
                <tr key={ri} className="border-b last:border-0">
                  {r.map((c, ci) => (
                    <td key={ci} className="px-3 py-1.5 align-top">
                      {inline(c, `${nk()}-td${ri}-${ci}`)}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>,
      );
      continue;
    }

    // blockquote
    if (/^\s*>\s?/.test(line)) {
      const body: string[] = [];
      while (i < lines.length && /^\s*>\s?/.test(lines[i])) {
        body.push(lines[i].replace(/^\s*>\s?/, ""));
        i++;
      }
      blocks.push(
        <blockquote
          key={nk()}
          className="my-4 border-l-2 border-border pl-4 text-muted-foreground italic"
        >
          {inline(body.join(" "), nk())}
        </blockquote>,
      );
      continue;
    }

    // lists (single level; ordered vs unordered by the first marker)
    const ulItem = /^\s*[-*+]\s+(.*)$/;
    const olItem = /^\s*\d+[.)]\s+(.*)$/;
    if (ulItem.test(line) || olItem.test(line)) {
      const ordered = olItem.test(line);
      const re = ordered ? olItem : ulItem;
      const items: string[] = [];
      while (i < lines.length && re.test(lines[i])) {
        items.push(re.exec(lines[i])![1]);
        i++;
      }
      const itemNodes = items.map((it, ii) => (
        <li key={ii} className="my-0.5">
          {inline(it, `${nk()}-li${ii}`)}
        </li>
      ));
      blocks.push(
        ordered ? (
          <ol key={nk()} className="my-3 list-decimal space-y-0.5 pl-6">
            {itemNodes}
          </ol>
        ) : (
          <ul key={nk()} className="my-3 list-disc space-y-0.5 pl-6">
            {itemNodes}
          </ul>
        ),
      );
      continue;
    }

    // paragraph — gather until a blank line or a block starter
    const para: string[] = [];
    while (
      i < lines.length &&
      !/^\s*$/.test(lines[i]) &&
      !/^\s*(```+|~~~+)/.test(lines[i]) &&
      !/^(#{1,6})\s+/.test(lines[i]) &&
      !/^\s*>\s?/.test(lines[i]) &&
      !ulItem.test(lines[i]) &&
      !olItem.test(lines[i])
    ) {
      para.push(lines[i]);
      i++;
    }
    line = para.join("\n");
    blocks.push(
      <p key={nk()} className="my-3 leading-relaxed">
        {inline(line, nk())}
      </p>,
    );
  }

  return <div className="text-sm">{blocks}</div>;
}
