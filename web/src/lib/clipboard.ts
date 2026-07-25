// Clipboard helper shared by the copy affordances (the copy field, the imports view, the
// code blocks). Lives in lib/ rather than beside a component so a component file exports only
// components — which is also what keeps Fast Refresh working.

/**
 * Copy text to the clipboard, falling back to the hidden-textarea trick for
 * plain-http origins where `navigator.clipboard` is unavailable (the common
 * self-hosted LAN deployment).
 */
export async function copyText(text: string): Promise<boolean> {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text);
      return true;
    }
  } catch {
    /* fall through to the legacy path */
  }
  try {
    const ta = document.createElement("textarea");
    ta.value = text;
    ta.setAttribute("readonly", "");
    ta.style.position = "fixed";
    ta.style.opacity = "0";
    document.body.appendChild(ta);
    ta.select();
    const ok = document.execCommand("copy");
    document.body.removeChild(ta);
    return ok;
  } catch {
    return false;
  }
}
