---
name: Cairn Console
description: The plain-spoken admin surface for a self-hosted S3-compatible object store.
colors:
  paper-white: "oklch(1 0 0)"
  near-black: "oklch(0.145 0 0)"
  neutral-primary: "oklch(0.205 0 0)"
  surface-tint: "oklch(0.985 0 0)"
  hover-fill: "oklch(0.97 0 0)"
  muted-ink: "oklch(0.51 0 0)"
  hairline: "oklch(0.922 0 0)"
  hairline-dark: "oklch(0.31 0 0)"
  link-blue: "oklch(0.5 0.2 255)"
  focus-blue: "oklch(0.53 0.2 255)"
  success-green: "oklch(0.52 0.13 155)"
  warning-amber: "oklch(0.55 0.13 75)"
  destructive-red: "oklch(0.577 0.245 27.3)"
typography:
  headline:
    fontFamily: "Geist Sans, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1.25rem"
    fontWeight: 600
    lineHeight: 1.3
    letterSpacing: "-0.01em"
  title:
    fontFamily: "Geist Sans, ui-sans-serif, system-ui, sans-serif"
    fontSize: "1rem"
    fontWeight: 600
    lineHeight: 1.4
    letterSpacing: "normal"
  body:
    fontFamily: "Geist Sans, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.875rem"
    fontWeight: 400
    lineHeight: 1.5
    letterSpacing: "normal"
  label:
    fontFamily: "Geist Sans, ui-sans-serif, system-ui, sans-serif"
    fontSize: "0.75rem"
    fontWeight: 500
    lineHeight: 1.4
    letterSpacing: "normal"
  mono:
    fontFamily: "Geist Mono, ui-monospace, SFMono-Regular, Menlo, monospace"
    fontSize: "0.8125rem"
    fontWeight: 400
    lineHeight: 1.4
    fontFeature: "tnum"
rounded:
  sm: "4px"
  md: "6px"
  lg: "8px"
  xl: "12px"
spacing:
  xs: "4px"
  sm: "8px"
  md: "16px"
  lg: "24px"
components:
  button-primary:
    backgroundColor: "{colors.neutral-primary}"
    textColor: "{colors.paper-white}"
    rounded: "{rounded.md}"
    padding: "8px 16px"
    height: "36px"
  button-outline:
    backgroundColor: "{colors.paper-white}"
    textColor: "{colors.near-black}"
    rounded: "{rounded.md}"
    padding: "8px 16px"
    height: "36px"
  button-destructive-outline:
    backgroundColor: "{colors.paper-white}"
    textColor: "{colors.destructive-red}"
    rounded: "{rounded.md}"
    padding: "8px 16px"
    height: "36px"
  card:
    backgroundColor: "{colors.paper-white}"
    textColor: "{colors.near-black}"
    rounded: "{rounded.lg}"
    padding: "24px"
  input:
    backgroundColor: "{colors.paper-white}"
    textColor: "{colors.near-black}"
    rounded: "{rounded.md}"
    padding: "8px 12px"
    height: "36px"
  data-cell-mono:
    textColor: "{colors.near-black}"
    typography: "{typography.mono}"
---

# Design System: Cairn Console

## 1. Overview

**Creative North Star: "The Plain-Spoken Utility"**

Cairn is infrastructure: a self-hosted, S3-compatible object store that someone is trusting with production data. The console is the reason a person picks Cairn over MinIO, so its whole job is to make running your own storage feel calm and legible instead of intimidating. The look is Vercel/Geist restraint in service of that legibility, executed in React + shadcn/ui on the Geist Sans/Mono faces: pure-white (or near-black) surfaces, hairline borders, near-black ink, and one neutral primary button. Color is silent until it has something to say. The interface should read like a competent operator explaining the system in plain language, never like a tool showing off.

This system is built on subtraction. Depth comes from a single 1px border, not a stack of shadows. Hierarchy comes from scale and weight, not boxes-within-boxes. The primary action is neutral black-on-white, not a saturated brand color, because in a tool full of irreversible actions the loudest thing on screen should be a warning, not a "Save" button. Density is reserved for the places data demands it (object lists, tables, the metrics dashboard) and even there the hierarchy stays quiet. The deliberate opposite of MinIO's cramped widget-soup: let the interface breathe, and reserve every drop of emphasis for the moment an operator needs to make a decision about their data.

What it explicitly rejects: consumer-cute / toy-like web console (mascots, bouncy or elastic motion, emoji-as-web console); MinIO's utilitarian density (capable but cramped and uncrafted); and the generic AI-SaaS template (gradient hero, purple-everywhere, identical icon+heading card grids, enterprise widget-soup). Crafted is the bar; functional is not enough.

**Key Characteristics:**
- Pure-neutral surfaces (white / near-black) with hairline borders; depth is structural, never shadowy.
- One neutral primary button. Blue is for links and focus only. Green/amber/red appear only as meaning.
- Geist Sans for everything web console; Geist Mono with tabular numerals for every identifier, size, address, and number.
- Fixed rem type scale (12 / 13px-mono / 14 / 16 / 20 / 24), never fluid `clamp()`.
- Spacious by default; dense only where data demands it, and calm even there.
- Light and dark at contrast parity; motion conveys state, never decorates.

## 2. Colors

A pure-neutral gray ramp carries 95% of every screen; a single blue and three semantic hues are the only saturated colors, and each earns its place.

### Primary
- **Neutral Primary** (`oklch(0.205 0 0)`, ~#171717): The primary button and other top-emphasis neutral surfaces. In dark mode it inverts to near-white (`oklch(0.93 0 0)`) so the primary button is black-on-light / white-on-dark. This is the system's "accent": a neutral, not a hue.
- **Link Blue** (`oklch(0.5 0.2 255)`): Links and informational text only. The one place a true color leads. Dark mode lifts it to `oklch(0.68 0.15 252)` for AA on near-black.
- **Focus Blue** (`oklch(0.53 0.2 255)`): The focus ring, and only the focus ring. A 2px ring offset 2px off every interactive control, identical everywhere.

### Neutral
- **Paper White** (`oklch(1 0 0)`, #ffffff): The light content surface. Cards, the main canvas, popovers. Flat and pure; no warm tint.
- **Near Black** (`oklch(0.145 0 0)`, #0a0a0a): Body ink on light; the content surface on dark. The single ink color for primary text.
- **Surface Tint** (`oklch(0.985 0 0)`, #fafafa): The page's faint second neutral layer: the sidebar rail, table headers, muted fills. A whisper cooler than the content surface, never a card.
- **Hover Fill** (`oklch(0.97 0 0)`, #f5f5f5): Ghost-button and row hover fills; the secondary button surface.
- **Muted Ink** (`oklch(0.51 0 0)`, ~#666): Secondary and descriptive text. Held to a real 5.7:1 on white, never decorative light-gray.
- **Hairline** (`oklch(0.922 0 0)`, #e5e5e5 light / `oklch(0.31 0 0)`, #2e2e2e dark): The 1px border that does all the structural work. Cards, inputs, table rows, dividers, the sidebar edge.

### Semantic (used only as meaning)
- **Success Green** (`oklch(0.52 0.13 155)`): An active/healthy state, a confirmed positive (an active user, "all caught up").
- **Warning Amber** (`oklch(0.55 0.13 75)`): A caution that isn't an error: a caps-lock notice, a quota hint, a forever-share warning.
- **Destructive Red** (`oklch(0.577 0.245 27.3)`, ~#dc2626 light / `oklch(0.66 0.21 25)` dark): Destructive actions and errors only. The loudest color on screen, reserved for the moment it should be.

### Named Rules
**The Meaningful-Color Rule.** Green, amber, and red appear *only* when they carry meaning (state, warning, harm). They are forbidden as decoration, as category coding, or as visual interest. If a colored element doesn't change meaning when you gray it out, it shouldn't be colored.

**The One Blue Rule.** Blue is reserved for links and the focus ring. It is never a button fill, never a heading color, never a background. The primary action is neutral; a tool full of irreversible operations must not teach the eye that blue means "press me."

## 3. Typography

**web console Font:** Geist Sans (with `ui-sans-serif, system-ui, -apple-system, "Segoe web console", Roboto` fallback)
**Mono Font:** Geist Mono (with `ui-monospace, SFMono-Regular, Menlo, Consolas` fallback)

**Character:** One family does all the web console work in three weights (400 / 500 / 600); Geist Mono is its exact-width partner for anything an operator might copy, compare, or trust as a literal value. The pairing is two members of one type family, not a contrast pairing: this is product, not editorial, and consistency is the point. `font-feature-settings: "tnum"` is global, so every numeral aligns in columns.

### Hierarchy
- **Headline** (Geist Sans 600, 1.25rem/20px, `tracking-tight` -0.01em): The page title (`<h1>` in PageHeader). The largest type on a normal screen. There is no hero/display tier; this is the ceiling.
- **Stat value** (Geist Sans 600, 1.5rem/24px, `tabular-nums`): The one step above headline, reserved for the big single-number readouts in StatCards and the metrics dashboard. Mono variant at 1.25rem for monospace figures.
- **Title** (Geist Sans 600, 1rem/16px): Card titles and section headings.
- **Body** (Geist Sans 400, 0.875rem/14px, line-height 1.5): The workhorse. All prose, labels in sentence case, descriptions. Prose capped at 65–75ch.
- **Mono / Data** (Geist Mono 400, 0.8125rem/13px, tabular): The single most common text in the console. Every identifier, object key, bucket name, byte size, access key, ARN, version id, and address. If it's a literal value, it's mono.
- **Label** (Geist Sans 500, 0.75rem/12px): Table column headers, meta text, badges. Sentence case, not all-caps.

### Named Rules
**The Mono-for-Truth Rule.** Every value an operator reads as a literal (key, size, id, address, count) is set in Geist Mono with tabular numerals. Prose is Geist Sans. The font *is* the signal of "this is exact data, not commentary."

**The Fixed-Scale Rule.** Type is a fixed rem scale (12 / 13 / 14 / 16 / 20 / 24). Fluid `clamp()` headings are forbidden: users view at a consistent DPI, a heading that shrinks in a sidebar looks worse, and product web console has too many type elements for exaggerated scale contrast. No type element exceeds 24px.

## 4. Elevation

This system is **flat by default**. Depth is conveyed by a single 1px hairline border, never by a shadow. A card, an input, a table, the sidebar: all sit flush on the canvas, separated only by their border. Shadows are reserved exclusively for layers that genuinely float above the page and need to detach from it.

### Shadow Vocabulary
- **Floating layer** (`box-shadow` on popovers, dropdown menus, dialogs, command palette, toasts): The *only* elements that cast a shadow. They leave the document flow and overlap content, so a soft shadow earns its place; in dark mode they also lift one surface step (`#171717` popover over `#0a0a0a` canvas) so the border alone isn't doing all the work.

### Named Rules
**The 1px-Border Rule.** Structure is hairline borders, not shadows. If a flat surface (card, panel, table, well) reaches for a `box-shadow` to separate from the page, it's wrong: use the `hairline` border. A shadow on a non-floating element is a bug.

**The No-Nested-Card Rule.** Cards never nest. A bordered surface inside a bordered surface is two boxes competing; flatten to one, or separate with a divider and spacing instead.

## 5. Components

The vocabulary is deliberately small and identical screen to screen. A "Save" button looks the same in eleven places; if it doesn't, one is wrong.

### Buttons
- **Shape:** Gently rounded (6px, `rounded-md`), 36px tall (`h-9`), `text-sm` 500 weight, with a `gap-2` icon slot.
- **Primary:** Neutral Primary fill, paper-white text (`#171717` / white text on light; inverts on dark). The default emphasis; never blue.
- **Outline / Secondary / Ghost:** Outline is a hairline border on the surface; secondary is the hover-fill surface; ghost is transparent until hover. Used for everything that isn't the one primary action on a screen.
- **Destructive & Destructive-outline:** Solid Destructive Red for the committing destructive action; `destructive-outline` (red text + red-tinted hairline border) for a destructive-but-secondary action, so red is never carried by text color alone.
- **Hover / Focus:** Hover shifts the fill one step (≤200ms). Focus is the universal 2px Focus-Blue ring offset 2px. Disabled drops to 50% opacity with pointer-events off.

### Cards & Containers
- **Corner:** 8px (`rounded-lg`).
- **Surface:** Paper White, flat, with a 1px Hairline border. No shadow (see Elevation).
- **Padding:** 24px (`gap-4` between internal blocks; cards stack with a 16px gap).
- **Footer:** A `border-t` + `pt-4` footer carries the card's primary action (the SettingsCard pattern).

### Inputs / Fields
- **Style:** Paper-white surface, 1px Hairline border, 6px radius, 36px tall, `text-sm`.
- **Focus:** The universal 2px Focus-Blue ring (no glow, no border-color swap as the only cue).
- **Error:** `aria-invalid` tints the border destructive; the message renders below in a shared `FieldError` (`role="alert"`, 13px, Destructive Red), never inline ad-hoc.

### Navigation
- **Sidebar:** The Surface-Tint rail with a hairline right edge. Items are `text-sm`, muted at rest, near-black + a tint fill when active (`aria-current="page"`). Collapses to an off-canvas drawer (hamburger) below the `md` breakpoint; the Buckets item expands into an inline accordion of bucket sub-links.
- **Tabs & Breadcrumbs:** Line-style tabs (underline on active); breadcrumbs via the shared primitive with `aria-current="page"` on the last segment.

### Data Table (signature)
- The shared `DataTable` wraps every list (buckets, users, activity, tags, objects) in a bordered, horizontally-scrollable shell with a `minWidth` floor (default 560px) so a narrow viewport scrolls instead of clipping. Header cells are Label type on the Surface-Tint header row; data cells are Mono. Loading is `SkeletonRows`, never a centered spinner. One table component, one row vocabulary, everywhere.

### PermissionBuilder (signature, the model for the whole console)
- The way Cairn makes an S3 concept legible: presets and a visual builder up front for the common case, raw JSON one click away for experts, the two always kept in sync. Plain choices over IAM jargon. Every complex configuration surface (replication, lifecycle) should aspire to this shape.

## 6. Do's and Don'ts

### Do:
- **Do** convey all structure with the 1px Hairline border (`#e5e5e5` light, `#2e2e2e` dark). Reserve `box-shadow` for floating layers only (popover, dialog, menu, toast).
- **Do** keep the primary button neutral (Neutral Primary fill). Make the loudest thing on a screen the destructive/warning state, not a routine action.
- **Do** set every literal value (key, size, id, address, count) in Geist Mono with tabular numerals; keep prose in Geist Sans.
- **Do** state plainly what an irreversible action will do before it happens (delete a bucket, rotate a key, reveal a one-time secret) and confirm before harm.
- **Do** use the shared vocabulary: `Page`/`PageHeader`, `Card`, `DataTable`/`SkeletonRows`, `StatCard`, `StatusBadge`, `TextLink`, `FieldError`, `ConfirmDialog`. One button shape, one form-control set, one icon style across every screen.
- **Do** render `ErrorAlert` above retained content and skeletons only on first load; teach the interface in empty states, never "nothing here."
- **Do** hold muted and placeholder text to real contrast (≥4.5:1); target AAA (7:1) where it doesn't fight the task. Honor `prefers-reduced-motion` (degrade to instant/crossfade, never gate content).

### Don't:
- **Don't** use `border-left`/`border-right` greater than 1px as a colored accent stripe on cards, alerts, or list items. Full hairline borders or background tints instead.
- **Don't** use gradient text (`background-clip: text`), decorative glassmorphism, or any gradient hero. This is the generic AI-SaaS template Cairn explicitly rejects.
- **Don't** drift toward consumer-cute / toy-like web console: no mascots, no playful illustrations, no bouncy or elastic motion, no emoji-as-web console. It undermines trust in an infrastructure tool.
- **Don't** ship MinIO's utilitarian density: controls jammed together with no hierarchy or air. Spacious by default; density only where data demands it, calm even there.
- **Don't** color anything that isn't carrying meaning. No green/amber/red for decoration or category coding; no blue except links and the focus ring; no full-saturation accents on inactive states.
- **Don't** nest cards, or reach for a modal as the first thought (exhaust inline / progressive alternatives first), or reinvent a standard affordance (custom scrollbars, weird form controls, non-standard modals) for flavor.
- **Don't** use fluid `clamp()` type or any text above 24px; don't set body copy or section labels in all-caps; don't add a tiny tracked uppercase eyebrow above sections.
