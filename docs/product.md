# Product

## Register

product

## Users

Operators and developers who self-host Cairn, an S3-compatible object store, usually on a single
node. Many are not storage or IAM experts: they know they need object storage and want to run their
own instead of paying for S3/R2. In the console they are in a focused admin task: create a bucket,
upload or share a file, mint a scoped access key for an app, turn on compression or replication,
check how much is stored. They are trusting this tool with production data, so confidence matters as
much as capability.

## Product Purpose

The Cairn admin console: the single browser surface for everything Cairn can do short of the S3 wire
protocol. Create and configure buckets; browse, preview, upload, download, and share objects; create
S3-API users scoped by an access policy; tune versioning, quotas, compression, and replication; and
see storage and compression at a glance. It is the reason a person picks Cairn over MinIO: the wire
protocol is a commodity, the console is the product. Success is an operator who sets up scoped access
and manages their storage without reading S3 IAM documentation, and trusts what they see.

## Brand Personality

Approachable, reassuring, trustworthy. The console lowers the intimidation of running your own object
storage. Approachable through clarity, plain language, and breathing room, never through playfulness:
no mascots, no bounce, no cuteness. The feeling is a calm, competent operator's tool that explains
itself, not a toy and not a wall of widgets. Quiet confidence over flourish.

## Visual Direction

Vercel/Geist minimalism, executed with React + shadcn/ui and the Geist Sans/Mono faces. Light mode is
pure white with hairline `#e5e5e5` borders and near-black ink; dark mode is near-black (`#0a0a0a`)
with `#2e2e2e` borders. Depth comes from 1px borders, not shadows (only floating layers — menus,
dialogs — cast one). The primary button is neutral (black on light, white on dark); blue is reserved
for links and the focus ring; semantic color (green/amber/red) appears only when it means something.
Identifiers, sizes, and addresses set in Geist Mono with tabular numerals.

## Anti-references

- **Consumer-cute / toy-like.** Rounded mascots, playful illustrations, bouncy or elastic motion,
  emoji-as-UI. Undermines trust for an infrastructure tool.
- **MinIO's utilitarian density.** The thing we replace: capable but cramped, dated, and uncrafted,
  every control jammed together with no hierarchy or air. Functional is not the bar; crafted is.
- Also avoid: the generic AI-SaaS template (gradient hero, purple-everywhere, identical icon+heading
  card grids) and the heavy enterprise widget-soup dashboard where everything competes at once.

## Design Principles

- **Approachable through clarity, not cuteness.** Lower the barrier with plain language, sensible
  defaults, and room to breathe, not with decoration or whimsy. A first-time self-hoster should feel
  oriented, not talked down to.
- **Earn trust at every step.** State plainly what an action will do, especially the irreversible
  ones (delete a bucket, rotate a key, change a policy, reveal a one-time secret). Confirm before
  harm; never surprise the operator with their own data.
- **Make S3 concepts legible.** Translate IAM, policies, versioning, and replication into plain
  choices. The PermissionBuilder is the model for the whole console: presets and a visual builder up
  front, raw JSON one click away for experts, the two always in sync.
- **Spacious, not dense.** The deliberate opposite of MinIO. Let the interface breathe; reserve
  density for the places data demands it (object lists, tables), and even there keep the hierarchy
  calm.
- **The tool disappears into the task.** Familiar product patterns, one consistent component
  vocabulary across every screen, standard affordances. Delight lives in small reassuring moments,
  not on every page.

## Accessibility & Inclusion

Target WCAG AAA where it does not fight the task (7:1 contrast for text, generous hit areas), with AA
as the non-negotiable floor everywhere. Full keyboard navigation with visible focus rings; honor
`prefers-reduced-motion` (motion conveys state, so it degrades to instant or crossfade, never gates
content); light and dark themes at contrast parity. Placeholder and muted text held to real contrast,
not decorative gray.
