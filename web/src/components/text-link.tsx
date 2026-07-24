// One source for the inline text-link recipe that was copy-pasted across views in
// two different class orderings. Use `<TextLink to=…>` for react-router navigation,
// or apply `inlineLinkClass` to a plain <a>/<button> that links inline in prose.

import type { ComponentProps } from "react";
import { Link } from "react-router";
import { cn } from "@/lib/utils";

export const inlineLinkClass =
  "text-link underline-offset-4 hover:underline rounded-sm focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring";

export function TextLink({ className, ...props }: ComponentProps<typeof Link>) {
  return <Link className={cn(inlineLinkClass, className)} {...props} />;
}
