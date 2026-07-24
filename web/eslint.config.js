import js from "@eslint/js";
import globals from "globals";
import tseslint from "typescript-eslint";
import reactHooks from "eslint-plugin-react-hooks";
import reactRefresh from "eslint-plugin-react-refresh";
import jsxA11y from "eslint-plugin-jsx-a11y";

// Flat-config ESLint for the console (React 19 + TypeScript + Vite). Layered on top of the strict
// `tsc -b` gate: tsc catches types + unused symbols; ESLint adds React-hooks correctness and
// accessibility. We enable the two classic, widely-adopted react-hooks rules (rules-of-hooks as an
// error, exhaustive-deps as a warning) rather than the plugin's newer experimental
// react-compiler-style checks, and keep jsx-a11y's recommended set as errors — the console's a11y is
// a first-class concern. `dist` (the vite output) is ignored.
export default tseslint.config(
  { ignores: ["dist", "node_modules"] },
  js.configs.recommended,
  ...tseslint.configs.recommended,
  jsxA11y.flatConfigs.recommended,
  {
    files: ["**/*.{ts,tsx}"],
    languageOptions: {
      ecmaVersion: 2022,
      globals: globals.browser,
    },
    plugins: {
      "react-hooks": reactHooks,
      "react-refresh": reactRefresh,
    },
    rules: {
      "react-hooks/rules-of-hooks": "error",
      "react-hooks/exhaustive-deps": "warn",
      "react-refresh/only-export-components": [
        "warn",
        { allowConstantExport: true },
      ],
      // Our `<label>`s wrap shadcn control *components* (a valid nested association); teach the rule
      // to recognize them as controls instead of flagging correct markup as unassociated.
      "jsx-a11y/label-has-associated-control": [
        "error",
        {
          controlComponents: [
            "Checkbox",
            "Input",
            "Textarea",
            "Switch",
            "Select",
            "SelectTrigger",
            "RadioGroupItem",
          ],
        },
      ],
      // The console uses autoFocus only in launcher/filter inputs (the command palette, the
      // folder-name filter) where focusing on open/mount is the expected, correct UX — never on a
      // page-load form. Off rather than per-attribute disables that fight the formatter.
      "jsx-a11y/no-autofocus": "off",
    },
  },
);
