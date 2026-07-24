import { StrictMode } from "react";
import { createRoot } from "react-dom/client";

// Geist, self-hosted: the woff2 files ship inside dist/assets so the console
// makes zero network requests beyond its own origin.
import "@fontsource/geist-sans/400.css";
import "@fontsource/geist-sans/500.css";
import "@fontsource/geist-sans/600.css";
import "@fontsource/geist-mono/400.css";
import "./globals.css";

import { App } from "./app";

createRoot(document.getElementById("root")!).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
