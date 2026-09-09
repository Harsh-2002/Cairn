import { createContext, useContext } from "react";
import type { EndpointStatus } from "@/lib/types";

export const EndpointContext = createContext<EndpointStatus | null>(null);

export function useEndpoints() {
  const status = useContext(EndpointContext);
  const apiIssue = status?.issues.find((issue) => issue.setting === "CAIRN_API_PUBLIC_URL");
  const consoleIssue = status?.issues.find((issue) => issue.setting !== "CAIRN_API_PUBLIC_URL");
  return { status, apiIssue, consoleIssue };
}

