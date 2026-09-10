import type { ReplicationConfiguration, ReplicationRule } from "./types";

const S3_NS = "http://s3.amazonaws.com/doc/2006-03-01/";
const elements = (el: Element, name: string) =>
  [...el.children].filter(
    (child) =>
      child.localName === name && child.namespaceURI === el.namespaceURI,
  );
const text = (el: Element, name: string) =>
  elements(el, name)[0]?.textContent ?? "";

function documentFor(xml: string): XMLDocument {
  const doc = new DOMParser().parseFromString(xml, "application/xml");
  if (
    doc.querySelector("parsererror") ||
    doc.documentElement.localName !== "ReplicationConfiguration" ||
    ![null, "", S3_NS].includes(doc.documentElement.namespaceURI)
  ) {
    throw new Error(
      "The replication configuration could not be read. Refresh before making changes.",
    );
  }
  return doc;
}

/** Read every rule; unknown settings remain visible but cannot enter the simple editor. */
export function parseReplication(xml: string): ReplicationConfiguration {
  const root = documentFor(xml).documentElement;
  const ruleElements = elements(root, "Rule");
  if (!ruleElements.length)
    throw new Error("The replication configuration contains no rules.");
  const rules: ReplicationRule[] = ruleElements.map((el) => {
    const destinations = elements(el, "Destination");
    const status = text(el, "Status");
    if (
      destinations.length !== 1 ||
      !text(destinations[0]!, "Bucket") ||
      !["Enabled", "Disabled"].includes(status)
    ) {
      throw new Error(
        "A replication rule has an unreadable destination or status.",
      );
    }
    const filter = elements(el, "Filter")[0];
    const and = filter && elements(filter, "And")[0];
    const prefix = filter ? text(and ?? filter, "Prefix") : text(el, "Prefix");
    const tags = filter
      ? [...elements(filter, "Tag"), ...(and ? elements(and, "Tag") : [])].map(
          (tag) => ({ key: text(tag, "Key"), value: text(tag, "Value") }),
        )
      : [];
    const enabled = (name: string) => {
      const setting = elements(el, name)[0];
      return setting != null && text(setting, "Status") === "Enabled";
    };
    return {
      id: text(el, "ID"),
      enabled: status === "Enabled",
      dest_bucket: text(destinations[0]!, "Bucket").replace(
        /^arn:aws:s3:::/,
        "",
      ),
      prefix,
      tags,
      existing_objects: enabled("ExistingObjectReplication"),
      delete_markers: enabled("DeleteMarkerReplication"),
    };
  });
  // An allow-list prevents a simple edit from dropping filters or options it cannot express.
  const allowed: Record<string, string[]> = {
    ReplicationConfiguration: ["Role", "Rule"],
    Rule: [
      "ID",
      "Status",
      "Priority",
      "Prefix",
      "Filter",
      "Destination",
      "ExistingObjectReplication",
      "DeleteMarkerReplication",
    ],
    Filter: ["Prefix"],
    Destination: ["Bucket"],
    ExistingObjectReplication: ["Status"],
    DeleteMarkerReplication: ["Status"],
  };
  const simple = (el: Element): boolean => {
    const names = new Set<string>();
    return (
      [...el.attributes].every(
        (a) => a.namespaceURI === "http://www.w3.org/2000/xmlns/",
      ) &&
      [...el.children].every((child) => {
        if (
          child.namespaceURI !== root.namespaceURI ||
          !(allowed[el.localName] ?? []).includes(child.localName) ||
          names.has(child.localName)
        )
          return false;
        names.add(child.localName);
        return simple(child);
      })
    );
  };
  const editable =
    rules.length === 1 &&
    rules[0]!.enabled &&
    simple(root) &&
    !(
      elements(ruleElements[0]!, "Prefix").length &&
      elements(ruleElements[0]!, "Filter").length
    );
  return { xml, rules, editable };
}

export function requireEditableReplication(
  current: ReplicationConfiguration | null,
  expected: ReplicationConfiguration | null,
): void {
  if (current && !current.editable)
    throw new Error(
      "This configuration needs the S3 API or CLI to edit its rules safely.",
    );
  if (current?.xml !== expected?.xml)
    throw new Error(
      "Replication settings changed. Refresh and review them before saving.",
    );
}

/** Edit only the controls this form owns; retain Role, ID, priority and document structure. */
export function replicationXml(
  current: ReplicationConfiguration | null,
  destination: string,
  prefix: string,
  existing: boolean,
  deletes: boolean,
): string {
  if (current && !current.editable)
    throw new Error(
      "This replication configuration is read-only in the simple editor.",
    );
  const doc = documentFor(
    current?.xml ??
      `<ReplicationConfiguration xmlns="${S3_NS}"><Role>cairn</Role><Rule><ID>cairn-web</ID><Status>Enabled</Status></Rule></ReplicationConfiguration>`,
  );
  const rule = elements(doc.documentElement, "Rule")[0]!;
  const child = (parent: Element, name: string) => {
    let el = elements(parent, name)[0];
    if (!el) {
      el = doc.createElementNS(parent.namespaceURI, name);
      parent.append(el);
    }
    return el;
  };
  child(child(rule, "Destination"), "Bucket").textContent = destination;
  const legacyPrefix = elements(rule, "Prefix")[0];
  (legacyPrefix ?? child(child(rule, "Filter"), "Prefix")).textContent = prefix;
  child(child(rule, "ExistingObjectReplication"), "Status").textContent =
    existing ? "Enabled" : "Disabled";
  child(child(rule, "DeleteMarkerReplication"), "Status").textContent = deletes
    ? "Enabled"
    : "Disabled";
  return new XMLSerializer().serializeToString(doc);
}
