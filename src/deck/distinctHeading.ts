/**
 * The deck's own heading, when it says something the document's name does not
 * — shown as a subtitle, never as a second name. Case, spacing and dashes do
 * not count as a difference ("audit-e2e" and "Audit E2E" are the same words).
 * Pure and dependency-free so the work-graph reader can use it too.
 */
export function distinctHeading(heading: string | undefined, documentTitle: string | undefined): string | undefined {
  const trimmed = (heading ?? '').trim().slice(0, 120);
  if (!trimmed) return undefined;
  const fold = (value: string) => value.toLowerCase().replace(/[^a-z0-9]+/g, ' ').trim();
  return fold(trimmed) === fold(documentTitle ?? '') ? undefined : trimmed;
}
