// One deck the whole suite renders, covering every layout the generator knows.
import type { DeckSpec } from '../DeckSpec';

export const SOURCE_A = { documentId: 'doc-sprint-review', title: 'Sprint review' };
export const SOURCE_B = { documentId: 'doc-roadmap', anchor: 'h2-q4', title: 'Q4 roadmap' };

export function sampleDeck(): DeckSpec {
  return {
    title: 'Q4 platform review',
    subtitle: 'What shipped, what it cost, what is next',
    theme: 'product',
    slides: [
      { id: 'cover', layout: 'title', title: 'Q4 platform review', eyebrow: 'Engineering', sources: [SOURCE_A] },
      { id: 'agenda', layout: 'bullets', title: 'Agenda', bullets: ['What shipped', 'What it cost', 'What is next'], notes: 'Keep this to 40 seconds.', sources: [SOURCE_A] },
      { id: 'split', layout: 'two-column', title: 'Shipped vs deferred', columns: [['Multi-node failover', 'Deck generator'], ['Mobile shell']], sources: [SOURCE_A, SOURCE_B] },
      {
        id: 'throughput', layout: 'chart', title: 'Runs per week',
        chart: { kind: 'bar', labels: ['W1', 'W2', 'W3'], series: [{ name: 'Completed', values: [12, 18, 24] }, { name: 'Failed', values: [3, 2, 1] }] },
        bullets: ['Failures fell as the retry policy landed'],
        sources: [SOURCE_B],
      },
      {
        id: 'latency', layout: 'chart', title: 'p95 latency',
        chart: { kind: 'line', labels: ['W1', 'W2', 'W3'], series: [{ name: 'p95 ms', values: [820, 640, 410] }] },
        sources: [SOURCE_B],
      },
      { id: 'voice', layout: 'quote', title: 'From the retro', quote: { text: 'The generated deck was the first artifact nobody had to reformat.', by: 'Frank' }, sources: [SOURCE_A] },
      { id: 'screenshot', layout: 'image', title: 'The artifact pane', image: { alt: 'Slide preview with the revisions rail' }, sources: [SOURCE_A] },
      { id: 'next', layout: 'section', title: 'What is next', sources: [SOURCE_B] },
    ],
  };
}
