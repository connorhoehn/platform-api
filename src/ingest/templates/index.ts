// social-api/src/ingest/templates/index.ts
//
// Pre-built pipeline definition templates for common knowledge workflows in
// the ingest subsystem. These templates can be instantiated via the
// /api/ingest/templates/:id/instantiate endpoint to create runnable pipelines
// without manual graph construction.
//
// Each template is a complete PipelineDefinition (nodes + edges) targeting
// common knowledge-management patterns: transcript extraction, COE generation,
// and agentic document review.

// ---------------------------------------------------------------------------
// Structural types — mirrors frontend/src/types/pipeline.ts shapes. The
// social-api does NOT depend on the frontend package; these definitions are
// the minimum needed to produce well-typed template graphs that the pipeline
// executor and definitions-repository accept as valid PipelineDefinition JSON.
// ---------------------------------------------------------------------------

interface TriggerNodeData {
  type: 'trigger';
  triggerType: 'manual' | 'document.finalize' | 'document.comment' | 'document.submit' | 'schedule' | 'webhook';
  documentTypeId?: string;
  schedule?: string;
  webhookPath?: string;
}

interface LLMNodeData {
  type: 'llm';
  provider: 'anthropic' | 'bedrock' | 'claude-code';
  model: string;
  systemPrompt: string;
  userPromptTemplate: string;
  temperature?: number;
  maxTokens?: number;
  streaming: boolean;
}

interface TransformNodeData {
  type: 'transform';
  transformType: 'jsonpath' | 'template' | 'javascript';
  expression: string;
  outputKey?: string;
  terminal?: boolean;
}

interface ConditionNodeData {
  type: 'condition';
  expression: string;
  label?: string;
}

interface ActionNodeData {
  type: 'action';
  actionType: 'update-document' | 'post-comment' | 'notify' | 'webhook' | 'mcp-tool' | 'sync-back';
  config: Record<string, unknown>;
  idempotent?: boolean;
  onError?: 'route-error' | 'fail-run';
}

interface ForkNodeData {
  type: 'fork';
  branchCount: number;
  branchLabels?: string[];
}

interface JoinNodeData {
  type: 'join';
  mode: 'all' | 'any' | 'n_of_m';
  n?: number;
  mergeStrategy: 'deep-merge' | 'array-collect' | 'last-writer-wins';
}

type NodeData =
  | TriggerNodeData
  | LLMNodeData
  | TransformNodeData
  | ConditionNodeData
  | ActionNodeData
  | ForkNodeData
  | JoinNodeData;

interface PipelineNode {
  id: string;
  type: string;
  position: { x: number; y: number };
  data: NodeData;
}

interface PipelineEdge {
  id: string;
  source: string;
  sourceHandle: string;
  target: string;
  targetHandle: string;
}

interface PipelineDefinition {
  id: string;
  name: string;
  description?: string;
  tags?: string[];
  version: number;
  status: 'draft' | 'published' | 'archived';
  publishedVersion?: number;
  triggerBinding?: {
    event: string;
    documentTypeId?: string;
    schedule?: string;
    webhookPath?: string;
  };
  nodes: PipelineNode[];
  edges: PipelineEdge[];
  createdAt: string;
  updatedAt: string;
  createdBy: string;
}

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

export interface PipelineTemplate {
  id: string;
  name: string;
  description: string;
  category: 'knowledge-base' | 'review' | 'transform';
  definition: PipelineDefinition;
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

const COL_W = 280;
const ROW_H = 160;
const BASE_X = 40;
const BASE_Y = 200;

function col(n: number): number {
  return BASE_X + n * COL_W;
}

function row(n: number): number {
  return BASE_Y + n * ROW_H;
}

function makeEdge(
  id: string,
  source: string,
  sourceHandle: string,
  target: string,
  targetHandle: string = 'in',
): PipelineEdge {
  return { id, source, sourceHandle, target, targetHandle };
}

// ---------------------------------------------------------------------------
// 1. Meeting Transcript -> Stakeholder Registry
// ---------------------------------------------------------------------------

function buildTranscriptToStakeholderRegistry(): PipelineDefinition {
  const now = new Date().toISOString();

  const triggerId = 'tpl-stakeholder-trigger';
  const extractLlmId = 'tpl-stakeholder-extract-llm';
  const transformId = 'tpl-stakeholder-transform';
  const createDocId = 'tpl-stakeholder-create-doc';

  const triggerData: TriggerNodeData = {
    type: 'trigger',
    triggerType: 'document.finalize',
    documentTypeId: 'meeting-transcript',
  };

  const extractLlm: LLMNodeData = {
    type: 'llm',
    provider: 'bedrock',
    model: 'claude-sonnet-4-6',
    systemPrompt:
      'You are a knowledge-management assistant specializing in stakeholder analysis. ' +
      'Read the provided meeting transcript and extract every person mentioned along with ' +
      'their role, responsibilities, and any decisions they own. Output strict JSON:\n' +
      '{"stakeholders": [{"name": string, "role": string, "responsibilities": string[], "decisions": string[]}]}',
    userPromptTemplate: '{{context.documentBody}}',
    temperature: 0.1,
    maxTokens: 2048,
    streaming: false,
  };

  const transformData: TransformNodeData = {
    type: 'transform',
    transformType: 'javascript',
    expression:
      '(() => { ' +
      'const raw = context.steps["tpl-stakeholder-extract-llm"]?.response || "{}"; ' +
      'try { const parsed = JSON.parse(raw); return { stakeholders: parsed.stakeholders || [] }; } ' +
      'catch { return { stakeholders: [] }; } ' +
      '})()',
    outputKey: 'registryFields',
  };

  const createDocData: ActionNodeData = {
    type: 'action',
    actionType: 'update-document',
    config: {
      operation: 'create',
      documentType: 'Stakeholder Registry',
      title: 'Stakeholder Registry - {{context.documentTitle}}',
      fields: {
        stakeholders: '{{context.registryFields.stakeholders}}',
        sourceTranscriptId: '{{context.documentId}}',
        extractedAt: '{{context.extractedAt}}',
      },
    },
    idempotent: true,
    onError: 'fail-run',
  };

  const nodes: PipelineNode[] = [
    { id: triggerId, type: 'trigger', position: { x: col(0), y: row(0) }, data: triggerData },
    { id: extractLlmId, type: 'llm', position: { x: col(1), y: row(0) }, data: extractLlm },
    { id: transformId, type: 'transform', position: { x: col(2), y: row(0) }, data: transformData },
    { id: createDocId, type: 'action', position: { x: col(3), y: row(0) }, data: createDocData },
  ];

  const edges: PipelineEdge[] = [
    makeEdge('e-stakeholder-1', triggerId, 'out', extractLlmId),
    makeEdge('e-stakeholder-2', extractLlmId, 'out', transformId),
    makeEdge('e-stakeholder-3', transformId, 'out', createDocId),
  ];

  return {
    id: 'tpl-transcript-to-stakeholder-registry',
    name: 'Meeting Transcript to Stakeholder Registry',
    description:
      'Analyzes a meeting transcript to extract stakeholders with their roles and responsibilities, then creates a Stakeholder Registry document.',
    version: 1,
    status: 'published',
    publishedVersion: 1,
    triggerBinding: { event: 'document.finalize', documentTypeId: 'meeting-transcript' },
    nodes,
    edges,
    createdAt: now,
    updatedAt: now,
    createdBy: 'system',
  };
}

// ---------------------------------------------------------------------------
// 2. Meeting Transcript -> Center of Excellence Document
// ---------------------------------------------------------------------------

function buildTranscriptToCOE(): PipelineDefinition {
  const now = new Date().toISOString();

  const triggerId = 'tpl-coe-trigger';
  const extractLlmId = 'tpl-coe-extract-llm';
  const generateLlmId = 'tpl-coe-generate-llm';
  const createDocId = 'tpl-coe-create-doc';

  const triggerData: TriggerNodeData = {
    type: 'trigger',
    triggerType: 'document.finalize',
    documentTypeId: 'meeting-transcript',
  };

  const extractLlm: LLMNodeData = {
    type: 'llm',
    provider: 'bedrock',
    model: 'claude-sonnet-4-6',
    systemPrompt:
      'You are an organizational knowledge analyst. Read this meeting transcript and extract:\n' +
      '1. Key decisions made\n' +
      '2. Action items with owners\n' +
      '3. Best practices discussed\n' +
      '4. Standards or guidelines proposed\n' +
      '5. Governance items (policies, escalation paths, ownership)\n\n' +
      'Output strict JSON:\n' +
      '{"decisions": string[], "actionItems": [{"owner": string, "item": string, "deadline": string|null}], ' +
      '"bestPractices": string[], "standards": string[], "governance": string[]}',
    userPromptTemplate: '{{context.documentBody}}',
    temperature: 0.1,
    maxTokens: 2048,
    streaming: false,
  };

  const generateLlm: LLMNodeData = {
    type: 'llm',
    provider: 'bedrock',
    model: 'claude-sonnet-4-6',
    systemPrompt:
      'You are a Center of Excellence (COE) document author. Using the extracted meeting insights, ' +
      'generate a structured COE section document with the following format:\n\n' +
      '## Purpose\n[Why this COE section exists based on the meeting context]\n\n' +
      '## Scope\n[What areas/teams this covers]\n\n' +
      '## Standards\n[Bullet list of standards and best practices]\n\n' +
      '## Governance\n[Who owns what, escalation paths, review cadence]\n\n' +
      '## Action Items\n[Table of actions, owners, deadlines]\n\n' +
      'Be concise but thorough. Use the extracted data directly.',
    userPromptTemplate:
      'Extracted insights from meeting transcript:\n\n{{context.steps.tpl-coe-extract-llm.response}}',
    temperature: 0.3,
    maxTokens: 3000,
    streaming: true,
  };

  const createDocData: ActionNodeData = {
    type: 'action',
    actionType: 'update-document',
    config: {
      operation: 'create',
      documentType: 'Center of Excellence',
      title: 'COE - {{context.documentTitle}}',
      fields: {
        body: '{{context.steps.tpl-coe-generate-llm.response}}',
        sourceTranscriptId: '{{context.documentId}}',
        generatedAt: '{{context.generatedAt}}',
        status: 'draft',
      },
    },
    idempotent: true,
    onError: 'fail-run',
  };

  const nodes: PipelineNode[] = [
    { id: triggerId, type: 'trigger', position: { x: col(0), y: row(0) }, data: triggerData },
    { id: extractLlmId, type: 'llm', position: { x: col(1), y: row(0) }, data: extractLlm },
    { id: generateLlmId, type: 'llm', position: { x: col(2), y: row(0) }, data: generateLlm },
    { id: createDocId, type: 'action', position: { x: col(3), y: row(0) }, data: createDocData },
  ];

  const edges: PipelineEdge[] = [
    makeEdge('e-coe-1', triggerId, 'out', extractLlmId),
    makeEdge('e-coe-2', extractLlmId, 'out', generateLlmId),
    makeEdge('e-coe-3', generateLlmId, 'out', createDocId),
  ];

  return {
    id: 'tpl-transcript-to-coe',
    name: 'Meeting Transcript to Center of Excellence',
    description:
      'Extracts decisions, best practices, and governance items from a meeting transcript, ' +
      'then generates a structured Center of Excellence document.',
    version: 1,
    status: 'published',
    publishedVersion: 1,
    triggerBinding: { event: 'document.finalize', documentTypeId: 'meeting-transcript' },
    nodes,
    edges,
    createdAt: now,
    updatedAt: now,
    createdBy: 'system',
  };
}

// ---------------------------------------------------------------------------
// 3. Agentic Document Review
// ---------------------------------------------------------------------------

function buildAgenticDocumentReview(): PipelineDefinition {
  const now = new Date().toISOString();

  const triggerId = 'tpl-review-trigger';
  const reviewLlmId = 'tpl-review-llm';
  const conditionId = 'tpl-review-condition';
  const forkId = 'tpl-review-fork';
  const postCommentId = 'tpl-review-post-comment';
  const improveLlmId = 'tpl-review-improve-llm';
  const updateDocId = 'tpl-review-update-doc';
  const joinId = 'tpl-review-join';
  const notifyId = 'tpl-review-notify';

  const triggerData: TriggerNodeData = {
    type: 'trigger',
    triggerType: 'document.finalize',
    documentTypeId: 'needs-review',
  };

  const reviewLlm: LLMNodeData = {
    type: 'llm',
    provider: 'bedrock',
    model: 'claude-sonnet-4-6',
    systemPrompt:
      'You are a senior document reviewer. Analyze this document for:\n' +
      '1. Completeness - are there gaps in coverage?\n' +
      '2. Accuracy - are there factual inconsistencies or unsupported claims?\n' +
      '3. Style - is the tone consistent, is it well-structured?\n' +
      '4. Actionability - are recommendations clear and implementable?\n\n' +
      'Output strict JSON:\n' +
      '{"hasSuggestions": boolean, "overallScore": number, "findings": ' +
      '[{"category": "completeness"|"accuracy"|"style"|"actionability", ' +
      '"severity": "high"|"medium"|"low", "location": string, "issue": string, ' +
      '"suggestion": string}], "summary": string}',
    userPromptTemplate: '{{context.documentBody}}',
    temperature: 0.2,
    maxTokens: 3000,
    streaming: false,
  };

  const conditionData: ConditionNodeData = {
    type: 'condition',
    expression: 'context.steps["tpl-review-llm"].output.hasSuggestions === true',
    label: 'Has suggestions?',
  };

  const forkData: ForkNodeData = {
    type: 'fork',
    branchCount: 2,
    branchLabels: ['comments', 'improved-version'],
  };

  const postCommentData: ActionNodeData = {
    type: 'action',
    actionType: 'post-comment',
    config: {
      body: '## Document Review Results\n\n' +
        'Score: {{context.steps.tpl-review-llm.output.overallScore}}/10\n\n' +
        '{{context.steps.tpl-review-llm.output.summary}}\n\n' +
        '### Findings\n{{context.steps.tpl-review-llm.output.findings}}',
    },
    idempotent: true,
    onError: 'route-error',
  };

  const improveLlm: LLMNodeData = {
    type: 'llm',
    provider: 'bedrock',
    model: 'claude-sonnet-4-6',
    systemPrompt:
      'You are a document improvement specialist. Given the original document and the review findings, ' +
      'produce an improved version that addresses all high and medium severity findings. ' +
      'Preserve the original structure and voice. Only make changes that directly address the findings. ' +
      'Output the full improved document text.',
    userPromptTemplate:
      'Original document:\n{{context.documentBody}}\n\n' +
      'Review findings:\n{{context.steps.tpl-review-llm.response}}',
    temperature: 0.3,
    maxTokens: 4096,
    streaming: true,
  };

  const updateDocData: ActionNodeData = {
    type: 'action',
    actionType: 'update-document',
    config: {
      field: 'suggestedRevision',
      value: '{{context.steps.tpl-review-improve-llm.response}}',
    },
    idempotent: true,
    onError: 'route-error',
  };

  const joinData: JoinNodeData = {
    type: 'join',
    mode: 'all',
    mergeStrategy: 'deep-merge',
  };

  const notifyData: ActionNodeData = {
    type: 'action',
    actionType: 'notify',
    config: {
      channel: 'document-owner',
      message:
        'Your document "{{context.documentTitle}}" has been reviewed. ' +
        'Score: {{context.steps.tpl-review-llm.output.overallScore}}/10. ' +
        'Check the comments for detailed findings and a suggested improved version.',
    },
    idempotent: false,
    onError: 'route-error',
  };

  const nodes: PipelineNode[] = [
    { id: triggerId, type: 'trigger', position: { x: col(0), y: row(0) }, data: triggerData },
    { id: reviewLlmId, type: 'llm', position: { x: col(1), y: row(0) }, data: reviewLlm },
    { id: conditionId, type: 'condition', position: { x: col(2), y: row(0) }, data: conditionData },
    { id: forkId, type: 'fork', position: { x: col(3), y: row(0) }, data: forkData },
    { id: postCommentId, type: 'action', position: { x: col(4), y: row(0) - ROW_H / 2 }, data: postCommentData },
    { id: improveLlmId, type: 'llm', position: { x: col(4), y: row(0) + ROW_H / 2 }, data: improveLlm },
    { id: updateDocId, type: 'action', position: { x: col(5), y: row(0) + ROW_H / 2 }, data: updateDocData },
    { id: joinId, type: 'join', position: { x: col(6), y: row(0) }, data: joinData },
    { id: notifyId, type: 'action', position: { x: col(7), y: row(0) }, data: notifyData },
  ];

  const edges: PipelineEdge[] = [
    makeEdge('e-review-1', triggerId, 'out', reviewLlmId),
    makeEdge('e-review-2', reviewLlmId, 'out', conditionId),
    makeEdge('e-review-3', conditionId, 'true', forkId),
    makeEdge('e-review-4', forkId, 'branch-0', postCommentId),
    makeEdge('e-review-5', forkId, 'branch-1', improveLlmId),
    makeEdge('e-review-6', improveLlmId, 'out', updateDocId),
    makeEdge('e-review-7', postCommentId, 'out', joinId, 'in-0'),
    makeEdge('e-review-8', updateDocId, 'out', joinId, 'in-1'),
    makeEdge('e-review-9', joinId, 'out', notifyId),
  ];

  return {
    id: 'tpl-agentic-document-review',
    name: 'Agentic Document Review',
    description:
      'Reviews a document for completeness, accuracy, and style. If issues are found, ' +
      'posts review comments and generates an improved version in parallel, then notifies the owner.',
    version: 1,
    status: 'published',
    publishedVersion: 1,
    triggerBinding: { event: 'document.finalize', documentTypeId: 'needs-review' },
    nodes,
    edges,
    createdAt: now,
    updatedAt: now,
    createdBy: 'system',
  };
}

// ---------------------------------------------------------------------------
// Registry
// ---------------------------------------------------------------------------

export const ingestPipelineTemplates: PipelineTemplate[] = [
  {
    id: 'transcript-to-stakeholder-registry',
    name: 'Meeting Transcript to Stakeholder Registry',
    description:
      'Analyzes a meeting transcript to extract stakeholders with their roles and responsibilities, then creates a Stakeholder Registry document.',
    category: 'knowledge-base',
    definition: buildTranscriptToStakeholderRegistry(),
  },
  {
    id: 'transcript-to-coe',
    name: 'Meeting Transcript to Center of Excellence',
    description:
      'Extracts decisions, best practices, and governance items from a meeting transcript, then generates a structured Center of Excellence document.',
    category: 'knowledge-base',
    definition: buildTranscriptToCOE(),
  },
  {
    id: 'agentic-document-review',
    name: 'Agentic Document Review',
    description:
      'Reviews a document for completeness, accuracy, and style. If issues are found, posts review comments and generates an improved version in parallel, then notifies the owner.',
    category: 'review',
    definition: buildAgenticDocumentReview(),
  },
];
