// platform-api/src/pipeline/image/BedrockImageGenClient.ts
//
// ImageGenClient backed by AWS Bedrock InvokeModel (Amazon Nova Canvas /
// Titan TEXT_IMAGE schema). This is the original transport that lived
// inline in ImageGenerateStepHandler, lifted behind the client seam.

import {
  BedrockRuntimeClient,
  InvokeModelCommand,
} from '@aws-sdk/client-bedrock-runtime';
import {
  ImageGenThrottlingError,
  type ImageGenClient,
  type ImageGenRequest,
  type ImageGenResult,
} from './ImageGenClient';

const DEFAULT_MODEL = 'amazon.nova-canvas-v1:0';
const THROTTLING_NAMES = new Set(['ThrottlingException', 'TooManyRequestsException']);

interface BedrockImageGenerateResponse {
  images?: string[];
  error?: string;
}

export class BedrockImageGenClient implements ImageGenClient {
  readonly provider = 'bedrock';
  private readonly client: BedrockRuntimeClient;

  constructor(client?: BedrockRuntimeClient) {
    const region =
      process.env['PIPELINE_IMAGE_AWS_REGION'] ??
      process.env['AWS_REGION'] ??
      'us-east-1';
    this.client = client ?? new BedrockRuntimeClient({ region });
  }

  async generate(req: ImageGenRequest): Promise<ImageGenResult> {
    const modelId = req.model ?? process.env['IMAGE_GENERATE_MODEL'] ?? DEFAULT_MODEL;

    const imageGenConfig: Record<string, unknown> = {
      width: req.width,
      height: req.height,
      numberOfImages: req.numImages,
    };
    if (req.seed !== undefined) imageGenConfig['seed'] = req.seed;

    const body = JSON.stringify({
      taskType: 'TEXT_IMAGE',
      textToImageParams: { text: req.prompt },
      imageGenerationConfig: imageGenConfig,
    });

    let responseBody: BedrockImageGenerateResponse;
    try {
      const response = await this.client.send(
        new InvokeModelCommand({
          modelId,
          body,
          contentType: 'application/json',
          accept: 'application/json',
        }),
      );
      const bodyText = Buffer.from(response.body).toString('utf-8');
      responseBody = JSON.parse(bodyText) as BedrockImageGenerateResponse;
    } catch (err) {
      const name = (err as Record<string, unknown>)['name'];
      if (typeof name === 'string' && THROTTLING_NAMES.has(name)) {
        throw new ImageGenThrottlingError();
      }
      throw new Error(
        `Bedrock invocation failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    }

    return { images: responseBody.images ?? [] };
  }
}
