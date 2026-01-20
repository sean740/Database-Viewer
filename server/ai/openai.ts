import OpenAI from "openai";

let openaiClient: OpenAI | null = null;

export function getOpenAIClient(): OpenAI | null {
  if (openaiClient) return openaiClient;

  const apiKey = process.env.AI_INTEGRATIONS_OPENAI_API_KEY;
  const baseURL = process.env.AI_INTEGRATIONS_OPENAI_BASE_URL;

  if (!apiKey || !baseURL) {
    return null;
  }

  openaiClient = new OpenAI({
    apiKey,
    baseURL,
  });

  return openaiClient;
}

export const AI_CONFIG = {
  nlq: {
    model: "gpt-4o",
    temperature: 0.1,
    maxTokens: 1200,
  },
  smartFollowup: {
    model: "gpt-4o",
    temperature: 0.3,
    maxTokens: 800,
  },
  reportChat: {
    model: "gpt-4o",
    temperature: 0.5,
    maxTokens: 1200,
  },
} as const;
