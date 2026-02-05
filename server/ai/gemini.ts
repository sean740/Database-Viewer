
import { GoogleGenerativeAI, HarmCategory, HarmBlockThreshold } from "@google/generative-ai";

let genAI: GoogleGenerativeAI | null = null;

export function getGeminiClient(): GoogleGenerativeAI | null {
    if (genAI) return genAI;

    const apiKey = process.env.GOOGLE_API_KEY;

    // DEBUG LOGGING
    console.log("[DEBUG] getGeminiClient called");
    console.log("[DEBUG] Current working directory:", process.cwd());
    console.log("[DEBUG] GOOGLE_API_KEY present:", !!apiKey);
    if (apiKey) {
        console.log("[DEBUG] GOOGLE_API_KEY length:", apiKey.length);
        console.log("[DEBUG] GOOGLE_API_KEY start:", apiKey.substring(0, 5) + "...");
    } else {
        console.log("[DEBUG] GOOGLE_API_KEY is missing from process.env");
        // Print all env keys to see what's available (security: keys only)
        console.log("[DEBUG] Available Env Keys:", Object.keys(process.env).filter(k => !k.includes("KEY") && !k.includes("SECRET")));
    }

    if (!apiKey) {
        return null;
    }

    genAI = new GoogleGenerativeAI(apiKey);
    return genAI;
}

export const GEMINI_CONFIG = {
    modelName: "gemini-flash-latest",
    // Safety settings to be permissive for data analysis (avoid blocking SQL or technical terms)
    safetySettings: [
        {
            category: HarmCategory.HARM_CATEGORY_HARASSMENT,
            threshold: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        },
        {
            category: HarmCategory.HARM_CATEGORY_HATE_SPEECH,
            threshold: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        },
        {
            category: HarmCategory.HARM_CATEGORY_SEXUALLY_EXPLICIT,
            threshold: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        },
        {
            category: HarmCategory.HARM_CATEGORY_DANGEROUS_CONTENT,
            threshold: HarmBlockThreshold.BLOCK_MEDIUM_AND_ABOVE,
        },
    ],
    generationConfig: {
        temperature: 0.2, // Lower temperature for more deterministic analysis
        maxOutputTokens: 2000,
    }
};
