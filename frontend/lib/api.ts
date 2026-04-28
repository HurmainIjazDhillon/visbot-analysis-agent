import { AnalysisResponse, ChatResponse } from "./types";

const API_BASE_URL =
  process.env.NEXT_PUBLIC_API_BASE_URL ?? "http://localhost:8000";

export async function sendChatMessage(input: {
  message: string;
  asset_id?: string;
}): Promise<ChatResponse> {
  const response = await fetch(`${API_BASE_URL}/api/v1/chat/message`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(input),
  });

  if (!response.ok) {
    let detail = "Failed to get analysis";
    try {
      const body = await response.json();
      detail = body.detail || body.message || detail;
    } catch {
      // Keep the fallback message when the response body is not JSON.
    }
    throw new Error(detail);
  }

  return response.json();
}

export async function runAnalysis(input: {
  asset_id: string;
  question: string;
  analysis_name?: string;
}): Promise<AnalysisResponse> {
  const response = await fetch(`${API_BASE_URL}/api/v1/analysis/run`, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(input),
  });

  if (!response.ok) {
    throw new Error("Failed to run analysis");
  }

  return response.json();
}
