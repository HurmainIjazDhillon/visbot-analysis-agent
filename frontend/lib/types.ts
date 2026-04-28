export type AssetProfile = {
  asset_id: string;
  db_asset_id: string;
  name: string;
  asset_type: string;
  description: string;
  parent_name: string;
  child_count: number;
  is_device: boolean;
  attribute_keys: string[];
  has_live_data: boolean;
  data_keys: string[];
  supported_analyses: string[];
  metrics: string[];
  source_table: string;
  analysis_instructions: string;
  sql_notes: string;
};

export type TimeWindow = {
  scope: "live" | "historical";
  value: number;
  unit: "minutes" | "hours" | "days" | "weeks" | "months";
  label: string;
};

export type AnalysisResponse = {
  asset: AssetProfile;
  plan: {
    analysis_name: string;
    reasoning: string;
    time_window: TimeWindow;
    query_started_at: string;
    query_ended_at: string;
    query_window_label: string;
    planner_prompt: string;
    sql_prompt: string;
    analyst_prompt: string;
    sql_query: string;
    chart_hints: string[];
    warnings: string[];
    total_tokens: number;
    llm_raw_response: string;
  };
  trend_chart: {
    title: string;
    chart_type: "line" | "bar";
    x_label: string;
    y_label: string;
    summary: string;
    series: {
      key: string;
      label: string;
      color: string;
      points: {
        label: string;
        value: number;
      }[];
    }[];
  } | null;
  trend_charts?: {
    title: string;
    chart_type: "line" | "bar";
    x_label: string;
    y_label: string;
    summary: string;
    series: {
      key: string;
      label: string;
      color: string;
      points: {
        label: string;
        value: number;
      }[];
    }[];
  }[];
  report: {
    report_id: string;
    title: string;
    summary: string;
    markdown: string;
    html: string;
    created_at: string;
  };
  rows: Record<string, unknown>[];
};

export type ChatResponse = {
  answer: string;
  analysis: AnalysisResponse;
};
