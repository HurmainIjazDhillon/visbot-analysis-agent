"use client";

import { FormEvent, KeyboardEvent, ReactNode, useEffect, useMemo, useState, useTransition } from "react";

import { SplineSceneBasic } from "@/components/ui/spline-scene-basic";
import { SparklesText } from "@/components/ui/sparkles-text";
import { sendChatMessage } from "../lib/api";
import { AnalysisResponse } from "../lib/types";

type ChatItem = {
  role: "user" | "assistant";
  content: string;
  analysis?: AnalysisResponse;
};

type Conversation = {
  id: string;
  title: string;
  items: ChatItem[];
  analysis?: AnalysisResponse;
};

const starterPrompts = [
  "Provide analysis on Physical Refinery.",
  "Analyze Coldroom 1 last 1 hour",
  "Scheduled T&H Monitoring",
  "Scheduled Filling Machines",
  "Scheduled T&H Monitoring for yesterday",
];

function createConversationId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(36).slice(2, 10)}`;
}

function formatQueryWindow(analysis: AnalysisResponse): string {
  return analysis.plan.query_window_label || analysis.plan.time_window.label;
}

function formatElapsed(seconds: number): string {
  const mins = Math.floor(seconds / 60);
  const secs = seconds % 60;
  return `${mins}:${secs.toString().padStart(2, "0")}`;
}

function summarizeFailure(errorMessage: string): string {
  const text = errorMessage.trim();
  const lower = text.toLowerCase();

  if (lower.includes("failed to fetch") || lower.includes("networkerror")) {
    return [
      "I could not reach the backend service.",
      "Summary: The request failed at network level before analysis could run.",
      "Check: backend server is running, API base URL is correct, and CORS is allowed.",
    ].join("\n");
  }

  if (lower.includes("timeout") || lower.includes("timed out")) {
    return [
      "The backend request timed out.",
      "Summary: The analysis took too long and the connection closed before completion.",
      "Check: retry with a shorter window, or increase backend timeout limits.",
    ].join("\n");
  }

  if (lower.includes("database") || lower.includes("sql") || lower.includes("query")) {
    return [
      "The backend could not complete the data query.",
      `Summary: ${text}`,
      "Check: asset mapping, SQL generation, and database availability.",
    ].join("\n");
  }

  return [
    "The backend could not complete this analysis request.",
    `Summary: ${text || "Unknown backend error."}`,
    "Check: backend logs for the exact failure point.",
  ].join("\n");
}

function renderInline(text: string): ReactNode[] {
  const pattern = /(\*\*[^*]+\*\*|\*[^*]+\*|`[^`]+`|<br\s*\/?>)/gi;
  const parts = text.split(pattern).filter(Boolean);

  return parts.map((part, index) => {
    if (part.startsWith("**") && part.endsWith("**")) {
      return <strong key={`${part}-${index}`}>{part.slice(2, -2)}</strong>;
    }
    if (part.startsWith("*") && part.endsWith("*")) {
      return <em key={`${part}-${index}`}>{part.slice(1, -1)}</em>;
    }
    if (part.startsWith("`") && part.endsWith("`")) {
      return <code className="inline-code" key={`${part}-${index}`}>{part.slice(1, -1)}</code>;
    }
    if (part.toLowerCase().match(/^<br\s*\/?>$/)) {
      return <br key={`br-${index}`} />;
    }
    return part;
  });
}

function renderMarkdownish(content: string): ReactNode {
  const blocks = content.trim().split(/\n\s*\n/).filter(Boolean);

  return blocks.map((block, blockIndex) => {
    const lines = block
      .split("\n")
      .map((line) => line.trimEnd())
      .filter((line) => !/^\s*([*-]|---+)\s*$/.test(line));
    const trimmed = block.trim();

    if (!lines.length) {
      return null;
    }

    if (/^#{1,6}\s+/.test(trimmed) && !trimmed.includes("\n")) {
      const headingLevel = Math.min(trimmed.match(/^#+/)?.[0].length || 3, 6);
      const headingText = trimmed.replace(/^#{1,6}\s+/, "");
      const className =
        headingLevel <= 2 ? "formatted-heading formatted-heading-large" : "formatted-heading";

      const isAiInsights = headingText.toLowerCase().includes("ai insights");

      return (
        <div className={className} key={blockIndex} style={{ display: 'flex', alignItems: 'center', gap: '8px' }}>
          {isAiInsights && (
            <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" style={{ flexShrink: 0 }}>
              <path d="M12 2a8 8 0 0 0-8 8c0 5.4 4.5 10.6 7.3 11.8a2 2 0 0 0 1.4 0C15.5 20.6 20 15.4 20 10a8 8 0 0 0-8-8z"></path>
              <circle cx="12" cy="10" r="3"></circle>
              <path d="M14 18l-4-4"></path>
              <path d="M10 18l4-4"></path>
            </svg>
          )}
          {renderInline(headingText)}
        </div>
      );
    }

    if (/^\*\*.*\*\*$/.test(trimmed) && !trimmed.includes("\n")) {
      return (
        <h3 className="formatted-heading" key={blockIndex}>
          {trimmed.slice(2, -2)}
        </h3>
      );
    }

    const tableLines = lines.filter((line) => line.trim().startsWith("|"));
    if (tableLines.length >= 2) {
      const rows = tableLines
        .map((line) =>
          line
            .split("|")
            .map((cell) => cell.trim())
            .filter(Boolean)
        )
        .filter((row) => row.length > 0);

      const filteredRows = rows.filter(
        (row) => !row.every((cell) => /^:?-+:?$/.test(cell))
      );

      if (filteredRows.length >= 2) {
        const [header, ...body] = filteredRows;
        return (
          <div className="table-wrap" key={blockIndex}>
            <table className="response-table">
              <thead>
                <tr>
                  {header.map((cell) => (
                    <th key={cell}>{renderInline(cell)}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {body.map((row, rowIndex) => {
                  let rowStatusClass = "";
                  const joinedRow = row.join(" ").toLowerCase();
                  if (joinedRow.includes("🔴 offline")) {
                    rowStatusClass = "row-offline";
                  } else if (joinedRow.includes("🟡 not working")) {
                    rowStatusClass = "row-warning";
                  } else if (joinedRow.includes("🟢 online")) {
                    rowStatusClass = "row-online";
                  }

                  return (
                    <tr key={rowIndex} className={rowStatusClass}>
                      {row.map((cell, cellIndex) => (
                        <td key={`${rowIndex}-${cellIndex}`}>{renderInline(cell)}</td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        );
      }
    }

    if (lines.every((line) => /^\d+\./.test(line) || line.length === 0)) {
      return (
        <ol className="formatted-list" key={blockIndex}>
          {lines
            .filter(Boolean)
            .map((line, index) => (
              <li key={index}>{renderInline(line.replace(/^\d+\.\s*/, ""))}</li>
            ))}
        </ol>
      );
    }

    if (lines.every((line) => line.startsWith("-") || line.startsWith("* ") || line.length === 0)) {
      return (
        <ul className="formatted-list" key={blockIndex}>
          {lines
            .filter(Boolean)
            .map((line, index) => (
              <li key={index}>{renderInline(line.replace(/^(-+|\*)\s*/, ""))}</li>
            ))}
        </ul>
      );
    }

    return (
      <div className="formatted-block" key={blockIndex}>
        {lines.map((line, index) =>
          line ? (
            <p className="formatted-paragraph" key={index}>
              {renderInline(line)}
            </p>
          ) : null
        )}
      </div>
    );
  });
}

function TrendChartView({ chart }: { chart: NonNullable<AnalysisResponse["trend_chart"]> }) {
  const [hoveredBar, setHoveredBar] = useState<{
    x: number;
    y: number;
    category: string;
    assetName: string;
    value: number;
    indexLabel: string;
  } | null>(null);
  const [hoveredLinePoint, setHoveredLinePoint] = useState<{
    x: number;
    y: number;
    category: string;
    pointLabel: string;
    value: number;
  } | null>(null);

  if (!chart || !chart.series.length) {
    return null;
  }

  const baseWidth = 920;
  const height = 320;
  const padding = 30;
  const allPoints = chart.series.flatMap((series) => series.points || []);
  if (!allPoints.length) {
    return null;
  }
  const parseLabelDate = (label: string): number => {
    const parsed = new Date(label);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.getTime();
    }
    const monthMap: Record<string, number> = {
      jan: 0, feb: 1, mar: 2, apr: 3, may: 4, jun: 5,
      jul: 6, aug: 7, sep: 8, oct: 9, nov: 10, dec: 11,
    };
    const match = label.match(/^(\d{1,2})\s+([A-Za-z]{3})\s+(\d{1,2}):(\d{2})\s*(AM|PM)$/i);
    if (!match) {
      return Number.NaN;
    }
    const day = Number(match[1]);
    const month = monthMap[match[2].toLowerCase()];
    const minute = Number(match[4]);
    const ampm = match[5].toUpperCase();
    let hour = Number(match[3]);
    if (ampm === "PM" && hour < 12) hour += 12;
    if (ampm === "AM" && hour === 12) hour = 0;
    const year = new Date().getFullYear();
    return new Date(year, month, day, hour, minute, 0, 0).getTime();
  };

  const labels = Array.from(
    new Set(
      allPoints
        .map((point) => (point && typeof point.label === "string" ? point.label : ""))
        .filter(Boolean)
    )
  ).sort((a, b) => {
    const ta = parseLabelDate(a);
    const tb = parseLabelDate(b);
    if (!Number.isNaN(ta) && !Number.isNaN(tb)) {
      return ta - tb;
    }
    if (!Number.isNaN(ta)) return -1;
    if (!Number.isNaN(tb)) return 1;
    return a.localeCompare(b);
  });
  if (!labels.length) {
    return null;
  }
  const values = allPoints
    .map((point) => point.value)
    .filter((value): value is number => Number.isFinite(value));
  if (!values.length) {
    return null;
  }
  const minValue = Math.min(...values);
  const maxValue = Math.max(...values);
  const range = maxValue - minValue || 1;
  const maxPointsPerSeries = Math.max(...chart.series.map((series) => (series.points || []).length));
  const shouldUseBarsForSnapshot = chart.chart_type === "line" && maxPointsPerSeries <= 1;
  const isBarMode = shouldUseBarsForSnapshot || chart.chart_type === "bar";
  const width = isBarMode ? Math.max(baseWidth, labels.length * 56) : baseWidth;
  const innerWidth = width - padding * 2;
  const innerHeight = height - padding * 2;
  const xForIndex = (index: number) =>
    padding + (labels.length === 1 ? innerWidth / 2 : (index / (labels.length - 1)) * innerWidth);
  const yForValue = (value: number) =>
    height - padding - ((value - minValue) / range) * innerHeight;
  const tickIndexes = Array.from(
    new Set(
      [
        0,
        Math.floor((labels.length - 1) * 0.25),
        Math.floor((labels.length - 1) * 0.5),
        Math.floor((labels.length - 1) * 0.75),
        labels.length - 1,
      ].filter((value) => value >= 0)
    )
  );
  const formatTickLabel = (raw: string): string => {
    const text = raw.trim();
    const parsed = new Date(text);
    if (!Number.isNaN(parsed.getTime())) {
      return parsed.toLocaleString("en-GB", {
        day: "2-digit",
        month: "short",
        hour: "2-digit",
        minute: "2-digit",
        hour12: true,
      });
    }
    return text.slice(0, 18);
  };
  const barValueRows =
    isBarMode && chart.series.length === 1
      ? chart.series[0].points
          .filter((point) => Number.isFinite(point.value))
          .map((point) => ({ name: point.label, value: point.value }))
      : [];
  const isCoolingMinutes = chart.y_label.toLowerCase().includes("cooling") && chart.y_label.toLowerCase().includes("min");
  const formatValue = (value: number) => {
    if (isCoolingMinutes) {
      const totalMinutes = Math.max(0, Math.round(value));
      const hours = Math.floor(totalMinutes / 60);
      const minutes = totalMinutes % 60;
      return `${hours} hr ${minutes} min`;
    }
    return value.toFixed(2);
  };
  const allValueRows = isBarMode
    ? chart.series.flatMap((series) =>
        (series.points || [])
          .filter((point) => Number.isFinite(point.value))
          .map((point) => ({
            category: series.label,
            name: point.label,
            value: point.value,
            color: series.color,
          }))
      )
    : chart.series
        .map((series) => {
          const validPoints = (series.points || []).filter((point) => Number.isFinite(point.value));
          if (!validPoints.length) {
            return null;
          }
          const latestPoint = validPoints[validPoints.length - 1];
          return {
            category: series.label,
            name: latestPoint.label,
            value: latestPoint.value,
            color: series.color,
          };
        })
        .filter((row): row is { category: string; name: string; value: number; color: string } => row !== null);

  return (
    <div className="trend-card">
      <div className="trend-card-head">
        <div>
          <strong>{chart.title}</strong>
          <div className="subtle trend-summary">{chart.summary}</div>
        </div>
      </div>

      <div className="trend-legend">
        {chart.series.map((series) => (
          <div className="trend-legend-item" key={series.key}>
            <span className="trend-legend-dot" style={{ backgroundColor: series.color }} />
            <span>{series.label}</span>
          </div>
        ))}
      </div>

      <div className="trend-svg-wrap">
        <svg
          viewBox={`0 0 ${width} ${height}`}
          className="trend-svg"
          role="img"
          aria-label={chart.title}
          style={isBarMode ? { width: `${width}px` } : undefined}
        >
          {[0, 1, 2, 3].map((tick) => {
            const value = maxValue - (range / 3) * tick;
            const y = padding + (innerHeight / 3) * tick;
            return (
              <g key={tick}>
                <line x1={padding} x2={width - padding} y1={y} y2={y} className="trend-grid-line" />
                <text x={6} y={y + 4} className="trend-axis-label">
                  {value.toFixed(1)}
                </text>
              </g>
            );
          })}

          {isBarMode
            ? labels.map((label, index) => (
                <g key={`${chart.title}-bar-label-${index}`}>
                  <text
                    x={xForIndex(index)}
                    y={height - 10}
                    textAnchor="middle"
                    className="trend-axis-label"
                  >
                    #{index + 1}
                  </text>
                  <title>{label}</title>
                </g>
              ))
            : tickIndexes.map((index) => {
                const x = xForIndex(index);
                const isFirst = index === tickIndexes[0];
                const isLast = index === tickIndexes[tickIndexes.length - 1];
                return (
                  <text
                    key={`${chart.title}-${index}`}
                    x={isFirst ? x + 4 : isLast ? x - 4 : x}
                    y={height - 6}
                    textAnchor={isFirst ? "start" : isLast ? "end" : "middle"}
                    className="trend-axis-label"
                  >
                    {formatTickLabel(labels[index] ?? "")}
                  </text>
                );
              })}

          {isBarMode
            ? chart.series.map((series, seriesIndex) => {
                const barWidth = Math.max(
                  10,
                  Math.min(
                    28,
                    innerWidth / Math.max(labels.length, 1) / Math.max(chart.series.length, 1) - 8
                  )
                );
                return (
                  <g key={series.key}>
                    {(series.points || []).map((point, pointIndex) => {
                      const labelIndex = labels.indexOf(point.label);
                      if (labelIndex < 0 || !Number.isFinite(point.value)) {
                        return null;
                      }
                      const centerX = xForIndex(labelIndex);
                      const seriesOffset =
                        seriesIndex * (barWidth + 4) - ((chart.series.length - 1) * (barWidth + 4)) / 2;
                      const y = yForValue(point.value);
                      const barHeight = height - padding - y;
                      return (
                        <rect
                          key={`${series.key}-${pointIndex}`}
                          x={centerX - barWidth / 2 + seriesOffset}
                          y={y}
                          width={barWidth}
                          height={barHeight}
                          rx={8}
                          fill={series.color}
                          fillOpacity="0.85"
                          onMouseEnter={() =>
                            setHoveredBar({
                              x: centerX + seriesOffset,
                              y: y,
                              category: series.label,
                              assetName: point.label,
                              value: point.value,
                              indexLabel: `#${labelIndex + 1}`,
                            })
                          }
                          onMouseLeave={() => setHoveredBar(null)}
                        />
                      );
                    })}
                  </g>
                );
              })
            : chart.series.map((series) => {
                const points = labels
                  .map((label, labelIndex) => {
                    const point = (series.points || []).find((entry) => entry.label === label);
                    if (!point || !Number.isFinite(point.value)) {
                      return null;
                    }
                    return {
                      x: xForIndex(labelIndex),
                      y: yForValue(point.value),
                      label: point.label,
                      value: point.value,
                    };
                  })
                  .filter(
                    (point): point is { x: number; y: number; label: string; value: number } => point !== null
                  );

                if (!points.length) {
                  return null;
                }
                const path = points
                  .map((point, pointIndex) => `${pointIndex === 0 ? "M" : "L"} ${point.x} ${point.y}`)
                  .join(" ");

                return (
                  <g key={series.key}>
                    <path
                      d={path}
                      fill="none"
                      stroke={series.color}
                      strokeWidth="3.5"
                      strokeLinecap="round"
                      strokeLinejoin="round"
                    />
                    {points
                      .map((point, pointIndex) => {
                        return (
                          <g key={`${series.key}-${pointIndex}`}>
                            <circle
                              cx={point.x}
                              cy={point.y}
                              r="2.4"
                              fill={series.color}
                              pointerEvents="none"
                            />
                            <circle
                              cx={point.x}
                              cy={point.y}
                              r="8"
                              fill="transparent"
                              style={{ cursor: "pointer" }}
                              onMouseEnter={() =>
                                setHoveredLinePoint({
                                  x: point.x,
                                  y: point.y,
                                  category: series.label,
                                  pointLabel: point.label,
                                  value: point.value,
                                })
                              }
                              onMouseMove={() =>
                                setHoveredLinePoint({
                                  x: point.x,
                                  y: point.y,
                                  category: series.label,
                                  pointLabel: point.label,
                                  value: point.value,
                                })
                              }
                              onMouseLeave={() => setHoveredLinePoint(null)}
                            />
                          </g>
                        );
                      })}
                  </g>
                );
              })}
          {hoveredBar && isBarMode ? (
            <g>
              <rect
                x={Math.max(padding, hoveredBar.x - 125)}
                y={Math.max(10, hoveredBar.y - 82)}
                width={250}
                height={72}
                rx={10}
                fill="rgba(14, 28, 53, 0.94)"
              />
              <text
                x={Math.max(padding, hoveredBar.x - 115)}
                y={Math.max(26, hoveredBar.y - 62)}
                className="trend-tooltip-title"
              >
                {hoveredBar.indexLabel} {hoveredBar.assetName}
              </text>
              <text
                x={Math.max(padding, hoveredBar.x - 115)}
                y={Math.max(42, hoveredBar.y - 44)}
                className="trend-tooltip-text"
              >
                Category: {hoveredBar.category}
              </text>
              <text
                x={Math.max(padding, hoveredBar.x - 115)}
                y={Math.max(58, hoveredBar.y - 26)}
                className="trend-tooltip-text"
              >
                Value: {formatValue(hoveredBar.value)} {isCoolingMinutes ? "" : chart.y_label}
              </text>
            </g>
          ) : null}
          {hoveredLinePoint && !isBarMode ? (
            <g>
              <rect
                x={Math.max(padding, hoveredLinePoint.x - 120)}
                y={Math.max(10, hoveredLinePoint.y - 78)}
                width={240}
                height={68}
                rx={10}
                fill="rgba(14, 28, 53, 0.94)"
              />
              <text
                x={Math.max(padding, hoveredLinePoint.x - 110)}
                y={Math.max(26, hoveredLinePoint.y - 58)}
                className="trend-tooltip-title"
              >
                {hoveredLinePoint.category}
              </text>
              <text
                x={Math.max(padding, hoveredLinePoint.x - 110)}
                y={Math.max(42, hoveredLinePoint.y - 40)}
                className="trend-tooltip-text"
              >
                Time: {hoveredLinePoint.pointLabel}
              </text>
              <text
                x={Math.max(padding, hoveredLinePoint.x - 110)}
                y={Math.max(58, hoveredLinePoint.y - 22)}
                className="trend-tooltip-text"
              >
                Value: {formatValue(hoveredLinePoint.value)} {isCoolingMinutes ? "" : chart.y_label}
              </text>
            </g>
          ) : null}

        </svg>
      </div>

      <div className="trend-axis-row">
        <span>{isBarMode ? "Asset Index (see table below)" : chart.x_label}</span>
        <span>{chart.y_label}</span>
      </div>

      {barValueRows.length ? (
        <div className="trend-value-grid">
          {barValueRows.map((row) => (
            <div className="trend-value-item" key={row.name}>
              <span className="trend-value-name">{row.name}</span>
              <strong className="trend-value-number">{row.value.toFixed(2)}</strong>
            </div>
          ))}
        </div>
      ) : null}

      {allValueRows.length ? (
        <div className="trend-table-wrap">
          <table className="trend-table">
            <thead>
              <tr>
                <th>{isBarMode ? "Category" : "Asset"}</th>
                <th>{isBarMode ? "Asset Name" : "Recorded At"}</th>
                <th>Value</th>
              </tr>
            </thead>
            <tbody>
              {allValueRows.map((row, index) => (
                <tr key={`${row.category}-${row.name}-${index}`}>
                  <td>
                    <span className="trend-table-category">
                      <span className="trend-table-dot" style={{ backgroundColor: row.color }} />
                      {row.category}
                    </span>
                  </td>
                  <td>{row.name}</td>
                  <td>{formatValue(row.value)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      ) : null}
    </div>
  );
}

export function ChatShell() {
  const [message, setMessage] = useState("");
  const [currentConversation, setCurrentConversation] = useState<Conversation>({
    id: "welcome",
    title: "Current Chat",
    items: [
      {
        role: "assistant",
        content:
          "VisBot Analysis is ready. Ask about any asset, category, or monitoring system by name.",
      },
    ],
  });
  const [history, setHistory] = useState<Conversation[]>([]);
  const [prompts, setPrompts] = useState<string[]>([]);
  const [historyIndex, setHistoryIndex] = useState(-1);
  const [showHistory, setShowHistory] = useState(false);
  const [showDetails, setShowDetails] = useState(false);
  const [error, setError] = useState<string>("");
  const [analysisStartedAt, setAnalysisStartedAt] = useState<number | null>(null);
  const [elapsedSeconds, setElapsedSeconds] = useState(0);
  const [isPending, startTransition] = useTransition();

  const selectedAnalysis = useMemo(
    () => currentConversation.analysis ?? null,
    [currentConversation]
  );

  useEffect(() => {
    if (!isPending || analysisStartedAt === null) {
      return;
    }

    const intervalId = window.setInterval(() => {
      setElapsedSeconds(Math.max(0, Math.floor((Date.now() - analysisStartedAt) / 1000)));
    }, 1000);

    return () => window.clearInterval(intervalId);
  }, [analysisStartedAt, isPending]);

  function archiveCurrentConversation() {
    if (
      currentConversation.id !== "welcome" &&
      currentConversation.items.some((item) => item.role === "assistant")
    ) {
      setHistory((current) => [currentConversation, ...current]);
    }
  }

  function submitMessage(event?: FormEvent<HTMLFormElement>) {
    event?.preventDefault();
    const trimmedMessage = message.trim();
    submitPrompt(trimmedMessage);
  }

  function submitPrompt(rawMessage: string) {
    const trimmedMessage = rawMessage.trim();
    if (!trimmedMessage) {
      return;
    }

    setError("");
    setShowDetails(false);
    setElapsedSeconds(0);
    setAnalysisStartedAt(Date.now());
    archiveCurrentConversation();
    setHistoryIndex(-1);
    setPrompts((current) => {
      const filtered = current.filter(p => p !== trimmedMessage);
      return [trimmedMessage, ...filtered];
    });

    const nextConversation: Conversation = {
      id: createConversationId(),
      title: trimmedMessage,
      items: [{ role: "user", content: trimmedMessage }],
    };

    setCurrentConversation(nextConversation);
    setMessage("");

    startTransition(async () => {
      try {
        const result = await sendChatMessage({ message: trimmedMessage });
        setCurrentConversation({
          ...nextConversation,
          items: [
            ...nextConversation.items,
            {
              role: "assistant",
              content: result.answer,
              analysis: result.analysis,
            },
          ],
          analysis: result.analysis,
        });
      } catch (err) {
        const finalMessage =
          err instanceof Error ? err.message : "Something went wrong";
        const summary = summarizeFailure(finalMessage);
        setCurrentConversation({
          ...nextConversation,
          items: [
            ...nextConversation.items,
            {
              role: "assistant",
              content: summary,
            },
          ],
        });
        setError(finalMessage);
      } finally {
        setAnalysisStartedAt(null);
      }
    });
  }

  function onComposerKeyDown(event: KeyboardEvent<HTMLTextAreaElement>) {
    if (event.key === "Enter" && !event.shiftKey) {
      event.preventDefault();
      submitMessage();
    }
  }

  function openHistoryConversation(conversation: Conversation) {
    setCurrentConversation(conversation);
    setShowHistory(false);
    setShowDetails(false);
    setError("");
  }

  return (
    <main className="shell">
      <section className="hero-card">
        <div className="hero-layout">
          <div className="hero-copy-wrap">
            <p className="eyebrow">VisBot Analysis</p>
            <h1 className="headline">Intelligent asset analysis from one chat.</h1>
            <p className="subtle hero-copy">
              Ask about a device, category, or monitoring system. VisBot Analysis
              resolves the hierarchy, fetches the data, and analyzes only the rows
              returned from the database.
            </p>
            <div className="prompt-row">
              {starterPrompts.map((prompt) => (
                <button
                  type="button"
                  key={prompt}
                  className="prompt-chip"
                  onClick={() => submitPrompt(prompt)}
                >
                  {prompt}
                </button>
              ))}
            </div>
          </div>
          <SplineSceneBasic />
        </div>
      </section>

      <section className="workspace split-workspace">
        <div className="chat-pane">
          <div className="workspace-header">
            <div>
              <p className="eyebrow">Chat</p>
              <h2 className="workspace-title">Hello! VisBot Here to Help.</h2>
            </div>
            <button
              type="button"
              className="secondary-button"
              onClick={() => setShowHistory((current) => !current)}
            >
              {showHistory ? "Hide Chat History" : "Show Chat History"}
            </button>
          </div>

          {showHistory ? (
            <div className="history-panel">
              {history.length ? (
                history.map((conversation) => (
                  <button
                    type="button"
                    key={conversation.id}
                    className="history-item"
                    onClick={() => openHistoryConversation(conversation)}
                  >
                    <strong>{conversation.analysis?.asset.name || "Previous Run"}</strong>
                    <span>{conversation.title}</span>
                  </button>
                ))
              ) : (
                <div className="empty-details">No previous chat history yet.</div>
              )}
            </div>
          ) : null}

          <div className="chat-log">
            {currentConversation.items.map((item, index) => (
              <article className={`message ${item.role}`} key={`${item.role}-${index}`}>
                <div className="message-head">
                  <strong>{item.role === "user" ? "You" : "VisBot Analysis"}</strong>
                </div>
                <div className="message-body">
                  {item.analysis
                    ? (
                      (item.analysis.trend_charts?.length
                        ? item.analysis.trend_charts
                        : item.analysis.trend_chart
                          ? [item.analysis.trend_chart]
                          : []
                      ).map((chart, chartIndex) => (
                        <TrendChartView chart={chart} key={`${chart.title}-${chartIndex}`} />
                      ))
                    )
                    : null}
                  {item.role === "assistant" ? renderMarkdownish(item.content) : item.content}
                </div>

                {item.analysis ? (
                  <div className="run-card">
                    <div className="run-meta">
                      <span>{item.analysis.asset.name}</span>
                      <span>{item.analysis.asset.parent_name || "No parent"}</span>
                      <span>{item.analysis.plan.analysis_name}</span>
                    </div>

                    {item.analysis.plan.warnings.length ? (
                      <div className="warning-box">
                        {item.analysis.plan.warnings.map((warning) => (
                          <div key={warning}>{warning}</div>
                        ))}
                      </div>
                    ) : null}
                  </div>
                ) : null}
              </article>
            ))}
          </div>

          <form className="composer" onSubmit={submitMessage}>
            <textarea
              rows={4}
              value={message}
              onChange={(event) => setMessage(event.target.value)}
              onKeyDown={onComposerKeyDown}
              placeholder="Ask about any device or category. Example: Provide analysis on T & H Monitoring"
            />
            <div className="actions">
              <span className="subtle">
                Press Enter to run analysis. Use Shift+Enter for a new line.
              </span>
              <div style={{ display: "flex", gap: "4px", alignItems: "center" }}>
                <div style={{ display: "flex", flexDirection: "column", gap: "2px", marginRight: "4px" }}>
                  <button
                    type="button"
                    className="secondary-button"
                    style={{ padding: "2px 8px", display: "flex", alignItems: "center", justifyContent: "center" }}
                    disabled={prompts.length === 0 || historyIndex >= prompts.length - 1}
                    onClick={() => {
                      const nextIndex = historyIndex + 1;
                      setHistoryIndex(nextIndex);
                      setMessage(prompts[nextIndex]);
                    }}
                    title="Previous Prompt"
                  >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="18 15 12 9 6 15"></polyline></svg>
                  </button>
                  <button
                    type="button"
                    className="secondary-button"
                    style={{ padding: "2px 8px", display: "flex", alignItems: "center", justifyContent: "center" }}
                    disabled={historyIndex < 0}
                    onClick={() => {
                      const prevIndex = historyIndex - 1;
                      setHistoryIndex(prevIndex);
                      setMessage(prevIndex >= 0 ? prompts[prevIndex] : "");
                    }}
                    title="Next Prompt"
                  >
                    <svg width="14" height="14" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round"><polyline points="6 9 12 15 18 9"></polyline></svg>
                  </button>
                </div>
                <button type="submit" disabled={isPending} className="run-analysis-button">
                  {isPending ? (
                    <SparklesText
                      text={`Analyzing... ${formatElapsed(elapsedSeconds)}`}
                      className="analyzing-sparkles"
                      sparklesCount={10}
                      colors={{ first: "#a7c9ff", second: "#8de3ff" }}
                    />
                  ) : (
                    "Run Analysis"
                  )}
                </button>
              </div>
            </div>
          </form>

          {error ? <div className="error-box">{error}</div> : null}
        </div>

        <aside className="details-pane">
          <div className="workspace-header">
            <div>
              <p className="eyebrow">Details</p>
              <h2 className="workspace-title"></h2>
            </div>
          </div>

          <button
            type="button"
            className="secondary-button diagnostics-toggle"
            onClick={() => setShowDetails((current) => !current)}
            disabled={!selectedAnalysis}
          >
            {showDetails ? "Hide Details" : "Show Details"}
          </button>

          {!selectedAnalysis ? (
            <div className="empty-details">
              Run an analysis first to inspect SQL, token usage, LLM output, and database rows.
            </div>
          ) : !showDetails ? (
            <div className="empty-details">
              Press <strong>Show Details</strong> to open diagnostics for the current run.
            </div>
          ) : (
            <div className="details-grid">
              <div className="detail-card">
                <strong>Total Tokens Consumed</strong>
                <pre>{String(selectedAnalysis.plan.total_tokens)}</pre>
              </div>
              <div className="detail-card">
                <strong>Query Time Window</strong>
                <pre>{selectedAnalysis.plan.query_window_label || selectedAnalysis.plan.time_window.label}</pre>
              </div>
              <div className="detail-card">
                <strong>Planner Prompt Sent To LLM</strong>
                <pre>{selectedAnalysis.plan.planner_prompt || "No planner prompt recorded."}</pre>
              </div>
              <div className="detail-card">
                <strong>SQL Prompt Sent To LLM</strong>
                <pre>{selectedAnalysis.plan.sql_prompt || "No SQL prompt recorded for this run."}</pre>
              </div>
              <div className="detail-card">
                <strong>Analysis Prompt Sent To LLM</strong>
                <pre>{selectedAnalysis.plan.analyst_prompt || "No analyst prompt recorded."}</pre>
              </div>
              <div className="detail-card">
                <strong>Generated SQL Query</strong>
                <pre>{selectedAnalysis.plan.sql_query}</pre>
              </div>
              <div className="detail-card">
                <strong>LLM Response</strong>
                <pre>{selectedAnalysis.plan.llm_raw_response || selectedAnalysis.plan.reasoning}</pre>
              </div>
              <div className="detail-card">
                <strong>Database Response</strong>
                <pre>{JSON.stringify(selectedAnalysis.rows, null, 2)}</pre>
              </div>
            </div>
          )}
        </aside>
      </section>
    </main>
  );
}
