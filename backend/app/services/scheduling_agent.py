import json
import re
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import Optional

from app.models.schemas import ChatResponse, AnalysisResponse, AnalysisPlan, TimeWindow, AssetProfile, AnalysisReport
from app.services.data_repository import data_repository
from app.services.llm_service import llm_service
from app.services.trend_chart_service import trend_chart_service

TH_SQL = r"""
WITH ShiftData AS (
    SELECT (DATE_TRUNC('day', NOW() + INTERVAL '3 hours' - INTERVAL '6 hours') + INTERVAL '6 hours') AS shift_start_local
),
Heartbeat AS (
    SELECT 
        a.name AS asset_name,
        MAX(ad.timestamp) AS absolute_latest_time
    FROM openremote.asset_datapoint ad
    JOIN openremote.asset a ON a.id = ad.entity_id
    WHERE (a.name ~* 'ColdRoom' OR a.name ~* 'LTRoom' OR a.name ~* 'QadLab' OR a.name ~* 'R.*D.*Lab')
      AND a.name NOT ILIKE '%Lab 35%' 
      AND a.name NOT ILIKE '%Lab 45%'
    GROUP BY a.name
),
RawData AS (
    SELECT 
        a.name AS asset_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '3 hours') AS local_time,
        regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\.-]', '', 'g')::numeric AS temp,
        regexp_replace(COALESCE(ad.value->>'Humidity', ad.value->>'humidity'), '[^0-9\.-]', '', 'g')::numeric AS humidity
    FROM openremote.asset_datapoint ad
    JOIN openremote.asset a ON a.id = ad.entity_id
    WHERE (a.name ~* 'ColdRoom' OR a.name ~* 'LTRoom' OR a.name ~* 'QadLab' OR a.name ~* 'R.*D.*Lab')
      AND a.name NOT ILIKE '%Lab 35%' 
      AND a.name NOT ILIKE '%Lab 45%'
      AND COALESCE(ad.value->>'Temperature', ad.value->>'temperature') IS NOT NULL
      AND NULLIF(regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\.-]', '', 'g'), '') IS NOT NULL
      AND regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\.-]', '', 'g')::numeric > -50 
      AND regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\.-]', '', 'g')::numeric < 100
),
Latest AS (
    SELECT *, ROW_NUMBER() OVER(PARTITION BY asset_name ORDER BY raw_time DESC) as rn FROM RawData
),
MinShift AS (
    SELECT * FROM (
        SELECT r.asset_name, r.temp as lowest_temp, TO_CHAR(r.local_time, 'DD-MM-YYYY HH12:MI AM') as lowest_time,
               ROW_NUMBER() OVER(PARTITION BY r.asset_name ORDER BY r.temp ASC) as rn
        FROM RawData r CROSS JOIN ShiftData s 
        WHERE r.local_time >= s.shift_start_local
    ) min_data WHERE rn = 1
),
MaxShift AS (
    SELECT * FROM (
        SELECT r.asset_name, r.temp as highest_temp, TO_CHAR(r.local_time, 'DD-MM-YYYY HH12:MI AM') as highest_time,
               ROW_NUMBER() OVER(PARTITION BY r.asset_name ORDER BY r.temp DESC) as rn
        FROM RawData r CROSS JOIN ShiftData s 
        WHERE r.local_time >= s.shift_start_local
    ) max_data WHERE rn = 1
),
HistoryData AS (
    SELECT 
        r.asset_name,
        json_agg(
            json_build_object(
                'time', TO_CHAR(r.local_time, 'YYYY-MM-DD"T"HH24:MI:SS'), 
                'temp', r.temp,
                'humidity', r.humidity
            ) ORDER BY r.local_time ASC
        ) AS history_array
    FROM RawData r CROSS JOIN ShiftData s 
    WHERE r.local_time >= s.shift_start_local - INTERVAL '6 hours'
    GROUP BY r.asset_name
)
SELECT 
    hb.asset_name, 
    l.temp AS latest_temp, 
    l.humidity AS latest_humidity, 
    TO_CHAR(l.local_time, 'DD-MM-YYYY HH12:MI AM') AS latest_time,
    COALESCE(min_s.lowest_temp, l.temp) AS lowest_temp, 
    COALESCE(min_s.lowest_time, TO_CHAR(l.local_time, 'DD-MM-YYYY HH12:MI AM')) AS lowest_time,
    COALESCE(max_s.highest_temp, l.temp) AS highest_temp, 
    COALESCE(max_s.highest_time, TO_CHAR(l.local_time, 'DD-MM-YYYY HH12:MI AM')) AS highest_time,
    COALESCE(hb.absolute_latest_time < NOW() - INTERVAL '2 hours', true) AS is_heartbeat_stale,
    COALESCE(l.raw_time < NOW() - INTERVAL '2 hours' OR l.raw_time IS NULL, true) AS is_payload_stale,
    (l.local_time < (SELECT shift_start_local FROM ShiftData LIMIT 1)) AS is_previous_shift,
    hd.history_array
FROM Heartbeat hb
LEFT JOIN Latest l ON hb.asset_name = l.asset_name AND l.rn = 1
LEFT JOIN MinShift min_s ON hb.asset_name = min_s.asset_name
LEFT JOIN MaxShift max_s ON hb.asset_name = max_s.asset_name
LEFT JOIN HistoryData hd ON hb.asset_name = hd.asset_name
ORDER BY regexp_replace(hb.asset_name, '[0-9]', '', 'g'), COALESCE(NULLIF(regexp_replace(hb.asset_name, '\D', '', 'g'), ''), '0')::int;
"""

FILLING_MACHINES_SQL = r"""
WITH CurrentLocalTime AS (
    SELECT (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + INTERVAL '3 hours' AS now_local
),
LatestFilling AS (
    SELECT 
        a.name AS asset_name,
        (ad.value->>'prod_shift_1_today')::numeric AS shift_1,
        (ad.value->>'prod_shift_2_today')::numeric AS shift_2,
        (ad.value->>'prod_shift_3_today')::numeric AS shift_3,
        (ad.value->>'prod_total_today')::numeric AS total_count,
        (NULLIF(ad.value->>'Timestamp', ''))::timestamp AS data_time,
        (ad.timestamp + INTERVAL '3 hours') AS db_time,
        ROW_NUMBER() OVER(PARTITION BY a.name ORDER BY ad.timestamp DESC) as rn
    FROM openremote.asset_datapoint ad
    JOIN openremote.asset a ON a.id = ad.entity_id
    CROSS JOIN CurrentLocalTime clt
    WHERE (a.name ILIKE 'Machine%' OR a.name ILIKE 'Oil Filling%')
      AND (ad.value->>'prod_total_today') IS NOT NULL
)
SELECT 
    lf.asset_name, 
    lf.shift_1, 
    lf.shift_2, 
    lf.shift_3, 
    lf.total_count, 
    TO_CHAR(COALESCE(lf.data_time, lf.db_time) - INTERVAL '6 hours', 'YYYY-MM-DD') AS recorded_time,
    (
        (lf.data_time IS NOT NULL AND lf.data_time < (lf.db_time - INTERVAL '2 hours'))
        OR 
        lf.db_time < (clt.now_local - INTERVAL '12 hours')
    ) AS is_stale,
    (COALESCE(lf.data_time, lf.db_time) < (DATE_TRUNC('day', clt.now_local - INTERVAL '6 hours') + INTERVAL '6 hours')) AS is_previous_shift
FROM LatestFilling lf
CROSS JOIN CurrentLocalTime clt
WHERE lf.rn = 1
ORDER BY COALESCE(NULLIF(regexp_replace(lf.asset_name, '\D', '', 'g'), ''), '0')::int, lf.asset_name;
"""

class N8NFastPathService:
    def try_intercept(self, message: str) -> Optional[ChatResponse]:
        text = message.lower()
        no_space = text.replace(" ", "")
        offset, span_days = self._parse_schedule_window(text)

        if "scheduledt&hmonitoring" in no_space or ("t&hmonitoring" in no_space and "schedule" in no_space):
            return self._handle_th(offset, span_days)
        if "schedulefillingmachines" in no_space or "scheduledfillingmachines" in no_space:
            return self._handle_filling(offset, span_days)
        return None

    def _parse_schedule_window(self, text: str) -> tuple[int, int]:
        offset = 0
        span_days = 1

        if "yesterday" in text:
            offset = 1
        else:
            match_offset = re.search(r'(\d+)\s*days?\s*ago', text)
            if match_offset:
                offset = int(match_offset.group(1))

        span_match = re.search(r'(last|past)\s+(\d+)\s*(day|days|week|weeks|month|months)', text)
        if span_match:
            value = max(1, int(span_match.group(2)))
            unit = span_match.group(3)
            if unit.startswith("week"):
                span_days = value * 7
            elif unit.startswith("month"):
                span_days = value * 30
            else:
                span_days = value

        span_days = max(1, min(span_days, 180))
        offset = max(0, min(offset, 365))
        return offset, span_days

    def _get_shift_date_str(self, offset=0):
        kNow = datetime.now(ZoneInfo('Asia/Karachi')) - timedelta(days=offset)
        shiftStart = kNow
        if shiftStart.hour < 6:
            shiftStart = shiftStart - timedelta(days=1)
        return shiftStart.strftime('%d/%m/%Y')

    def _get_window_label(self, offset: int, span_days: int) -> str:
        now_local = datetime.now(ZoneInfo('Asia/Karachi')) - timedelta(days=offset)
        if span_days <= 1:
            return self._get_shift_date_str(offset)
        start_local = now_local - timedelta(days=span_days)
        return f"{start_local.strftime('%d/%m/%Y')} to {now_local.strftime('%d/%m/%Y')}"

    def _build_th_sql(self, offset: int, span_days: int) -> str:
        if span_days > 1:
            points_per_day = 16
            return f"""
WITH WindowBounds AS (
    SELECT
        (NOW() + INTERVAL '3 hours' - INTERVAL '{offset} days') AS window_end_local,
        (NOW() + INTERVAL '3 hours' - INTERVAL '{offset} days' - INTERVAL '{span_days} days') AS window_start_local
),
Heartbeat AS (
    SELECT
        a.name AS asset_name,
        MAX(ad.timestamp) AS absolute_latest_time
    FROM openremote.asset_datapoint ad
    JOIN openremote.asset a ON a.id = ad.entity_id
    WHERE (a.name ~* 'ColdRoom' OR a.name ~* 'LTRoom' OR a.name ~* 'QadLab' OR a.name ~* 'R.*D.*Lab')
      AND a.name NOT ILIKE '%Lab 35%'
      AND a.name NOT ILIKE '%Lab 45%'
    GROUP BY a.name
),
RawSource AS (
    SELECT
        a.name AS asset_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '3 hours') AS local_time,
        regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\\.-]', '', 'g')::numeric AS temp,
        regexp_replace(COALESCE(ad.value->>'Humidity', ad.value->>'humidity'), '[^0-9\\.-]', '', 'g')::numeric AS humidity
    FROM openremote.asset_datapoint ad
    JOIN openremote.asset a ON a.id = ad.entity_id
    CROSS JOIN WindowBounds wb
    WHERE (a.name ~* 'ColdRoom' OR a.name ~* 'LTRoom' OR a.name ~* 'QadLab' OR a.name ~* 'R.*D.*Lab')
      AND a.name NOT ILIKE '%Lab 35%'
      AND a.name NOT ILIKE '%Lab 45%'
      AND COALESCE(ad.value->>'Temperature', ad.value->>'temperature') IS NOT NULL
      AND NULLIF(regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\\.-]', '', 'g'), '') IS NOT NULL
      AND regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\\.-]', '', 'g')::numeric > -50
      AND regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\\.-]', '', 'g')::numeric < 100
      AND (ad.timestamp + INTERVAL '3 hours') >= wb.window_start_local
      AND (ad.timestamp + INTERVAL '3 hours') <= wb.window_end_local
),
Bucketed AS (
    SELECT
        rs.*,
        date_trunc('day', rs.local_time) AS day_bucket,
        ntile({points_per_day}) OVER (
            PARTITION BY rs.asset_name, date_trunc('day', rs.local_time)
            ORDER BY rs.local_time
        ) AS sample_bucket
    FROM RawSource rs
),
RawData AS (
    SELECT
        ranked.asset_name,
        ranked.raw_time,
        ranked.local_time,
        ranked.temp,
        ranked.humidity
    FROM (
        SELECT
            b.*,
            row_number() OVER (
                PARTITION BY b.asset_name, b.day_bucket, b.sample_bucket
                ORDER BY b.local_time DESC
            ) AS sample_rn
        FROM Bucketed b
    ) ranked
    WHERE sample_rn = 1
),
Latest AS (
    SELECT
        rd.*,
        ROW_NUMBER() OVER(PARTITION BY rd.asset_name ORDER BY rd.raw_time DESC) AS latest_rn
    FROM RawData rd
),
MinWindow AS (
    SELECT * FROM (
        SELECT
            r.asset_name,
            r.temp AS lowest_temp,
            TO_CHAR(r.local_time, 'DD-MM-YYYY HH12:MI AM') AS lowest_time,
            ROW_NUMBER() OVER(PARTITION BY r.asset_name ORDER BY r.temp ASC) AS rn
        FROM RawData r
    ) ranked WHERE rn = 1
),
MaxWindow AS (
    SELECT * FROM (
        SELECT
            r.asset_name,
            r.temp AS highest_temp,
            TO_CHAR(r.local_time, 'DD-MM-YYYY HH12:MI AM') AS highest_time,
            ROW_NUMBER() OVER(PARTITION BY r.asset_name ORDER BY r.temp DESC) AS rn
        FROM RawData r
    ) ranked WHERE rn = 1
),
HistoryData AS (
    SELECT
        r.asset_name,
        json_agg(
            json_build_object(
                'time', TO_CHAR(r.local_time, 'YYYY-MM-DD"T"HH24:MI:SS'),
                'temp', r.temp,
                'humidity', r.humidity
            ) ORDER BY r.local_time ASC
        ) AS history_array
    FROM RawData r
    GROUP BY r.asset_name
)
SELECT
    hb.asset_name,
    l.temp AS latest_temp,
    l.humidity AS latest_humidity,
    TO_CHAR(l.local_time, 'DD-MM-YYYY HH12:MI AM') AS latest_time,
    COALESCE(mn.lowest_temp, l.temp) AS lowest_temp,
    COALESCE(mn.lowest_time, TO_CHAR(l.local_time, 'DD-MM-YYYY HH12:MI AM')) AS lowest_time,
    COALESCE(mx.highest_temp, l.temp) AS highest_temp,
    COALESCE(mx.highest_time, TO_CHAR(l.local_time, 'DD-MM-YYYY HH12:MI AM')) AS highest_time,
    COALESCE(hb.absolute_latest_time < NOW() - INTERVAL '2 hours', true) AS is_heartbeat_stale,
    COALESCE(l.raw_time < NOW() - INTERVAL '2 hours' OR l.raw_time IS NULL, true) AS is_payload_stale,
    false AS is_previous_shift,
    hd.history_array
FROM Heartbeat hb
LEFT JOIN Latest l ON hb.asset_name = l.asset_name AND l.latest_rn = 1
LEFT JOIN MinWindow mn ON hb.asset_name = mn.asset_name
LEFT JOIN MaxWindow mx ON hb.asset_name = mx.asset_name
LEFT JOIN HistoryData hd ON hb.asset_name = hd.asset_name
ORDER BY regexp_replace(hb.asset_name, '[0-9]', '', 'g'), COALESCE(NULLIF(regexp_replace(hb.asset_name, '\\D', '', 'g'), ''), '0')::int;
""".strip()
        return f"""
WITH WindowBounds AS (
    SELECT
        (NOW() + INTERVAL '3 hours' - INTERVAL '{offset} days') AS window_end_local,
        (NOW() + INTERVAL '3 hours' - INTERVAL '{offset} days' - INTERVAL '{span_days} days') AS window_start_local,
        (DATE_TRUNC('day', NOW() + INTERVAL '3 hours' - INTERVAL '{offset} days' - INTERVAL '6 hours') + INTERVAL '6 hours') AS shift_start_local
),
Heartbeat AS (
    SELECT
        a.name AS asset_name,
        MAX(ad.timestamp) AS absolute_latest_time
    FROM openremote.asset_datapoint ad
    JOIN openremote.asset a ON a.id = ad.entity_id
    WHERE (a.name ~* 'ColdRoom' OR a.name ~* 'LTRoom' OR a.name ~* 'QadLab' OR a.name ~* 'R.*D.*Lab')
      AND a.name NOT ILIKE '%Lab 35%'
      AND a.name NOT ILIKE '%Lab 45%'
    GROUP BY a.name
),
RawData AS (
    SELECT
        a.name AS asset_name,
        ad.timestamp AS raw_time,
        (ad.timestamp + INTERVAL '3 hours') AS local_time,
        regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\\.-]', '', 'g')::numeric AS temp,
        regexp_replace(COALESCE(ad.value->>'Humidity', ad.value->>'humidity'), '[^0-9\\.-]', '', 'g')::numeric AS humidity
    FROM openremote.asset_datapoint ad
    JOIN openremote.asset a ON a.id = ad.entity_id
    CROSS JOIN WindowBounds wb
    WHERE (a.name ~* 'ColdRoom' OR a.name ~* 'LTRoom' OR a.name ~* 'QadLab' OR a.name ~* 'R.*D.*Lab')
      AND a.name NOT ILIKE '%Lab 35%'
      AND a.name NOT ILIKE '%Lab 45%'
      AND COALESCE(ad.value->>'Temperature', ad.value->>'temperature') IS NOT NULL
      AND NULLIF(regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\\.-]', '', 'g'), '') IS NOT NULL
      AND regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\\.-]', '', 'g')::numeric > -50
      AND regexp_replace(COALESCE(ad.value->>'Temperature', ad.value->>'temperature'), '[^0-9\\.-]', '', 'g')::numeric < 100
      AND (ad.timestamp + INTERVAL '3 hours') >= wb.window_start_local
      AND (ad.timestamp + INTERVAL '3 hours') <= wb.window_end_local
),
Latest AS (
    SELECT
        rd.*,
        ROW_NUMBER() OVER(PARTITION BY rd.asset_name ORDER BY rd.raw_time DESC) AS latest_rn
    FROM RawData rd
),
MinWindow AS (
    SELECT * FROM (
        SELECT
            r.asset_name,
            r.temp AS lowest_temp,
            TO_CHAR(r.local_time, 'DD-MM-YYYY HH12:MI AM') AS lowest_time,
            ROW_NUMBER() OVER(PARTITION BY r.asset_name ORDER BY r.temp ASC) AS rn
        FROM RawData r
    ) ranked WHERE rn = 1
),
MaxWindow AS (
    SELECT * FROM (
        SELECT
            r.asset_name,
            r.temp AS highest_temp,
            TO_CHAR(r.local_time, 'DD-MM-YYYY HH12:MI AM') AS highest_time,
            ROW_NUMBER() OVER(PARTITION BY r.asset_name ORDER BY r.temp DESC) AS rn
        FROM RawData r
    ) ranked WHERE rn = 1
),
HistoryData AS (
    SELECT
        r.asset_name,
        json_agg(
            json_build_object(
                'time', TO_CHAR(r.local_time, 'YYYY-MM-DD"T"HH24:MI:SS'),
                'temp', r.temp,
                'humidity', r.humidity
            ) ORDER BY r.local_time ASC
        ) AS history_array
    FROM RawData r
    GROUP BY r.asset_name
)
SELECT
    hb.asset_name,
    l.temp AS latest_temp,
    l.humidity AS latest_humidity,
    TO_CHAR(l.local_time, 'DD-MM-YYYY HH12:MI AM') AS latest_time,
    COALESCE(mn.lowest_temp, l.temp) AS lowest_temp,
    COALESCE(mn.lowest_time, TO_CHAR(l.local_time, 'DD-MM-YYYY HH12:MI AM')) AS lowest_time,
    COALESCE(mx.highest_temp, l.temp) AS highest_temp,
    COALESCE(mx.highest_time, TO_CHAR(l.local_time, 'DD-MM-YYYY HH12:MI AM')) AS highest_time,
    COALESCE(hb.absolute_latest_time < NOW() - INTERVAL '2 hours', true) AS is_heartbeat_stale,
    COALESCE(l.raw_time < NOW() - INTERVAL '2 hours' OR l.raw_time IS NULL, true) AS is_payload_stale,
    CASE
        WHEN {span_days} = 1 THEN (l.local_time < (SELECT shift_start_local FROM WindowBounds LIMIT 1))
        ELSE false
    END AS is_previous_shift,
    hd.history_array
FROM Heartbeat hb
LEFT JOIN Latest l ON hb.asset_name = l.asset_name AND l.latest_rn = 1
LEFT JOIN MinWindow mn ON hb.asset_name = mn.asset_name
LEFT JOIN MaxWindow mx ON hb.asset_name = mx.asset_name
LEFT JOIN HistoryData hd ON hb.asset_name = hd.asset_name
ORDER BY regexp_replace(hb.asset_name, '[0-9]', '', 'g'), COALESCE(NULLIF(regexp_replace(hb.asset_name, '\\D', '', 'g'), ''), '0')::int;
""".strip()

    def _build_filling_sql(self, offset: int, span_days: int) -> str:
        return f"""
WITH WindowBounds AS (
    SELECT
        ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + INTERVAL '3 hours' - INTERVAL '{offset} days') AS window_end_local,
        ((CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + INTERVAL '3 hours' - INTERVAL '{offset} days' - INTERVAL '{span_days} days') AS window_start_local,
        (DATE_TRUNC('day', (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + INTERVAL '3 hours' - INTERVAL '{offset} days' - INTERVAL '6 hours') + INTERVAL '6 hours') AS shift_start_local
),
RawFilling AS (
    SELECT
        a.name AS asset_name,
        (ad.value->>'prod_shift_1_today')::numeric AS shift_1,
        (ad.value->>'prod_shift_2_today')::numeric AS shift_2,
        (ad.value->>'prod_shift_3_today')::numeric AS shift_3,
        (ad.value->>'prod_total_today')::numeric AS total_count,
        (NULLIF(ad.value->>'Timestamp', ''))::timestamp AS data_time,
        (ad.timestamp + INTERVAL '3 hours') AS db_time
    FROM openremote.asset_datapoint ad
    JOIN openremote.asset a ON a.id = ad.entity_id
    CROSS JOIN WindowBounds wb
    WHERE (a.name ILIKE 'Machine%' OR a.name ILIKE 'Oil Filling%')
      AND (ad.value->>'prod_total_today') IS NOT NULL
      AND (ad.timestamp + INTERVAL '3 hours') >= wb.window_start_local
      AND (ad.timestamp + INTERVAL '3 hours') <= wb.window_end_local
),
LatestFilling AS (
    SELECT *,
           ROW_NUMBER() OVER(PARTITION BY asset_name ORDER BY db_time DESC) AS rn
    FROM RawFilling
)
SELECT
    lf.asset_name,
    lf.shift_1,
    lf.shift_2,
    lf.shift_3,
    lf.total_count,
    TO_CHAR(COALESCE(lf.data_time, lf.db_time) - INTERVAL '6 hours', 'YYYY-MM-DD') AS recorded_time,
    (
        (lf.data_time IS NOT NULL AND lf.data_time < (lf.db_time - INTERVAL '2 hours'))
        OR
        lf.db_time < (((CURRENT_TIMESTAMP AT TIME ZONE 'UTC') + INTERVAL '3 hours') - INTERVAL '12 hours')
    ) AS is_stale,
    CASE
        WHEN {span_days} = 1 THEN (COALESCE(lf.data_time, lf.db_time) < (SELECT shift_start_local FROM WindowBounds LIMIT 1))
        ELSE false
    END AS is_previous_shift
FROM LatestFilling lf
WHERE lf.rn = 1
ORDER BY COALESCE(NULLIF(regexp_replace(lf.asset_name, '\\D', '', 'g'), ''), '0')::int, lf.asset_name;
""".strip()

    def _format_compact(self, iso_string):
        if not iso_string: return ""
        try:
            d = datetime.fromisoformat(iso_string)
        except:
            d = datetime.strptime(iso_string, '%Y-%m-%dT%H:%M:%S')
        h = d.hour
        m = d.minute
        ampm = 'pm' if h >= 12 else 'am'
        h = h % 12
        h = h if h else 12
        mStr = "" if m == 0 else f":{m:02d}"
        datePart = d.strftime('%d %b')
        return f"{h}{mStr}{ampm} ({datePart})"

    def _sample_history(self, history: list[dict], span_days: int) -> list[dict]:
        if span_days <= 1 or len(history) <= 200:
            return history
        points_per_day = 16
        buckets: dict[str, list[dict]] = {}
        for point in history:
            t = str(point.get("time") or "")
            day = t[:10] if len(t) >= 10 else "unknown"
            buckets.setdefault(day, []).append(point)

        sampled: list[dict] = []
        for day in sorted(buckets.keys()):
            day_points = buckets[day]
            if len(day_points) <= points_per_day:
                sampled.extend(day_points)
                continue
            step = max(1, len(day_points) // points_per_day)
            picked = [day_points[i] for i in range(0, len(day_points), step)]
            if day_points[-1] not in picked:
                picked.append(day_points[-1])
            sampled.extend(picked[:points_per_day])
        return sampled

    def _analyze_cooling_cycles(self, history_data, asset_name, is_broken, offset=0, span_days=1):
        if is_broken: return {"text": "N/A", "totalTime": "0 hr 0 min"}
        name = asset_name.lower()
        if 'ltroom' in name or 'qadlab' in name or 'r & d' in name:
            return {"text": "N/A", "totalTime": "N/A"}
        if not history_data or len(history_data) < 2:
            return {"text": "N/A", "totalTime": "0 hr 0 min"}
            
        history = history_data if isinstance(history_data, list) else json.loads(history_data)
        history = self._sample_history(history, span_days)
        
        cycles = []
        is_cooling = False
        potential_start = None
        
        for i in range(1, len(history)):
            prev = history[i - 1]
            curr = history[i]
            if curr['temp'] < prev['temp']:
                if not is_cooling and potential_start is None:
                    potential_start = prev
            elif curr['temp'] > prev['temp'] and not is_cooling and curr['temp'] > 20.0:
                potential_start = None
                
            if not is_cooling and curr['temp'] < 20.0 and potential_start:
                is_cooling = True
                
            if is_cooling:
                if curr['temp'] >= 20.0:
                    lowest_point = prev
                    for k in range(i - 1, 0, -1):
                        if history[k]['time'] == potential_start['time'] or history[k]['temp'] > 20:
                            break
                        if history[k]['temp'] < lowest_point['temp']:
                            lowest_point = history[k]
                    cycles.append({"start": potential_start, "end": lowest_point})
                    is_cooling = False
                    potential_start = None
                    
        if is_cooling and potential_start:
            cycles.append({"start": potential_start, "end": {"time": "running", "temp": history[-1]['temp']}})
            
        kNow = datetime.now(ZoneInfo('Asia/Karachi')) - timedelta(days=offset)
        if span_days <= 1:
            window_start = kNow
            if window_start.hour < 6:
                window_start = window_start - timedelta(days=1)
            window_start = window_start.replace(hour=6, minute=0, second=0, microsecond=0)
        else:
            window_start = (kNow - timedelta(days=span_days)).replace(minute=0, second=0, microsecond=0)
        window_start_str = f"{window_start.year}-{window_start.month:02d}-{window_start.day:02d}T{window_start.hour:02d}:{window_start.minute:02d}:00"
        
        filtered_cycles = []
        for c in cycles:
            end_time = c['end']['time']
            if end_time == 'running' or end_time > window_start_str:
                if c['start']['time'] < window_start_str:
                    c['start']['time'] = window_start_str
                filtered_cycles.append(c)
                
        if not filtered_cycles: return {"text": "N/A", "totalTime": "0 hr 0 min"}
        
        total_minutes = 0
        cycle_strings = []
        for c in filtered_cycles:
            try:
                d1 = datetime.fromisoformat(c['start']['time'])
            except:
                d1 = datetime.strptime(c['start']['time'], '%Y-%m-%dT%H:%M:%S')
            start_str = self._format_compact(c['start']['time'])
            end_str = ""
            if c['end']['time'] != "running":
                try:
                    d2 = datetime.fromisoformat(c['end']['time'])
                except:
                    d2 = datetime.strptime(c['end']['time'], '%Y-%m-%dT%H:%M:%S')
                total_minutes += round((d2 - d1).total_seconds() / 60)
                end_str = self._format_compact(c['end']['time'])
            else:
                try:
                    d2 = datetime.fromisoformat(history[-1]['time'])
                except:
                    d2 = datetime.strptime(history[-1]['time'], '%Y-%m-%dT%H:%M:%S')
                total_minutes += round((d2 - d1).total_seconds() / 60)
                end_str = f"RUNNING ({d2.strftime('%d %b')})"
            cycle_strings.append(f"• {start_str} - {end_str}")
            
        hrs = total_minutes // 60
        mins = total_minutes % 60
        if len(cycle_strings) > 8:
            cycle_strings = cycle_strings[:8] + [f"• +{len(cycle_strings) - 8} more cycles"]
        return {
            "text": "<br/>".join(cycle_strings),
            "totalTime": f"{hrs} hr {mins} min"
        }

    def _build_th_trend_rows(self, rows: list[dict]) -> list[dict]:
        trend_rows: list[dict] = []
        for row in rows:
            asset_name = str(row.get("asset_name") or "").strip()
            history = row.get("history_array")
            if not asset_name or not history:
                continue
            parsed_history = history
            if isinstance(history, str):
                try:
                    parsed_history = json.loads(history)
                except Exception:
                    parsed_history = []
            if not isinstance(parsed_history, list):
                continue
            for point in parsed_history:
                if not isinstance(point, dict):
                    continue
                trend_rows.append(
                    {
                        "asset_name": asset_name,
                        "timestamp": point.get("time"),
                        "temperature": point.get("temp"),
                    }
                )
        return trend_rows

    def _build_th_humidity_trend_rows(self, rows: list[dict]) -> list[dict]:
        trend_rows: list[dict] = []
        for row in rows:
            asset_name = str(row.get("asset_name") or "").strip()
            history = row.get("history_array")
            if not asset_name or not history:
                continue
            parsed_history = history
            if isinstance(history, str):
                try:
                    parsed_history = json.loads(history)
                except Exception:
                    parsed_history = []
            if not isinstance(parsed_history, list):
                continue
            for point in parsed_history:
                if not isinstance(point, dict):
                    continue
                humidity_value = point.get("humidity")
                if humidity_value is None:
                    continue
                trend_rows.append(
                    {
                        "asset_name": asset_name,
                        "timestamp": point.get("time"),
                        "humidity": humidity_value,
                    }
                )
        return trend_rows

    def _build_th_cooling_cycle_rows(self, rows: list[dict], offset: int = 0, span_days: int = 1) -> list[dict]:
        trend_rows: list[dict] = []
        for row in rows:
            asset_name = str(row.get("asset_name") or "").strip()
            history = row.get("history_array")
            if not asset_name or not history:
                continue
            parsed_history = history
            if isinstance(history, str):
                try:
                    parsed_history = json.loads(history)
                except Exception:
                    parsed_history = []
            if not isinstance(parsed_history, list):
                continue

            analytics = self._analyze_cooling_cycles(parsed_history, asset_name, False, offset, span_days)
            total_on_time = str(analytics.get("totalTime", "")).strip()
            match = re.search(r"(\d+)\s*(?:h|hr)\s*(\d+)\s*(?:m|min)", total_on_time)
            if not match:
                continue
            total_minutes = int(match.group(1)) * 60 + int(match.group(2))
            latest_point_time = parsed_history[-1].get("time") if parsed_history else None
            trend_rows.append(
                {
                    "asset_name": asset_name,
                    "timestamp": latest_point_time,
                    "cooling_on_minutes": total_minutes,
                }
            )
        return trend_rows

    def _build_filling_trend_rows(self, rows: list[dict], offset: int, span_days: int) -> list[dict]:
        names = [str(row.get("asset_name") or "").strip() for row in rows if row.get("asset_name")]
        names = sorted(set(name for name in names if name))
        if not names:
            return []

        safe_names = ", ".join("'" + name.replace("'", "''") + "'" for name in names)
        base_now = f"(NOW() - INTERVAL '{offset} days')"
        points_per_day = 12
        trend_sql = f"""
        WITH raw AS (
            SELECT
                a.name AS asset_name,
                ad.timestamp AS timestamp,
                COALESCE(
                    NULLIF(ad.value->>'prod_total_today', '')::numeric,
                    NULLIF(ad.value->>'total_count', '')::numeric
                ) AS total_count,
                date_trunc('day', ad.timestamp + INTERVAL '3 hours') AS day_bucket
            FROM openremote.asset_datapoint ad
            JOIN openremote.asset a ON a.id = ad.entity_id
            WHERE a.name IN ({safe_names})
              AND ad.timestamp >= {base_now} - INTERVAL '{span_days} days'
              AND ad.timestamp <= {base_now}
              AND (
                ad.value->>'prod_total_today' IS NOT NULL
                OR ad.value->>'total_count' IS NOT NULL
              )
        ),
        bucketed AS (
            SELECT
                r.*,
                ntile({points_per_day}) OVER (
                    PARTITION BY r.asset_name, r.day_bucket
                    ORDER BY r.timestamp
                ) AS sample_bucket
            FROM raw r
        )
        SELECT
            asset_name,
            timestamp,
            total_count
        FROM (
            SELECT
                b.*,
                row_number() OVER (
                    PARTITION BY b.asset_name, b.day_bucket, b.sample_bucket
                    ORDER BY b.timestamp DESC
                ) AS rn
            FROM bucketed b
        ) ranked
        WHERE rn = 1
        ORDER BY asset_name, timestamp;
        """
        return data_repository.execute_query(trend_sql)

    def _handle_th(self, offset=0, span_days=1) -> ChatResponse:
        sql = self._build_th_sql(offset=offset, span_days=span_days)
        rows = data_repository.execute_query(sql)
        date_str = self._get_window_label(offset=offset, span_days=span_days)
        if span_days > 1:
            for row in rows:
                history = row.get("history_array")
                if isinstance(history, str):
                    try:
                        history = json.loads(history)
                    except Exception:
                        history = []
                if isinstance(history, list):
                    row["history_array"] = self._sample_history(history, span_days)
        
        cold_rooms = [r for r in rows if not re.search(r'(ltroom|qadlab|r.*d.*lab)', str(r.get('asset_name', '')), re.IGNORECASE)]
        lab_rooms = [r for r in rows if re.search(r'(ltroom|qadlab|r.*d.*lab)', str(r.get('asset_name', '')), re.IGNORECASE)]
        
        answer = f"## Scheduled T & H Monitoring Report ({date_str})\n\n"
        answer += "### Temperature Monitoring (ColdRooms)\n\n"
        answer += "| Sr. | Device Name | Current Temp/Hum | Lowest | Highest | Cooling Cycles | Total ON Time | Recorded | Status |\n"
        answer += "|---|---|---|---|---|---|---|---|---|\n"
        
        for i, r in enumerate(cold_rooms):
            display_name = r['asset_name']
            if 'coldroom' in display_name.lower(): display_name += ' - BMR'
            has_no_data = not r['latest_temp'] or str(r['latest_temp']).lower() == 'null'
            hb_stale = str(r.get('is_heartbeat_stale', '')).lower() in ['true', 't']
            pl_stale = str(r.get('is_payload_stale', '')).lower() in ['true', 't']
            
            is_offline = hb_stale and pl_stale
            is_not_working = has_no_data or (not hb_stale and pl_stale)
            is_broken = is_offline or is_not_working
            is_prev = str(r.get('is_previous_shift', '')).lower() in ['true', 't']
            
            if is_prev or is_offline:
                status = "🔴 OFFLINE (Latest Data Not Available)" if is_prev else "🔴 OFFLINE"
            else:
                status = "🟡 NOT WORKING" if is_not_working else "🟢 ONLINE"
                
            analytics = self._analyze_cooling_cycles(r['history_array'], r['asset_name'], is_broken, offset, span_days)
            
            # Show the data even if outdated, only use N/A if it's logically empty
            cur = f"{r['latest_temp']}°C / {r['latest_humidity']}%" if r['latest_temp'] is not None else "N/A"
            low = f"{r['lowest_temp']}°C" if r['lowest_temp'] is not None else "N/A"
            high = f"{r['highest_temp']}°C" if r['highest_temp'] is not None else "N/A"
            cyc = analytics['text']
            time = analytics['totalTime']
            rec = r.get('latest_time', 'N/A')
            
            answer += f"| {i+1} | {display_name} | {cur} | {low} | {high} | {cyc} | {time} | {rec} | {status} |\n"
            
        answer += "\n### Temperature Monitoring (Labs & LTRoom)\n\n"
        answer += "| Sr. | Device Name | Current Temp/Hum | Lowest | Highest | Recorded | Status |\n"
        answer += "|---|---|---|---|---|---|---|\n"
        
        for i, r in enumerate(lab_rooms):
            hb_stale = str(r.get('is_heartbeat_stale', '')).lower() in ['true', 't']
            pl_stale = str(r.get('is_payload_stale', '')).lower() in ['true', 't']
            has_data = r['latest_temp'] is not None and str(r['latest_temp']) != 'null'
            
            is_offline = hb_stale and pl_stale
            is_not_working = not has_data or (not hb_stale and pl_stale)
            is_prev = str(r.get('is_previous_shift', '')).lower() in ['true', 't']
            
            if is_prev or is_offline:
                status = "🔴 OFFLINE (OUTDATED)" if is_prev else "🔴 OFFLINE"
            else:
                status = "🟡 NOT WORKING" if is_not_working else "🟢 ONLINE"
            
            # Use 'is_broken' logic internally to match ColdRooms display consistency
            is_broken = is_offline or is_not_working
            
            cur = f"{r['latest_temp']}°C / {r.get('latest_humidity', 'N/A')}%" if r['latest_temp'] is not None else "N/A"
            low = f"{r['lowest_temp']}°C" if r['lowest_temp'] is not None else "N/A"
            high = f"{r['highest_temp']}°C" if r['highest_temp'] is not None else "N/A"
            rec = r.get('latest_time', 'N/A')
            
            answer += f"| {i+1} | {r['asset_name']} | {cur} | {low} | {high} | {rec} | {status} |\n"
        
        insight_response = llm_service.analyze_result(
            user_question="Provide a brief, 2-3 sentence insight or summary highlighting any critical outliers or offline systems from this data.",
            asset_context="Scheduled T & H Monitoring",
            sql_query="[Bypassed with N8N logic]",
            deterministic_findings={"status": "Generated report.", "table": answer},
            rows=[]
        )
        
        final_answer = answer + f"\n\n### AI Insights\n\n{insight_response['answer']}"

        mock_plan = AnalysisPlan(
            analysis_name="scheduled_th_report",
            reasoning="Fast-path executed based on explicit keyword match.",
            time_window=TimeWindow(
                scope="historical",
                value=span_days,
                unit="days",
                label=f"Last {span_days} day{'s' if span_days != 1 else ''}",
            ),
            query_started_at=datetime.now().isoformat(),
            query_ended_at=datetime.now().isoformat(),
            query_window_label="Fixed Shift Window",
            planner_prompt="[BYPASSED]",
            sql_prompt="[BYPASSED]",
            analyst_prompt=insight_response.get("prompt_text", "[BYPASSED]"),
            sql_query=sql,
            chart_hints=[],
            total_tokens=insight_response.get("total_tokens", 0),
            warnings=["Report generated directly from DB using Schedule logic, bypassing LLM."],
            llm_raw_response=insight_response.get("raw_response", "[BYPASSED]")
        )
        fake_asset = AssetProfile(asset_id="fast_path", name="Scheduled Report", asset_type="report", description="Fast-path report table generation")
        fake_report = AnalysisReport(title="Scheduled T&H Monitoring", summary=final_answer, markdown=final_answer, html="")
        trend_rows = self._build_th_trend_rows(rows)
        humidity_trend_rows = self._build_th_humidity_trend_rows(rows)
        cooling_trend_rows = self._build_th_cooling_cycle_rows(rows, offset, span_days)
        trend_chart = trend_chart_service.build(
            fake_asset,
            mock_plan.analysis_name,
            mock_plan.time_window,
            trend_rows if trend_rows else rows,
        )
        trend_charts = [trend_chart] if trend_chart else []
        if humidity_trend_rows:
            humidity_chart = trend_chart_service.build(
                fake_asset,
                mock_plan.analysis_name,
                mock_plan.time_window,
                humidity_trend_rows,
            )
            if humidity_chart:
                humidity_chart.title = "Scheduled T&H humidity trend graph"
                humidity_chart.y_label = "Humidity"
                trend_charts.append(humidity_chart)
        if cooling_trend_rows:
            cooling_chart = trend_chart_service.build(
                fake_asset,
                mock_plan.analysis_name,
                mock_plan.time_window,
                cooling_trend_rows,
            )
            if cooling_chart:
                cooling_chart.title = "Scheduled T&H cooling-cycle trend graph"
                cooling_chart.y_label = "Cooling ON Minutes"
                trend_charts.append(cooling_chart)
        return ChatResponse(
            answer=final_answer, 
            analysis=AnalysisResponse(
                asset=fake_asset,
                plan=mock_plan,
                report=fake_report,
                trend_chart=trend_chart,
                trend_charts=trend_charts,
                rows=rows,
            )
        )

    def _handle_filling(self, offset=0, span_days=1) -> ChatResponse:
        sql = self._build_filling_sql(offset=offset, span_days=span_days)
        rows = data_repository.execute_query(sql)
        
        answer = f"## Filling Machines (End of Shift)\n\n"
        answer += "| Sr. | Device Name | Shift 1 | Shift 2 | Shift 3 | Total | Date Recorded | Status |\n"
        answer += "|---|---|---|---|---|---|---|---|\n"
        
        for i, r in enumerate(rows):
            is_stale = str(r.get('is_stale', '')).lower() in ['true', 't']
            is_prev = str(r.get('is_previous_shift', '')).lower() in ['true', 't']
            
            if is_prev or is_stale:
                status = "🔴 OFFLINE (Latest Data Not Available)" if is_prev else "🔴 OFFLINE"
            else:
                status = "🟢 ONLINE"
                
            date_val = f"{r.get('recorded_time')} 06:00 AM" if r.get('recorded_time') and r.get('recorded_time') != 'N/A' else 'N/A'
            answer += f"| {i+1} | {r.get('asset_name', 'N/A')} | {r.get('shift_1', 0)} | {r.get('shift_2', 0)} | {r.get('shift_3', 0)} | {r.get('total_count', 0)} | {date_val} | {status} |\n"

        insight_response = llm_service.analyze_result(
            user_question="Provide a brief, 2-3 sentence insight or summary highlighting any critical outliers or offline systems from this data.",
            asset_context="Scheduled Filling Machines",
            sql_query="[Bypassed with N8N logic]",
            deterministic_findings={"status": "Generated report.", "table": answer},
            rows=[]
        )
        final_answer = answer + f"\n\n### AI Insights\n\n{insight_response['answer']}"

        mock_plan = AnalysisPlan(
            analysis_name="scheduled_filling_report",
            reasoning="Fast-path executed based on explicit keyword match.",
            time_window=TimeWindow(
                scope="historical",
                value=span_days,
                unit="days",
                label=f"Last {span_days} day{'s' if span_days != 1 else ''}",
            ),
            query_started_at=datetime.now().isoformat(),
            query_ended_at=datetime.now().isoformat(),
            query_window_label="Fixed Shift Window",
            planner_prompt="[BYPASSED]",
            sql_prompt="[BYPASSED]",
            analyst_prompt=insight_response.get("prompt_text", "[BYPASSED]"),
            sql_query=sql,
            chart_hints=[],
            total_tokens=insight_response.get("total_tokens", 0),
            warnings=["Report generated directly from DB using Schedule logic, bypassing LLM."],
            llm_raw_response=insight_response.get("raw_response", "[BYPASSED]")
        )
        fake_asset = AssetProfile(asset_id="fast_path", name="Scheduled Report", asset_type="report", description="Fast-path report table generation")
        fake_report = AnalysisReport(title="Scheduled Filling Machines", summary=final_answer, markdown=final_answer, html="")
        
        trend_rows = self._build_filling_trend_rows(rows, offset, span_days)
        trend_chart = trend_chart_service.build(
            fake_asset,
            mock_plan.analysis_name,
            mock_plan.time_window,
            trend_rows if trend_rows else rows,
        )
        return ChatResponse(
            answer=final_answer, 
            analysis=AnalysisResponse(
                asset=fake_asset,
                plan=mock_plan,
                report=fake_report,
                trend_chart=trend_chart,
                trend_charts=[trend_chart] if trend_chart else [],
                rows=rows,
            )
        )

n8n_fast_path_service = N8NFastPathService()
