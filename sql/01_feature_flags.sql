SET search_path TO audit_analytics;

WITH base AS (
    SELECT
        g.*,
        COALESCE(ec.tz, g.time_zone) AS tz,
        COALESCE(ec.business_start, 8) AS business_start,
        COALESCE(ec.business_end, 18) AS business_end,
        COALESCE(ec.weekend_start, 6) AS weekend_start,
        COALESCE(ec.weekend_end, 0) AS weekend_end,
        g.amount::numeric AS amount_numeric,
        ABS(g.amount)::numeric AS abs_amount
    FROM gl_entries g
    LEFT JOIN entity_calendar ec USING (entity)
),
localized AS (
    SELECT
        b.*,
        (b.posting_timestamp AT TIME ZONE 'UTC') AT TIME ZONE b.tz AS posting_ts_local,
        EXTRACT(HOUR FROM ((b.posting_timestamp AT TIME ZONE 'UTC') AT TIME ZONE b.tz)) AS local_hour,
        EXTRACT(DOW FROM b.posting_date) AS dow
    FROM base b
),
-- Amount z-score by account to identify magnitude outliers.
amount_stats AS (
    SELECT
        account,
        AVG(abs_amount) AS avg_abs_amount,
        STDDEV_POP(abs_amount) AS std_abs_amount
    FROM localized
    GROUP BY account
),
scored AS (
    SELECT
        l.*,
        a.avg_abs_amount,
        NULLIF(a.std_abs_amount, 0) AS std_abs_amount,
        CASE
            WHEN a.std_abs_amount IS NULL OR a.std_abs_amount = 0 THEN 0
            ELSE (l.abs_amount - a.avg_abs_amount) / a.std_abs_amount
        END AS amount_z,
        CASE WHEN MOD(l.abs_amount, 100) = 0 THEN 1 ELSE 0 END AS round_dollar_flag,
        CASE WHEN l.local_hour < l.business_start OR l.local_hour >= l.business_end THEN 1 ELSE 0 END AS after_hours_flag,
        CASE WHEN l.dow IN (l.weekend_start, l.weekend_end) THEN 1 ELSE 0 END AS weekend_flag,
        CASE
            WHEN l.posting_date >= (date_trunc('month', l.posting_date) + interval '1 month - 3 day') THEN 1
            ELSE 0
        END AS period_close_flag,
        CASE WHEN l.approval_status IS NULL OR LOWER(l.approval_status) NOT IN ('approved','posted') THEN 1 ELSE 0 END AS approval_pending_flag,
        CASE WHEN l.description ILIKE '%gift%' OR l.description ILIKE '%manual%' THEN 1 ELSE 0 END AS keyword_flag
    FROM localized l
    LEFT JOIN amount_stats a USING (account)
),
user_month AS (
    SELECT
        created_by,
        date_trunc('month', posting_date) AS month_start,
        COUNT(DISTINCT entry_id) AS entries_posted
    FROM scored
    GROUP BY created_by, date_trunc('month', posting_date)
),
user_stats AS (
    SELECT
        created_by,
        AVG(entries_posted) AS avg_entries,
        STDDEV_POP(entries_posted) AS std_entries
    FROM user_month
    GROUP BY created_by
),
user_z AS (
    SELECT
        um.*,
        us.avg_entries,
        NULLIF(us.std_entries, 0) AS std_entries,
        CASE
            WHEN us.std_entries IS NULL OR us.std_entries = 0 THEN 0
            ELSE (um.entries_posted - us.avg_entries) / us.std_entries
        END AS entry_volume_z
    FROM user_month um
    LEFT JOIN user_stats us USING (created_by)
),
account_pairs AS (
    SELECT
        account,
        offset_account,
        COUNT(*) AS pair_count
    FROM scored
    GROUP BY account, offset_account
),
pair_rarity AS (
    SELECT
        ap.*,
        NTILE(4) OVER (PARTITION BY ap.account ORDER BY ap.pair_count DESC) AS pair_freq_quartile
    FROM account_pairs ap
),
flagged AS (
    SELECT
        s.*,
        COALESCE(uz.entry_volume_z, 0) AS user_volume_z,
        CASE WHEN pr.pair_freq_quartile = 4 OR pr.pair_count = 1 THEN 1 ELSE 0 END AS rare_pair_flag,
        CASE WHEN s.amount_z >= 2 THEN 1 ELSE 0 END AS amount_outlier_flag,
        CASE WHEN COALESCE(uz.entry_volume_z, 0) >= 2 THEN 1 ELSE 0 END AS user_volume_outlier_flag
    FROM scored s
    LEFT JOIN user_z uz ON uz.created_by = s.created_by AND uz.month_start = date_trunc('month', s.posting_date)
    LEFT JOIN pair_rarity pr ON pr.account = s.account AND pr.offset_account = s.offset_account
)
SELECT
    entry_id,
    entity,
    je_number,
    line_num,
    account,
    offset_account,
    description,
    amount,
    currency,
    debit_credit,
    posting_date,
    posting_timestamp,
    created_by,
    source,
    approval_status,
    amount_z,
    user_volume_z,
    round_dollar_flag,
    after_hours_flag,
    weekend_flag,
    period_close_flag,
    approval_pending_flag,
    keyword_flag,
    rare_pair_flag,
    amount_outlier_flag,
    user_volume_outlier_flag,
    -- Transparent risk score (weights can be tuned with business input).
    LEAST(
        100,
        0
        + round_dollar_flag * 10
        + after_hours_flag * 15
        + weekend_flag * 10
        + period_close_flag * 10
        + approval_pending_flag * 10
        + keyword_flag * 5
        + rare_pair_flag * 15
        + amount_outlier_flag * 20
        + user_volume_outlier_flag * 15
    ) AS risk_score
FROM flagged
ORDER BY risk_score DESC, posting_date DESC, entry_id;
