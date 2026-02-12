WITH base AS (
    SELECT
        user_id,
        game_name,
        TO_DATE(payment_date, 'YYYY-MM-DD') AS payment_date,
        revenue_amount_usd,
        language,
        age
    FROM public.games_payments
),
monthly_revenue AS (
    SELECT
        user_id,
        DATE_TRUNC('month', payment_date)::date AS month,
        SUM(revenue_amount_usd) AS revenue,
        MAX(language) AS language,
        MAX(age) AS age
    FROM base
    GROUP BY 1,2
),
monthly_with_age_group AS (
    SELECT
        *,
        CASE
            WHEN age < 18 THEN '0-17'
            WHEN age BETWEEN 18 AND 24 THEN '18-24'
            WHEN age BETWEEN 25 AND 34 THEN '25-34'
            WHEN age BETWEEN 35 AND 44 THEN '35-44'
            WHEN age BETWEEN 45 AND 54 THEN '45-54'
            ELSE '55+'
        END AS age_group
    FROM monthly_revenue
),
first_payment AS (
    SELECT
        user_id,
        MIN(month) AS first_month
    FROM monthly_with_age_group
    GROUP BY 1
),
revenue_with_lag AS (
    SELECT
        m.*,
        LAG(revenue) OVER (PARTITION BY user_id ORDER BY month) AS prev_revenue
    FROM monthly_with_age_group m
),
monthly_metrics AS (
    SELECT
        r.month,
        r.language,
        r.age_group,
		SUM(r.revenue) AS mrr,
        COUNT(DISTINCT r.user_id) AS paid_users,
        SUM(r.revenue) / COUNT(DISTINCT r.user_id) AS arppu,
		COUNT(DISTINCT CASE WHEN f.first_month = r.month THEN r.user_id END) AS new_paid_users,
        SUM(CASE WHEN f.first_month = r.month THEN r.revenue ELSE 0 END) AS new_mrr,
		SUM(
            CASE
                WHEN r.prev_revenue IS NOT NULL AND r.revenue > r.prev_revenue
                THEN r.revenue - r.prev_revenue
                ELSE 0
            END
        ) AS expansion_mrr,
        SUM(
            CASE
                WHEN r.prev_revenue IS NOT NULL AND r.revenue < r.prev_revenue
                THEN r.prev_revenue - r.revenue
                ELSE 0
            END
        ) AS contraction_mrr
	FROM revenue_with_lag r
    LEFT JOIN first_payment f ON r.user_id = f.user_id
    GROUP BY 1,2,3
),
churn_calc AS (
    SELECT
        r1.month + INTERVAL '1 month' AS month,
        r1.language,
        r1.age_group,
        COUNT(DISTINCT r1.user_id) AS churned_users,
        SUM(r1.revenue) AS churned_revenue
    FROM monthly_with_age_group r1
    LEFT JOIN monthly_with_age_group r2
        ON r1.user_id = r2.user_id
        AND r2.month = r1.month + INTERVAL '1 month'
    WHERE r2.user_id IS NULL
    GROUP BY 1,2,3
)
SELECT
    m.month,
    m.language,
    m.age_group,
	m.mrr,
    m.paid_users,
    m.arppu,
	m.new_paid_users,
    m.new_mrr,
	COALESCE(c.churned_users,0) AS churned_users,
    COALESCE(c.churned_revenue,0) AS churned_revenue,
	m.expansion_mrr,
    m.contraction_mrr,
    COALESCE(c.churned_users,0)::numeric
    / NULLIF(LAG(m.paid_users) OVER (PARTITION BY m.language, m.age_group ORDER BY m.month),0)
    AS churn_rate,
    COALESCE(c.churned_revenue,0)::numeric
    / NULLIF(LAG(m.mrr) OVER (PARTITION BY m.language, m.age_group ORDER BY m.month),0)
    AS revenue_churn_rate
FROM monthly_metrics m
LEFT JOIN churn_calc c
    ON m.month = c.month
    AND m.language = c.language
    AND m.age_group = c.age_group
ORDER BY m.month;

