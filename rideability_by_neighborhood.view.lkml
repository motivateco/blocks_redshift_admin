view: rideability_by_neighborhood {
  derived_table: {
    sql: WITH rideability AS (SELECT *
          FROM nyc_ops.rideability AS R LEFT JOIN nyc_ops.neighborhoods AS N ON N.id = R.station_id
       )
SELECT * FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY z___min_rank) as z___pivot_row_rank, RANK() OVER (PARTITION BY z__pivot_col_rank ORDER BY z___min_rank) as z__pivot_col_ordering FROM (
SELECT *, MIN(z___rank) OVER (PARTITION BY "rideability.day_date","rideability.day_time","rideability.period") as z___min_rank FROM (
SELECT *, RANK() OVER (ORDER BY "rideability.day_date" DESC, z__pivot_col_rank, "rideability.day_time", "rideability.period") AS z___rank FROM (
SELECT *, DENSE_RANK() OVER (ORDER BY "rideability.neighborhood" NULLS LAST) AS z__pivot_col_rank FROM (
SELECT
  DATE(rideability.day ) AS "rideability.day_date",
  TO_CHAR(rideability.day , 'YYYY-MM-DD HH24:MI:SS') AS "rideability.day_time",
  rideability.period  AS "rideability.period",
  rideability.neighborhood  AS "rideability.neighborhood",
  (COALESCE(SUM(rideability.weighted_score ), 0))/(COALESCE(SUM(rideability.weight ), 0) + 0.01) AS "rideability.weighted_rideability"
FROM rideability

WHERE
  (((rideability.day ) >= ((DATEADD(day,-6, DATE_TRUNC('day',GETDATE()) ))) AND (rideability.day ) < ((DATEADD(day,7, DATEADD(day,-6, DATE_TRUNC('day',GETDATE()) ) )))))
GROUP BY 1,2,3,4) ww
) bb WHERE z__pivot_col_rank <= 16384
) aa
) xx
) zz
 WHERE z___pivot_row_rank <= 500 OR z__pivot_col_ordering = 1 ORDER BY z___pivot_row_rank
 ;;
  }

  measure: count {
    type: count
    drill_fields: [detail*]
  }

  dimension: rideability_day_date {
    type: date
    sql: ${TABLE}."rideability.day_date" ;;
  }

  dimension: rideability_day_time {
    type: string
    sql: ${TABLE}."rideability.day_time" ;;
  }

  dimension: rideability_period {
    type: number
    sql: ${TABLE}."rideability.period" ;;
  }

  dimension: rideability_neighborhood {
    type: string
    sql: ${TABLE}."rideability.neighborhood" ;;
  }

  dimension: rideability_weighted_rideability {
    type: number
    sql: ${TABLE}."rideability.weighted_rideability" ;;
  }

  dimension: z__pivot_col_rank {
    type: number
    sql: ${TABLE}.z__pivot_col_rank ;;
  }

  dimension: z___rank {
    type: number
    sql: ${TABLE}.z___rank ;;
  }

  dimension: z___min_rank {
    type: number
    sql: ${TABLE}.z___min_rank ;;
  }

  dimension: z___pivot_row_rank {
    type: number
    sql: ${TABLE}.z___pivot_row_rank ;;
  }

  dimension: z__pivot_col_ordering {
    type: number
    sql: ${TABLE}.z__pivot_col_ordering ;;
  }

  set: detail {
    fields: [
      rideability_day_date,
      rideability_day_time,
      rideability_period,
      rideability_neighborhood,
      rideability_weighted_rideability,
      z__pivot_col_rank,
      z___rank,
      z___min_rank,
      z___pivot_row_rank,
      z__pivot_col_ordering
    ]
  }
}
