box::use(
  supabaseR[
    get_table_query,
  ],
  glue[
    glue
  ],
  checkmate[
    assert,
    check_string
  ]
)

#' Calculate the staleness of the CRON cache
#' @param cron_time The CRON time object
#' @return A list containing the timestamp, difference, and status.
calculate_staleness <- function(
  cron_time
) {
  cron_date <- as.Date(cron_time$created_at)
  cron_time <- cron_time$cron_time
  current_time <- format(
    Sys.time(),
    "%Y-%m-%d %H:%M:%S",
    tz = Sys.getenv("TZ")
  )
  cron_today <- paste(
    cron_date,
    format(cron_time, "%H:%M:%S")
  )
  difference <- difftime(
    current_time,
    cron_today,
    units = "mins"
  ) |>
    as.numeric()
  list(
    cron_timestamp = cron_today,
    cron_difference = difference,
    cron_status = if (difference < 60) {
      "green"
    } else if (difference < 300 && difference > 60) {
      "yellow"
    } else {
      "red"
    }
  )
}

#' Get the cache age
#' @param endpoint The endpoint to check the age of.
#' @param table_name The name of the table.
#' @param schema The schema name.
#' @return A list containing the cache age and the difference in minutes.
#' @export
get_staleness <- function(
  endpoint = NULL,
  table_name = "hrafnagud_cache",
  schema = Sys.getenv("SUPABASE_SCHEMA")
) {
  assert(
    check_string(endpoint),
    check_string(table_name),
    check_string(schema),
    combine = "and"
  )
  cron_time <- get_table_query(
    table_name = table_name,
    schema = schema,
    columns = list(
      "created_at",
      "cron_time"
    ),
    filter_query = list(
      glue("WHERE endpoint = '{endpoint}'"),
      "ORDER BY created_at DESC",
      "LIMIT 1"
    )
  )
  calculate_staleness(cron_time)
}
