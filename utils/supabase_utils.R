box::use(
  DBI[
    dbConnect,
    dbDisconnect,
    dbExecute,
    dbGetQuery,
    dbQuoteLiteral
  ],
  dplyr[
    pull
  ],
  purrr[
    map2
  ],
  RPostgres[
    Postgres
  ],
  glue[
    glue,
    glue_collapse,
    glue_sql,
    glue_sql_collapse
  ],
  checkmate[
    assert,
    assert_class,
    assert_string,
    check_string,
    check_logical,
    assert_list,
    check_numeric,
    check_list,
    check_class
  ],
  logger[
    log_error
  ],
  supabaseR[
    get_table_query,
    get_table_schema,
    get_table_data,
    put_table_row,
    delete_table_row
  ],
)

#' Retrieve the schema of a table
#'
#' @param table_name The name of the table.
#' @param schema The schema name.
#' @return A data frame with table schema details.
#' @export
get_table_schema <- function(
  table_name = NULL,
  schema = "hrafnagud"
) {
  get_table_schema(
    table_name = table_name,
    schema = schema
  )
}

#' Retrieve data from a table
#'
#' @param table_name The name of the table.
#' @param limit The number of rows to retrieve.
#' @param schema The schema name.
#' @return A data frame with table data.
#' @export
get_table_data <- function(
  table_name = NULL,
  limit = 0,
  schema = "hrafnagud"
) {
  get_table_data(
    table_name = table_name,
    limit = limit,
    schema = schema
  )
}

#' Insert or update a table row
#'
#' @param table_name The name of the table.
#' @param input_list A list of column-value pairs.
#' @param is_update Whether the operation is an update.
#' @param schema The schema name.
#' @export
put_table_row <- function(
  table_name = NULL,
  input_list = list(),
  is_update = FALSE,
  schema = "hrafnagud"
) {
  put_table_row(
    table_name = table_name,
    input_list = input_list,
    is_update = is_update,
    schema = schema
  )
}

#' Delete a row from a table
#'
#' @param table_name The name of the table.
#' @param id_value The ID value of the row to delete.
#' @param id_column The ID column name.
#' @param schema The schema name.
#' @export
delete_table_row <- function(
  table_name = NULL,
  id_value = NULL,
  id_column = "id",
  schema = "hrafnagud"
) {
  delete_table_row(
    table_name = table_name,
    id_value = id_value,
    id_column = id_column,
    schema = schema
  )
}

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
    "%Y-%m-%d %H:%M:%S"
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
#' @param table_name The name of the table.
#' @param schema The schema name.
#' @return A list containing the cache age and the difference in minutes.
#' @export
get_staleness <- function(
  table_name = "hrafnagud_cache",
  schema = "hrafnagud"
) {
  cron_time <- get_table_query(
    table_name = table_name,
    schema = schema,
    columns = list(
      "created_at",
      "cron_time"
    ),
    filter_query = list(
      "ORDER BY created_at DESC",
      "LIMIT 1"
    )
  )
  calculate_staleness(cron_time)
}
