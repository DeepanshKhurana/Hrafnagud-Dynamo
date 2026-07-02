box::use(
  supabaseR[
    sb_db_query,
    sb_db_schema,
    sb_db_insert,
    sb_db_update,
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
  cron_date <- as.Date(
    format(
      cron_time$created_at,
      tz = Sys.getenv("TZ")
    )
  )
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
    } else if (difference < 300) {
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
  cron_time <- sb_db_query(
    sql = glue(
      "SELECT created_at, cron_time FROM {schema}.{table_name}
       WHERE endpoint = '{endpoint}'
       ORDER BY created_at DESC
       LIMIT 1"
    )
  )
  calculate_staleness(cron_time)
}

#' Reimplementation of supabaseR (pre-1.0)'s get_cron_time(), which the
#' 1.0 rewrite dropped from the package's public API entirely.
#' @param time The time to round down to a cron slot
#' @param tz The timezone to interpret `time` in
#' @export
get_cron_time <- function(
  time = Sys.time(),
  tz = "Asia/Kolkata"
) {
  minute <- as.numeric(format(time, "%M", tz = tz))
  hour <- as.numeric(format(time, "%H", tz = tz))
  glue("{hour}:{ifelse(minute < 15, '00', ifelse(minute < 45, '30', '00'))}")
}

#' Get the next id for a table
#'
#' supabaseR (pre-1.0) auto-computed the next id as 1 + the current max
#' id on every insert, since id columns here have no database-level
#' default. sb_db_insert() in 1.0+ is a plain data.frame append with no
#' id handling, so this is reimplemented here.
#' @param table_name The table to compute the next id for
#' @param schema The schema the table lives in
#' @export
get_next_id <- function(
  table_name,
  schema = Sys.getenv("SUPABASE_SCHEMA")
) {
  latest <- sb_db_query(
    sql = glue(
      "SELECT id FROM {schema}.{table_name} ORDER BY id DESC LIMIT 1"
    )
  )
  latest_key <- if (nrow(latest) > 0) as.numeric(latest[[1]]) else 1
  1 + latest_key
}

#' Build a named, typed row from a positional input list
#'
#' supabaseR (pre-1.0)'s put_table_row() matched an unnamed input_list
#' positionally against the table's own column order (everything but
#' created_at) and coerced each value to the column's declared type.
#' sb_db_insert()/sb_db_update() in 1.0+ expect already-named,
#' already-typed input, so that matching is reimplemented here.
#' @param table_name The table the row belongs to
#' @param input_list Unnamed list of values, in column order
#' @param id If given, prepended as the row's id (new rows only)
#' @export
build_table_row <- function(
  table_name,
  input_list,
  id = NULL
) {
  table_schema <- sb_db_schema(table = table_name)
  columns <- table_schema$column_name[table_schema$column_name != "created_at"]

  if (!is.null(id)) {
    input_list <- c(id = id, input_list)
  }
  names(input_list) <- columns

  type_caster <- list(
    smallint = as.numeric,
    integer = as.integer,
    bigint = as.numeric,
    real = as.numeric,
    "double precision" = as.numeric,
    numeric = as.numeric,
    json = as.character,
    jsonb = as.character,
    text = as.character,
    "character varying" = as.character,
    uuid = as.character,
    date = function(x) as.Date(x, format = "%Y-%m-%d"),
    "time without time zone" = as.character,
    "time with time zone" = as.character,
    "timestamp without time zone" = as.character,
    "timestamp with time zone" = as.character,
    boolean = as.logical
  )
  data_types <- table_schema$data_type
  names(data_types) <- table_schema$column_name

  Map(
    function(value, column) type_caster[[data_types[[column]]]](as.character(value)),
    input_list,
    columns
  )
}

#' Insert a new row built from a positional input list
#' @param table_name The table to insert into
#' @param input_list Unnamed list of values, in column order
#' @export
create_table_row <- function(
  table_name,
  input_list
) {
  row <- build_table_row(
    table_name = table_name,
    input_list = input_list,
    id = get_next_id(table_name)
  )
  sb_db_insert(
    table = table_name,
    data = as.data.frame(row, stringsAsFactors = FALSE)
  )
}

#' Update an existing row built from a positional input list
#'
#' supabaseR (pre-1.0) took the first element of input_list as the row's
#' id (used for the WHERE clause) and set the rest of the columns.
#' @param table_name The table to update
#' @param input_list Unnamed list starting with the row's id, followed
#' by the remaining values in column order
#' @export
update_table_row <- function(
  table_name,
  input_list
) {
  id_value <- input_list[[1]]
  row <- build_table_row(
    table_name = table_name,
    input_list = input_list[-1],
    id = id_value
  )
  sb_db_update(
    table = table_name,
    data = row[names(row) != "id"],
    where = list(id = id_value)
  )
}
