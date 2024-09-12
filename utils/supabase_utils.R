box::use(
  DBI[
    dbConnect,
    dbDisconnect,
    dbExecute,
    dbGetQuery,
    dbQuoteLiteral
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
    assert_string,
    check_string,
    check_logical,
    assert_list,
    check_numeric,
    check_list
  ],
)

#' Helper function generate an error message
generate_error <- function() {
  print(
    "Cannot connect to Supabase. Are environment variables set?"
  )
}

#' Read Supabase credentials from environment variables
#'
#' @return A list of Supabase credentials.
read_supabase_creds <- function() {
  creds <- list(
    host = Sys.getenv("SUPABASE_HOST"),
    port = 6543,
    dbname = Sys.getenv("SUPABASE_DBNAME"),
    user = Sys.getenv("SUPABASE_USER"),
    password = Sys.getenv("SUPABASE_PASSWORD")
  )
  if (any(sapply(creds, nchar) == 0)) {
    stop(generate_error())
  } else {
    creds
  }
}

#' Create a database connection
#'
#' @param supabase_creds A list of Supabase credentials.
#' @return A database connection object.
make_connection <- function(
  supabase_creds = read_supabase_creds()
) {
  dbConnect(
    Postgres(),
    host = supabase_creds$host,
    port = supabase_creds$port,
    dbname = supabase_creds$dbname,
    user = supabase_creds$user,
    password = supabase_creds$password
  )
}

#' Get a list of tables in the schema
#'
#' @param table_schema The schema name.
#' @param conn A database connection object.
#' @return A vector of table names.
get_table_list <- function(
  table_schema = "hrafnagud",
  conn = make_connection()
) {
  dbGetQuery(
    conn,
    glue_sql(
      "
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = {table_schema}
        AND table_type = 'BASE TABLE';
      ",
      .con = conn
    )
  ) |>
    as.list() |>
    unname() |>
    unlist()
}

#' Check if the table exists
#'
#' @param table_name The name of the table.
#' @param conn A database connection object.
#' @return TRUE if the table exists, FALSE otherwise.
is_valid_table <- function(
  table_name = NULL,
  conn = make_connection()
) {
  assert_string(table_name)
  table_name %in% get_table_list(conn = conn)
}

#' Get the latest key value for a table
#'
#' @param table_name The name of the table.
#' @param id_column The ID column name.
#' @param schema The schema name.
#' @param conn A database connection object.
#' @return The latest key value plus one.
get_latest_key <- function(
  table_name = NULL,
  id_column = "id",
  schema = "hrafnagud",
  conn = make_connection()
) {
  assert(
    check_string(table_name),
    check_string(id_column),
    combine = "and"
  )

  if (is_valid_table(table_name, conn)) {
    latest_key_query <- glue(
      "
        SELECT {id_column}
        FROM {schema}.{table_name}
        ORDER BY {id_column} DESC
        LIMIT 1
      ",
      .con = conn
    )
    latest_key <- dbGetQuery(conn, latest_key_query)

    if (nrow(latest_key) > 0) {
      as.numeric(latest_key[[1]])
    } else {
      1
    }
  } else {
    stop(
      glue(
        "Table '{table_name}' does not exist!"
      )
    )
  }
}

#' Retrieve the schema of a table
#'
#' @param table_name The name of the table.
#' @param schema The schema name.
#' @param conn A database connection object.
#' @return A data frame with table schema details.
#' @export
get_table_schema <- function(
  table_name = NULL,
  schema = "hrafnagud",
  conn = make_connection()
) {
  assert(
    check_string(table_name),
    check_string(schema),
    combine = "and"
  )
  if (is_valid_table(table_name, conn)) {
    schema_query <- glue(
      "
        SELECT column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = {schema}
        AND table_name = {table_name}
      ",
      .con = conn
    )
    dbGetQuery(conn, schema_query)
  } else {
    stop(
      glue(
        "Table '{table_name}' does not exist!"
      )
    )
  }
}

#' Retrieve data from a table
#'
#' @param table_name The name of the table.
#' @param limit The number of rows to retrieve.
#' @param schema The schema name.
#' @param conn A database connection object.
#' @return A data frame with table data.
#' @export
get_table_data <- function(
  table_name = NULL,
  limit = 0,
  schema = "hrafnagud",
  conn = make_connection()
) {
  on.exit(dbDisconnect(conn))
  assert(
    check_string(table_name),
    check_numeric(limit),
    combine = "and"
  )

  query_filter <- if (limit > 0) glue("LIMIT {limit}") else ""

  if (is_valid_table(table_name, conn)) {
    dbGetQuery(
      conn,
      glue(
        "
          SELECT * FROM {schema}.{table_name}
          {query_filter}
        ",
        .con = conn
      )
    )
  } else {
    stop(
      glue(
        "Table '{table_name}' does not exist!"
      )
    )
  }
}

#' Insert or update a table row
#'
#' @param table_name The name of the table.
#' @param input_list A list of column-value pairs.
#' @param is_update Whether the operation is an update.
#' @param schema The schema name.
#' @param conn A database connection object.
#' @export
put_table_row <- function(
  table_name = NULL,
  input_list = list(),
  is_update = FALSE,
  schema = "hrafnagud",
  conn = make_connection()
) {
  assert(
    check_string(table_name),
    check_list(input_list),
    check_logical(is_update),
    combine = "and"
  )

  if (is_valid_table(table_name, conn)) {

    columns <- names(input_list)
    values <- lapply(input_list, function(x) dbQuoteLiteral(conn, x))

    if (is_update) {
      set_clause <- glue_collapse(
        mapply(function(col, val) glue("{col} = {val}"), columns, values, SIMPLIFY = FALSE),
        sep = ", "
      )
      query <- glue(
        "
          UPDATE {schema}.{table_name}
          SET {set_clause}
          WHERE id = {input_list[['id']]}
        "
      )
    } else {
      if (!"id" %in% columns) {
        input_list[["id"]] <- 1 + get_latest_key(
          table_name,
          schema = schema,
          conn = conn
      )
        columns <- names(input_list)
        values <- lapply(input_list, function(x) dbQuoteLiteral(conn, x))
      }

      query <- glue(
        "
          INSERT INTO {schema}.{table_name}
          ({glue_collapse(columns, sep = ', ')})
          VALUES ({glue_collapse(values, sep = ', ')})
        "
      )
    }
    dbExecute(conn, query)
  } else {
    stop(
      glue(
        "Table '{table_name}' does not exist!"
      )
    )
  }
}

#' Delete a row from a table
#'
#' @param table_name The name of the table.
#' @param id_value The ID value of the row to delete.
#' @param id_column The ID column name.
#' @param schema The schema name.
#' @param conn A database connection object.
#' @export
delete_table_row <- function(
  table_name = NULL,
  id_value = NULL,
  id_column = "id",
  schema = "hrafnagud",
  conn = make_connection()
) {
  assert(
    check_string(table_name),
    check_numeric(id_value),
    check_string(id_column),
    combine = "and"
  )

  if (is_valid_table(table_name, conn)) {
    delete_query <- glue(
      "
        DELETE FROM {schema}.{table_name}
        WHERE {id_column} = {id_value}
      "
    )
    dbExecute(conn, delete_query)
  } else {
    stop(
      glue(
        "Table '{table_name}' does not exist!"
      )
    )
  }
}
