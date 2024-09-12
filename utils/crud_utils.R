box::use(
  paws[
    dynamodb
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
  glue[glue],
  dplyr[
    na_if,
    mutate_all,
    arrange,
    select
  ],
  methods[as],
  stats[setNames],
  utils[
    tail
  ]
)

box::use(
  utils/global_utils[
    generate_error
  ]
)

#' Simple function to check if table name is valid
#' @param table_name Character string. Table name to validate.
#' @param conn `paws::dynamodb()` object. Dependent on envvars.
#' @return Logical. Whether table exists or not.
#' @export
is_valid_table <- function(
  table_name = NULL,
  conn = dynamodb()
) {
  assert_string(table_name)
  if (!is.null(conn)) {
    table_name %in% conn$list_tables()$TableNames
  } else {
    stop(generate_error())
  }
}

#' Function to get the table data from DynamoDb
#' @param table_name Character string. Table name to validate.
#' @param limit Numeric. If 0, then no limit is set.
#' @param conn `paws::dynamodb()` object. Dependent on envvars.
#' @return `list` object with the data
get_table_data <- function(
  table_name = NULL,
  limit = 0,
  conn = dynamodb()
) {
  assert(
    check_string(table_name),
    check_numeric(limit),
    combine = "and"
  )

  if (is_valid_table(table_name, conn)) {
    conn$scan(
      TableName = table_name,
      Limit = ifelse(
        limit == 0,
        NA,
        limit
      ),
      ConsistentRead = TRUE
    )
  } else {
    stop(
      glue(
        "Table '{table_name}' does not exist!"
      )
    )
  }
}

#' Function to process the object received from DynamoDb
#' @param scan_data `list` object with the data
#' @param null_list Character vector. Default is "NULL" and ""
#' @return `data.frame` object
process_table_scan <- function(
  scan_data = NULL,
  null_list = c("NULL", "")
) {
  assert_list(scan_data)
  items <- lapply(scan_data$Items, unlist)
  items <- lapply(
    items, function(x) {
      x[sort(names(x))]
    }
  ) |>
    data.frame() |>
    t() |>
    data.frame()

  column_names <- colnames(items)

  # Use the X.Type syntax to fix types

  items <- fix_data_types(items)

  # Parse the nested list

  colnames(items) <- lapply(
    strsplit(
      column_names, "\\."
    ), function(x) unlist(x)[[1]]
  ) |>
    unlist()

  # Find rownames & replace "NULL" with NA

  rownames(items) <- items$id
  items[
    order(as.numeric(items$id)),
  ] |>
    mutate_all(
      ~ ifelse(
        . %in% null_list,
        NA,
        .
      )
    )
}

#' Wrapper to both get and process the table received from DynamoDb
#' @param table_name Character string. Table name to fetch.
#' @param limit Numeric. If 0, then no limit is set.
#' @param conn `paws::dynamodb()` object. Dependent on envvars.
#' @return `data.frame` object
#' @export
get_processed_table_data <- function(
  table_name = NULL,
  limit = 0,
  conn = dynamodb()
) {
  process_table_scan(
    get_table_data(
      table_name,
      limit,
      conn
    )
  )
}

#' A simple function to get the mapping list for types
#' @return List. Data type map for DynamoDb and R
get_type_mapping <- function() {
  list(
    S = "character",
    N = "numeric",
    B = "raw",
    BOOL = "logical",
    `NULL` = "character",
    M = "list",
    L = "list",
    SS = "list",
    NS = "list",
    BS = "list"
  )
}

#' Fix data types in a dataframe
#'
#' @param dataframe The dataframe to be processed.
#' @param type_mapping A list specifying the mapping of DynamoDb types to
#' R data types. Default is `get_type_mapping()`
#' @return The dataframe with corrected data types.
fix_data_types <- function(
  dataframe,
  type_mapping = get_type_mapping()
) {
  col_names <- names(dataframe)
  types <- gsub(".*\\.", "", col_names)
  dataframe <- data.frame(
    lapply(seq_along(col_names), function(column) {
      as(dataframe[[column]], type_mapping[[types[column]]])
    })
  )
}

#' Get the schema of a table
#'
#' @param table_name Character string. Table name to fetch schema for
#' @return A named list with the correct data types
#' @export
get_table_schema <- function(
  table_name = NULL,
  conn = dynamodb()
) {
  assert_string(table_name)
  if (is_valid_table(table_name, conn)) {
    data_row <- get_processed_table_data(table_name, limit = 1, conn)
    list(
      classes = lapply(
        lapply(
          data_row,
          function(value) ifelse(!is.na(value), value, "")
        ),
        class
      ),
      sample = data_row
    )
  } else {
    stop(
      glue(
        "Table '{table_name}' does not exist!"
      )
    )
  }
}

#' Add or update a row in a table
#'
#' @param table_name Character string. Table name to use
#' @param input_list List. The data to enter into the row
#' @param type_mapping List. A list of mapping for DynamoDb & R classes
#' @param is_update Logical. Whether this is an update or a new row
#' @param conn `paws::dynamodb()` object. Dependent on envvars.
#'
#' @export
put_table_row <- function(
  table_name = NULL,
  input_list = list(),
  type_mapping = get_type_mapping(),
  is_update = FALSE,
  conn = dynamodb()
) {

  assert(
    check_string(table_name),
    check_list(type_mapping),
    check_list(input_list),
    check_logical(is_update),
    combine = "and"
  )

  if (is_valid_table(table_name, conn)) {

    put_list <- process_table_input_row(
      input_list = input_list,
      table_name = table_name,
      type_mapping = type_mapping,
      is_update = is_update,
      conn = conn
    )

    conn$put_item(
      TableName = table_name,
      Item = put_list,
      ReturnValues = "NONE"
    )

  } else {
    stop(
      glue(
        "Table '{table_name}' does not exist!"
      )
    )
  }
}

#' Helper function for `put_table_row`
#'
#' @param table_name Character string. Table name to use
#' @param input_list List. The data to enter into the row
#' @param type_mapping List. A list of mapping for DynamoDb & R classes
#' @param is_update Logical. Whether this is an update or a new row
#' @param conn `paws::dynamodb()` object. Dependent on envvars.
#'
process_table_input_row <- function(
  table_name = NULL,
  input_list = NULL,
  type_mapping = get_type_mapping(),
  is_update = FALSE,
  conn = dynamodb()
) {

  schema <- get_table_schema(table_name)

  type_mapping <- setNames(
    names(type_mapping),
    type_mapping
  )

  data_contract <- lapply(
    schema$classes,
    function(class) type_mapping[class][[1]]
  )

  if (is_update) {
    put_condition <- length(input_list) == length(schema$classes)
  } else {
    put_condition <- length(input_list) == length(schema$classes) - 1
  }

  if (put_condition) {

    put_list <- do.call(
      c,
      lapply(
        names(data_contract),
        function(column_name) {

          db_class <- data_contract[[column_name]][[1]]
          key_class <- names(
            which(
              type_mapping == db_class
            )
          )

          # Auto-increment if not updating a row

          if (is_update) {
            key_value <- input_list[[
              which(names(data_contract) == column_name)
            ]]
          }

          if (!is_update) {
            adjusted_contract <- data_contract[names(data_contract) != "id"]
            if (column_name == "id") {
              key_value <- as.numeric(
                get_latest_key(table_name, conn = conn) + 1
              )
            } else {
              key_value <- input_list[[
                which(names(adjusted_contract) == column_name)
              ]]
            }
          }

          list(
            setNames(
              list(
                tryCatch(
                  as(key_value, key_class),
                  error = function(e, key_value, db_class) {
                    stop(
                      glue(
                        "Conversion error. {key_value} invalid for {db_class}"
                      )
                    )
                  }
                )
              ),
              db_class
            )
          )
        }
      )
    )
    names(put_list) <- names(schema$classes)
    put_list
  } else {
    stop(
      "Invalid number of values provided. Please double-check the schema."
    )
  }
}

#' Get the latest key for the table
#'
#' @param table_name Character string. Table name to use
#' @param id_column Character. The id column name. Defaults to "id"
#' @param conn `paws::dynamodb()` object. Dependent on envvars.
#'
get_latest_key <- function(
  table_name = NULL,
  id_column = "id",
  conn = dynamodb()
) {
  if (is_valid_table(table_name, conn)) {
    get_processed_table_data(table_name, conn = conn) |>
      arrange(`id_column`, "desc") |>
      tail(1) |>
      select(`id_column`) |>
      as.integer()
  }  else {
    stop(
      glue(
        "Table '{table_name}' does not exist!"
      )
    )
  }
}

#' Delete a table row (irreversible)
#'
#' @param table_name Character string. Table name to use
#' @param id_value The value or index of the row. Defaults to NULL
#' @param id_column Character. The id column name. Defaults to "id"
#' @param conn `paws::dynamodb()` object. Dependent on envvars.
#'
#' @export
delete_table_row <- function(
  table_name = NULL,
  id_value = NULL,
  id_column = "id",
  conn = dynamodb()
) {

  assert(
    check_string(table_name),
    check_numeric(id_value),
    check_string(id_column),
    combine = "and"
  )

  key <- list()
  key[[id_column]] <- list(N = id_value)

  conn$delete_item(
    table_name,
    Key = key,
    ReturnValues = "NONE"
  )
}
