box::use(
  plumber[...],
  config[
    get
  ],
  dplyr[
    mutate,
    case_when,
    summarise,
    group_by,
    n,
    select
  ],
  lubridate[
    dmy,
    month,
    year
  ],
  tidyr[
    complete
  ],
  purrr[
    map_dbl
  ],
  checkmate[
    assert_subset
  ],
)

database_utils <- get("database_utils")

assert_subset(
  database_utils,
  c(
    "dynamodb",
    "supabase"
  )
)

if (database_utils == "supabase") {
  box::use(
    utils/supabase_utils[
      get_table_data,
      get_table_schema,
      put_table_row,
      delete_table_row
    ],
  )
}

if (database_utils == "dynamodb") {
  box::use(
    utils/dynamo_utils[
      get_table_data = get_processed_table_data,
      get_table_schema,
      put_table_row,
      delete_table_row
    ],
  )
}

box::use(
  utils/crud_utils[
    get_processed_table_data,
    get_table_schema,
    put_table_row,
    delete_table_row
  ]
)

# Helper ----

#' Function to help with API Authentication
#' @param res the response object from Plumber
#' @param req the request object from Plumber
#' @param FUN the function call if authentication succeeds
#' @param ... params to pass to the function handler
auth_helper <- function(
  res,
  req,
  FUN, #nolint: object_name_linter
  ...
) {
  req_has_key <- "HTTP_X_API_KEY" %in% names(req)
  key_is_valid <- req$HTTP_X_API_KEY == Sys.getenv("API_KEY")
  environment_not_set <- nchar(Sys.getenv("API_KEY")) <= 1
  if (!req_has_key || !key_is_valid || environment_not_set) {
    res$body <- "Unauthorized"
    res$status <- 401
    return("Missing or invalid API key, or invalid configuration!")
  } else {
    FUN(...) #nolint: object_name_linter
  }
}

# API Spec ----

#* @apiTitle Hrafnagud
#* @apiDescription An all-seeing API for personal use
#* @apiTag CRUD DynamoDb Utility Endpoints

## CRUD ----

### Schema ----

#* Schema
#* @param table_name:chr The table name to fetch the schema for.
#* @get /schema
#* @tag CRUD
function(
  res,
  req,
  table_name
) {
  auth_helper(
    res,
    req,
    get_table_schema,
    table_name = table_name
  )

}

### Create ----

#* New row
#* @param table_name:chr The table name to add the row to.
#* @param input_list:[chr] The list of values to add in the row.
#* @param show_old:logical Show the last values of the row?
#* @put /create
#* @tag CRUD
function(
  res,
  req,
  table_name,
  input_list,
  show_old
) {
  auth_helper(
    res,
    req,
    put_table_row,
    table_name = table_name,
    input_list = as.list(input_list),
    show_old = as.logical(show_old)
  )
}

### Read ----

#* Table
#* @param table_name:chr The table name to fetch data from.
#* @param limit:numeric The number of rows to limit at. Use 0 for all rows.
#* @get /read
#* @tag CRUD
function(
  res,
  req,
  table_name,
  limit
) {
  auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = table_name,
    limit = as.numeric(limit)
  )
}

### Update ----

#* Update row
#* @param table_name:chr The table name to modify the row in.
#* @param input_list:[chr] The list of values to add in the row.
#* @param show_old:logical Show the last values of the row?
#* @put /update
#* @tag CRUD
function(
  res,
  req,
  table_name,
  input_list,
  show_old
) {
  auth_helper(
    res,
    req,
    put_table_row,
    table_name = table_name,
    input_list = as.list(input_list),
    show_old = as.logical(show_old),
    is_update = TRUE
  )
}

### Delete ----

#* Delete row
#* @param table_name:chr The table name to remove the row from.
#* @param row_key:numeric The index of the row to delete.
#* @param show_old:logical Show the last values of the row?
#* @delete /delete
#* @tag CRUD
function(
  res,
  req,
  table_name,
  row_key,
  show_old
) {
  auth_helper(
    res,
    req,
    delete_table_row,
    table_name = table_name,
    id_value = as.numeric(row_key),
    show_old = as.logical(show_old)
  )
}
