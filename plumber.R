box::use(
  plumber[...],
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
  ]
)

box::use(
#  `Hrafnagud-Dynamo`/utils/dynamodb_utils[
  utils/dynamodb_utils[
    get_processed_table_data,
    get_table_schema,
    put_table_row,
    delete_table_row
  ],
#  `Hrafnagud-Dynamo`/utils/sheets_utils[
  utils/sheets_utils[
    load_sheet
  ],
#  `Hrafnagud-Dynamo`/utils/gold_utils[
  utils/gold_utils[
    get_mmtc_price,
    get_bullions_price
  ],
# `Hrafnagud-Dynamo`/utils/stocks_utils[
  utils/stocks_utils[
    calculate_portfolio
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
#* @apiTag Ticker Custom Google Finance Endpoints
#* @apiTag Midas Custom Gold Price Endpoints
#* @apiTag Livingston Trip-related Endpoints
#* @apiTag Ebenezer Finance-related Endpoints
#* @apiTag Fogg Task-related Endpoints

## CRUD ----

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
    row_key = as.numeric(row_key),
    show_old = as.logical(show_old)
  )
}

## Livingston ----

#* Trips
#* @get /livingston/trips
#* @tag Livingston
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "livingston_trips"
  )
}

#* Details
#* @get /livingston/details
#* @param trip_id:numeric The id of the trip to fetch details for
#* @tag Livingston
function(
  res,
  req,
  trip_id
) {

  # TODO Make functions here to wrap inside the third param
  data <- auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "livingston_details"
  )
  data[
    as.numeric(data$trip_id) == as.numeric(trip_id),
  ]
}

#* Counts
#* @get /livingston/counts
#* @tag Livingston
function(
  res,
  req
) {
  data <- auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "livingston_trips"
  )

  current <- Sys.Date()

  data |>
    mutate(start = dmy(start_date),
           end = dmy(end_date)) |>
    mutate(trip_status = case_when(
      end < current ~ "Past",
      start <= current & end >= current ~ "Ongoing",
      start > current ~ "Upcoming"
    )) |>
    group_by(trip_status) |>
    summarise(trip_count = n()) |>
    complete(
      trip_status = c("Past", "Ongoing", "Upcoming"),
      fill = list(trip_count = 0)
    )
}

## Ticker ----

#* Stocks
#* @get /ticker/stocks
#* @tag Ticker
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    load_sheet,
    sheet_name = "Stocks"
  )
}

#* Funds
#* @get /ticker/funds
#* @tag Ticker
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    load_sheet,
    sheet_name = "Funds"
  )
}

## Midas ----

#* MMTC
#* @get /midas/mmtc
#* @tag Midas
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    get_mmtc_price
  )
}

#* Bullions
#* @get /midas/bullions
#* @tag Midas
function(
    res,
    req
) {
  auth_helper(
    res,
    req,
    get_bullions_price
  )
}

## Ebenezer ----

#* Stocks
#* @get /ebenezer/stocks
#* @tag Ebenezer
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_stocks"
  )
}

#* Mutual Funds
#* @get /ebenezer/funds
#* @tag Ebenezer
function(
  res,
  req
) {
  ticker_data <- auth_helper(
    res,
    req,
    load_sheet,
    sheet_name = "Funds"
  )

  funds_data <- auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_funds"
  )

  merge(
    funds_data |>
      select(-provider),
    ticker_data,
    by.x = "name",
    by.y = "fund_name"
  )
}

#* Deposits
#* @get /ebenezer/deposits
#* @tag Ebenezer
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_deposits"
  )
}

#* Savings
#* @get /ebenezer/savings
#* @tag Ebenezer
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_savings"
  )
}

#* MMTC
#* @get /ebenezer/mmtc
#* @tag Ebenezer
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_mmtc"
  )
}

#* SGBs
#* @get /ebenezer/sgbs
#* @tag Ebenezer
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_sgbs"
  )
}

#* Portfolio
#* @get /ebenezer/portfolio
#* @tag Ebenezer
function(
  res,
  req
) {
  ticker_data <- auth_helper(
      res,
      req,
      load_sheet,
      sheet_name = "Stocks"
    )

  stocks_data <- auth_helper(
      res,
      req,
      get_processed_table_data,
      table_name = "ebenezer_stocks"
    )

  calculate_portfolio(
    stocks_data,
    ticker_data
  )

}
