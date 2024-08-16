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
  ],
)

box::use(
`Hrafnagud-Dynamo`/utils/crud_utils[ # nolint
  # utils/crud_utils[ # nolint
    get_processed_table_data,
    get_table_schema,
    put_table_row,
    delete_table_row
  ],
`Hrafnagud-Dynamo`/utils/robin_utils[ # nolint
 # utils/robin_utils[ # nolint
    load_sheet
  ],
`Hrafnagud-Dynamo`/utils/midas_utils[ # nolint
 # utils/midas_utils[ # nolint
    get_mmtc_price,
    get_bullions_price
  ],
`Hrafnagud-Dynamo`/utils/ebenezer_utils[ # nolint
 # utils/ebenezer_utils[ # nolint
    calculate_portfolio,
    summarise_portfolio,
    calculate_funds,
    summarise_funds,
    summarise_deposits,
    summarise_savings,
    summarise_mmtc,
    summarise_sgbs
  ],
`Hrafnagud-Dynamo`/utils/chronos_utils[ # nolint
# utils/chronos_utils[ # nolint
    get_combined_calendars
  ],
`Hrafnagud-Dynamo`/utils/fogg_utils[ # nolint
# utils/fogg_utils[ # nolint
    get_labelled_tasks_df,
    get_tasks_analysis
  ],
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
#* @apiTag Robin Custom Google Finance Ticker Endpoints
#* @apiTag Midas Custom Gold Price Crawler Endpoints
#* @apiTag Livingston Trip-related Endpoints
#* @apiTag Ebenezer Finance-related Endpoints
#* @apiTag Fogg Todoist Task-related Endpoints
#* @apiTag Chronos Google Calendar-related Endpoints

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

## Livingston ----

### Trips ----

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

### Details ----

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

### Counts ----

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

## Robin ----

### Stocks ----

#* Stocks
#* @get /robin/stocks
#* @tag Robin
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

### Funds ----

#* Funds
#* @get /robin/funds
#* @tag Robin
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

### MMTC ----

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

### Bullions ----

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

### Stocks ----

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

### Mutual Funds ----

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

  calculate_funds(
    funds_data,
    ticker_data
  )
}

### Deposits ----

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

### Savings ----

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

### MMTC ----

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

### SGBs ----

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

### Portfolio ----

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

### Stocks (Portfolio) Summary ----

#* Stocks (Portfolio) Summary
#* @get /ebenezer/summary/stocks
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
  portfolio <- summarise_portfolio(
    stocks_data,
    ticker_data
  )
}

### Mutual Funds Summary ----

#* Mutual Funds Summary
#* @get /ebenezer/summary/funds
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
  summarise_funds(
    funds_data,
    ticker_data
  )
}

### Deposits Summary ----

#* Deposits Summary
#* @get /ebenezer/summary/deposits
#* @tag Ebenezer
function(
  res,
  req
) {
  deposits_data <- auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_deposits"
  )
  summarise_deposits(deposits_data)
}

### Savings Summary ----

#* Savings Summary
#* @get /ebenezer/summary/savings
#* @tag Ebenezer
function(
  res,
  req
) {
  savings_data <- auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_savings"
  )
  summarise_savings(savings_data)
}

### MMTC Summary ----

#* MMTC Summary
#* @get /ebenezer/summary/mmtc
#* @tag Ebenezer
function(
  res,
  req
) {
  mmtc_data <- auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_mmtc"
  )
  summarise_mmtc(
    mmtc_data,
    get_mmtc_price()
  )
}

### SGBs Summary ----

#* SGBs Summary
#* @get /ebenezer/summary/sgbs
#* @tag Ebenezer
function(
  res,
  req
) {
  sgbs_data <- auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_sgbs"
  )
  summarise_sgbs(
    sgbs_data,
    get_bullions_price()
  )
}

### Networth ----

#* Networth
#* @get /ebenezer/networth
#* @tag Ebenezer
function(
  res,
  req
) {

  sgbs_data <- auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_sgbs"
  )

  mmtc_data <- auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_mmtc"
  )

  savings_data <- auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_savings"
  )

  deposits_data <- auth_helper(
    res,
    req,
    get_processed_table_data,
    table_name = "ebenezer_deposits"
  )

  funds_ticker_data <- auth_helper(
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

  stocks_ticker_data <- auth_helper(
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

  networth <- list(
    "stocks" = summarise_portfolio(
      stocks_data,
      stocks_ticker_data
    )[
      c(
        "invested",
        "current"
      )
    ],
    "funds" = summarise_funds(
      funds_data,
      funds_ticker_data
    ),
    "deposits" = summarise_deposits(
      deposits_data
    ),
    "savings" = summarise_savings(
      savings_data
    ),
    "mmtc" = summarise_mmtc(
      mmtc_data,
      get_mmtc_price()
    ),
    "sgbs" = summarise_sgbs(
      sgbs_data,
      get_bullions_price()
    )
  )

  totals <- list(
    "invested" = lapply(
      networth,
      function(asset) asset$invested
    ) |>
      unlist() |>
      as.numeric() |>
      sum(),
    "current" = lapply(
      networth,
      function(asset) asset$current
    ) |>
      unlist() |>
      as.numeric() |>
      sum()
  )

  c(list("networth" = totals), networth)
}

## Chronos ----

#* Events
#* @get /chronos/events
#* @tag Chronos
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    get_combined_calendars
  )
}

## Fogg ----

#* Today's Tasks
#* @get /fogg/tasks
#* @tag Fogg
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    get_labelled_tasks_df
  )
}

#* Today's Task Analysis
#* @get /fogg/analysis
#* @tag Fogg
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    get_tasks_analysis
  )
}
