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
    ymd,
    month,
    year
  ],
  tidyr[
    complete
  ],
  memoise[
    memoise
  ],
  glue[
    glue
  ],
  checkmate[
    assert_subset
  ],
  jsonlite[
    fromJSON
  ],
  supabaseR[
    get_table_query
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
    utils/supabase_utils[ # nolint
      get_table_data,
      get_table_schema,
      put_table_row,
      delete_table_row,
      get_staleness
    ],
  )
}

if (database_utils == "dynamodb") {
  box::use(
    utils/dynamo_utils[ # nolint
      get_table_data = get_processed_table_data,
      get_table_schema,
      put_table_row,
      delete_table_row
    ],
  )
}

box::use(
  utils/robin_utils[ # nolint
    load_sheet
  ],
 utils/midas_utils[ # nolint
    get_mmtc_price,
    get_bullions_price
  ],
 utils/ebenezer_utils[ # nolint
    calculate_portfolio,
    summarise_portfolio,
    calculate_funds,
    summarise_funds,
    summarise_deposits,
    summarise_savings,
    summarise_mmtc,
    summarise_sgbs
  ],
utils/fogg_utils[ # nolint
    get_labelled_tasks_df,
    get_tasks_analysis
  ],
utils/icarus_utils[ #nolint
    get_flight_data
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
    memoised <- memoise(FUN) #nolint: object_name_linter
    memoised(
      ...
    )
  }
}

#' Retrieve cached API response from a database
#'
#' Fetches the most recent cached response from the specified cache table
#'
#' @param req_path Character. The request path for the cached response
#' @param cache_table Character. The name of the cache table
#' @param cache_schema Character. The schema of the cache table
#' @param cache_columns List. The columns to retrieve from the cache table
#'
#' @return A parsed JSON object representing the cached response
cache_helper <- function(
  req_path = NULL,
  cache_table = "hrafnagud_cache",
  cache_schema = "hrafnagud",
  cache_columns = list("response")
) {
  if (substr(req_path, 1, 1) == "/") {
    req_path <- substr(req_path, 2, nchar(req_path))
  }
  get_table_query(
    cache_table,
    schema = cache_schema,
    columns = cache_columns,
    filter_query = list(
      glue(
        "WHERE endpoint = '{req_path}'"
      ),
      "ORDER BY created_at DESC",
      "LIMIT 1"
    )
  ) |>
    as.character() |>
    fromJSON()
}

# API Spec ----

#* @apiTitle Hrafnagud
#* @apiDescription An all-seeing API for personal use
#* @apiTag Health Health-check Endpoint
#* @apiTag CRUD DynamoDb Utility Endpoints
#* @apiTag Robin Custom Google Finance Ticker Endpoints
#* @apiTag Midas Custom Gold Price Crawler Endpoints
#* @apiTag Icarus Flight Data AviationStack Endpoints
#* @apiTag Livingston Trip-related Endpoints
#* @apiTag Ebenezer Finance-related Endpoints
#* @apiTag Fogg Todoist Task-related Endpoints
#* @apiTag Chronos Google Calendar-related Endpoints

## Health ----

### Health Check ----

#* Health Check
#* @get /health
#* @tag Health
function() {
  "API is healthy!"
}

### Cache Staleness ----

#* Cache Staleness
#* @get /staleness
#* @tag Health
function(
  res,
  req
) {
  auth_helper(
    res,
    req,
    get_staleness
  )
}

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
#* @put /create
#* @tag CRUD
function(
  res,
  req,
  table_name,
  input_list
) {
  auth_helper(
    res,
    req,
    put_table_row,
    table_name = table_name,
    input_list = as.list(input_list)
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
    get_table_data,
    table_name = table_name,
    limit = as.numeric(limit)
  )
}

### Update ----

#* Update row
#* @param table_name:chr The table name to modify the row in.
#* @param input_list:[chr] The list of values to add in the row.
#* @put /update
#* @tag CRUD
function(
  res,
  req,
  table_name,
  input_list
) {
  auth_helper(
    res,
    req,
    put_table_row,
    table_name = table_name,
    input_list = as.list(input_list),
    is_update = TRUE
  )
}

### Delete ----

#* Delete row
#* @param table_name:chr The table name to remove the row from.
#* @param row_key:numeric The index of the row to delete.
#* @delete /delete
#* @tag CRUD
function(
  res,
  req,
  table_name,
  row_key
) {
  auth_helper(
    res,
    req,
    delete_table_row,
    table_name = table_name,
    id_value = as.numeric(row_key)
  )
}

## Icarus ----

### Flights ----

#* Flights
#* @param flight_iata:chr The IATA code of the flight e.g. UK616
#* @param flight_date:chr The flight date in YYYY-MM-DD format
#* @get /icarus/flights
#* @tag Icarus
function(
  res,
  req,
  flight_iata,
  flight_date
) {
  auth_helper(
    res,
    req,
    get_flight_data,
    flight_iata = flight_iata,
    flight_date = as.Date(flight_date)
  )
}

## Livingston ----

### Trips ----

#* Trips
#* @get /livingston/trips
#* @param cached:bool Whether to use cached data or not
#* @tag Livingston
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      get_table_data,
      table_name = "livingston_trips"
    )
  }
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
    get_table_data,
    table_name = "livingston_details"
  )
  data[
    as.numeric(data$trip_id) == as.numeric(trip_id),
  ]
}

### Counts ----

#* Counts
#* @get /livingston/counts
#* @param cached:bool Whether to use cached data or not
#* @tag Livingston
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "livingston_trips"
    )

    current <- Sys.Date()

    data |>
      mutate(start = ymd(start_date),
             end = ymd(end_date)) |>
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
}

## Robin ----

### Stocks ----

#* Stocks
#* @get /robin/stocks
#* @param cached:bool Whether to use cached data or not
#* @tag Robin
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      load_sheet,
      sheet_name = "Stocks"
    )
  }
}

### Funds ----

#* Funds
#* @get /robin/funds
#* @param cached:bool Whether to use cached data or not
#* @tag Robin
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      load_sheet,
      sheet_name = "Funds"
    )
  }
}

## Midas ----

### MMTC ----

#* MMTC
#* @get /midas/mmtc
#* @param cached:bool Whether to use cached data or not
#* @tag Midas
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      get_mmtc_price
    )
  }
}

### Bullions ----

#* Bullions
#* @get /midas/bullions
#* @param cached:bool Whether to use cached data or not
#* @tag Midas
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      get_bullions_price
    )
  }
}

## Ebenezer ----

### Stocks ----

#* Stocks
#* @get /ebenezer/stocks
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_stocks"
    )
  }
}

### Mutual Funds ----

#* Mutual Funds
#* @get /ebenezer/funds
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    ticker_data <- auth_helper(
      res,
      req,
      load_sheet,
      sheet_name = "Funds"
    )

    funds_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_funds"
    )

    calculate_funds(
      funds_data,
      ticker_data
    )
  }
}

### Deposits ----

#* Deposits
#* @get /ebenezer/deposits
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_deposits"
    )
  }
}

### Savings ----

#* Savings
#* @get /ebenezer/savings
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_savings"
    )
  }
}

### MMTC ----

#* MMTC
#* @get /ebenezer/mmtc
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_mmtc"
    )
  }
}

### SGBs ----

#* SGBs
#* @get /ebenezer/sgbs
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_sgbs"
    )
  }
}

### Portfolio ----

#* Portfolio
#* @get /ebenezer/portfolio
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    ticker_data <- auth_helper(
      res,
      req,
      load_sheet,
      sheet_name = "Stocks"
    )

    stocks_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_stocks"
    )

    calculate_portfolio(
      stocks_data,
      ticker_data
    )
  }
}

### Stocks (Portfolio) Summary ----

#* Stocks (Portfolio) Summary
#* @get /ebenezer/summary/stocks
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    ticker_data <- auth_helper(
      res,
      req,
      load_sheet,
      sheet_name = "Stocks"
    )

    stocks_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_stocks"
    )

    calculate_portfolio(
      stocks_data,
      ticker_data
    )
  }
}

### Mutual Funds Summary ----

#* Mutual Funds Summary
#* @get /ebenezer/summary/funds
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    ticker_data <- auth_helper(
      res,
      req,
      load_sheet,
      sheet_name = "Funds"
    )
    funds_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_funds"
    )
    summarise_funds(
      funds_data,
      ticker_data
    )
  }
}

### Deposits Summary ----

#* Deposits Summary
#* @get /ebenezer/summary/deposits
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    deposits_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_deposits"
    )
    summarise_deposits(deposits_data)
  }
}

### Savings Summary ----

#* Savings Summary
#* @get /ebenezer/summary/savings
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    savings_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_savings"
    )
    summarise_savings(savings_data)
  }
}

### MMTC Summary ----

#* MMTC Summary
#* @get /ebenezer/summary/mmtc
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    mmtc_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_mmtc"
    )
    summarise_mmtc(
      mmtc_data,
      get_mmtc_price()
    )
  }
}

### SGBs Summary ----

#* SGBs Summary
#* @get /ebenezer/summary/sgbs
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    sgbs_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_sgbs"
    )
    summarise_sgbs(
      sgbs_data,
      get_bullions_price()
    )
  }
}

### Networth ----

#* Networth
#* @get /ebenezer/networth
#* @param cached:bool Whether to use cached data or not
#* @tag Ebenezer
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    sgbs_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_sgbs"
    )

    mmtc_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_mmtc"
    )

    savings_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_savings"
    )

    deposits_data <- auth_helper(
      res,
      req,
      get_table_data,
      table_name = "ebenezer_deposits"
    )

    funds_ticker_data <- auth_helper(
      res,
      req,
      load_sheet,
      sheet_name = "Funds"
    )

    mutual_funds_data <- auth_helper(
      res,
      req,
      get_table_data,
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
      get_table_data,
      table_name = "ebenezer_stocks"
    )

    etfs_data <- stocks_data[
      stocks_data$stock_symbol %in% c("HDFSIL", "HDFGOL"),
    ]

    stocks_data <-  stocks_data[
      !(stocks_data$stock_symbol %in% c("HDFSIL", "HDFGOL")),
    ]

    debt_funds_data <- mutual_funds_data[
      mutual_funds_data$name == "Dynamic Bond Fund",
    ]

    mutual_funds_data <- mutual_funds_data[
      mutual_funds_data$name != "Dynamic Bond Fund",
    ]

    networth <- list(
      "stocks" = c(
        summarise_portfolio(
          stocks_data,
          stocks_ticker_data
        )[
          c(
            "invested",
            "current"
          )
        ],
        "type" = "EQUITY"
      ),
      "etfs" = c(
        summarise_portfolio(
          etfs_data,
          stocks_ticker_data
        )[
          c(
            "invested",
            "current"
          )
        ],
        "type" = "BULLION"
      ),
      "debt_funds" = c(
        summarise_funds(
          debt_funds_data,
          funds_ticker_data
        ),
        "type" = "DEBT"
      ),
      "mutual_funds" = c(
        summarise_funds(
          mutual_funds_data,
          funds_ticker_data
        ),
        "type" = "EQUITY"
      ),
      "deposits" = c(
        summarise_deposits(
          deposits_data
        ),
        "type" = "DEBT"
      ),
      "savings" = c(
        summarise_savings(
          savings_data
        ),
        "type" = "DEBT"
      ),
      "mmtc" = c(
        summarise_mmtc(
          mmtc_data,
          get_mmtc_price()
        ),
        "type" = "BULLION"
      ),
      "sgbs" = c(
        summarise_sgbs(
          sgbs_data,
          get_bullions_price()
        ),
        "type" = "BULLION"
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
        sum(),
      "type" = "TOTAL"
    )

    c(list("networth" = totals), networth)
  }
}

## Chronos ----

### Events ----

#* Events
#* @get /chronos/events
#* @param cached:bool Whether to use cached data or not
#* @tag Chronos
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      get_table_data,
      table_name = "chronos_cache"
    )
  }
}

## Fogg ----

### Today's Tasks ----

#* Today's Tasks
#* @get /fogg/tasks
#* @param cached:bool Whether to use cached data or not
#* @tag Fogg
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      get_labelled_tasks_df
    )
  }
}

### Today's Task Analysis ----

#* Today's Task Analysis
#* @get /fogg/analysis
#* @param cached:bool Whether to use cached data or not
#* @tag Fogg
function(
  res,
  req,
  cached = FALSE
) {
  if (cached) {
    cache_helper(
      req_path = req$PATH_INFO
    )
  } else {
    auth_helper(
      res,
      req,
      get_tasks_analysis
    )
  }
}
