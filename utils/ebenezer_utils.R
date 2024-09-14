box::use(
  dplyr[...],
  tidyr[
    uncount
  ]
)

# Utils ----

#' @description function to round a number correctly
#' @param number the number to round
#' @param digits the digits to round till
#' @export
true_round <- function(
  number,
  digits
) {
  number <- as.numeric(number)
  posneg <- sign(number)
  number <- abs(number) * 10 ^ digits
  number <- number + 0.5 + sqrt(.Machine$double.eps)
  number <- trunc(number)
  number <- number / 10 ^ digits
  number * posneg
}

#' @description function to format price/change values
#' @param value the value to format
#' @param round the digits to round till
#' @param format the number format "d" or "f"
#' @param big the separator for big values, default ","
#' @return character url for the sheet for the user
#' @export
format_price <- function(
  value,
  round = 2,
  format = "f",
  big = ","
) {
  formatC(
    as.numeric(
      as.character(true_round(value, round))
    ),
    digits = round,
    format = format,
    big.mark = big
  )
}

# Stocks ----

#' @description Calculates the average day price for a given stock vector.
#' @param stock_vector A data frame containing information about stocks
#' including columns for Stock, Date, Quantity, Price, brokerage, Charges,
#' StampDuty, and STT.
#' @param transaction_type A numeric value indicating the type of transaction
#' (buy or sell)
#' @return A data frame containing columns for Stock, Date, Quantity, and
#' Average Day Price
calculate_avg_day_price <- function(
  stock_vector,
  transaction_type
) {
  stock_vector <- stock_vector |>
    mutate(
      total_charge_rate =
        (transaction_type * (brokerage + transaction_charges + stamp_duty)) /
        quantity,
      transaction_price = transaction_price + total_charge_rate
    ) |>
    select(
      -c(
        brokerage,
        transaction_charges,
        stamp_duty,
        stt,
        company_name,
        total_charge_rate
      )
    ) |>
    mutate(transaction_date = as.Date(transaction_date)) |>
    select(stock_symbol, transaction_date, quantity, transaction_price) |>
    mutate(
      quantity = quantity,
      cost = transaction_price * quantity
    ) |>
    group_by(stock_symbol, transaction_date) |>
    mutate(avg_day_price = (cost + lag(cost)) / (quantity + lag(quantity))) |>
    mutate(avg_day_price = case_when(
      is.na(avg_day_price) ~ transaction_price,
      TRUE ~ avg_day_price
    )) |>
    summarise(
      quantity = sum(quantity),
      avg_day_price = avg_day_price
    ) |>
    group_by(stock_symbol, transaction_date) |>
    slice(n()) |>
    ungroup() |>
    uncount(quantity) |>
    data.frame()

  stock_vector
}

#' @description Returns a subset of a stocks dataframe based on the subset type.
#' @param stocks_processed A list with two dataframes, each with columns Stock,
#' Date, Quantity, and Average Day Price
#' @param subset_type A character value indicating the subset to be returned.
#' It can only be one of "Buys" or "Sells"
#' @return A subset of the stocks_processed dataframe based on the subset_type
#' specified
get_subset <- function(
  stocks_processed,
  subset_type
) {
  if (subset_type %in% c("Buys", "Sells")) {
    stocks_processed[[subset_type]]
  } else {
    return("subset_type can only be one of `Buys` or `Sells`.")
  }
}

#' Summarizes the quantity of a given column in a subset of stocks dataframe.
#' @param subset A subset generated from get_subset()
#' @param column_name A character value representing the name of the column
#' whose quantity is to be summarized. Default value is "BuyQty". Values can
#' only be "BuyQty" and "SellQty"
#' @return A dataframe with columns for Stock and the specified
#' column_name with its summarized quantity
summarise_quantity <- function(
  subset,
  column_name = "buy_qty"
) {
  if (column_name %in% c("buy_qty", "sell_qty")) {
    subset |>
      group_by(stock_symbol) |>
      summarise(!!(column_name) := n())
  } else {
    return("column_name can only be one of `buy_qty` or `sell_qty`.")
  }
}

#' Calculates the remaining quantity of a stock after considering both buy and
#' sell quantity.
#' @param buys A dataframe containing buy transactions for stocks with columns
#' for Stock, BuyQty and other details.
#' @param sells A dataframe containing sell transactions for stocks with columns
#' for Stock, SellQty and other details.
#' @return A dataframe with columns for Stock and the remaining quantity after
#' considering both buy and sell transactions
calculate_quantity <- function(
  buys,
  sells
) {
  buy_qty <- summarise_quantity(buys, "buy_qty")
  sell_qty <- summarise_quantity(sells, "sell_qty")

  buy_sell <- left_join(buy_qty, sell_qty, by = "stock_symbol")
  buy_sell[is.na(buy_sell$sell_qty), "sell_qty"] <- 0
  buy_sell$remaining <- buy_sell$buy_qty - buy_sell$sell_qty

  buy_sell |>
    select(stock_symbol, remaining)
}

#' Get initial positions of stocks.
#' @param quantity A dataframe containing stock symbols and remaining quantity
#' of each stock.
#' @param buys A dataframe containing buy transactions for stocks with columns
#' for Stock, Date, Quantity, Price, and other details.
#' @param sells A dataframe containing sell transactions for stocks with columns
#' for Stock, Date, Quantity, Price, and other details.
#' @return A dataframe with columns for Stock, Date, Average Day Price, and
#' Average Sell Price for initial positions of stocks.
get_initial_positions <- function(
  quantity,
  buys,
  sells
) {
  positions <- quantity |>
    filter(stock_symbol %in% sells$stock_symbol) |>
    group_by(stock_symbol) |>
    uncount(remaining) |>
    mutate(
      transaction_date = Sys.Date(),
      avg_sell_price = 0
    )

  sells <- rbind(sells, positions) |> arrange(stock_symbol, transaction_date)
  buys <- buys |> arrange(stock_symbol, transaction_date)
  trxns <- buys |> filter(stock_symbol %in% sells$stock_symbol)

  cbind(trxns, sells) |>
    select(c(1, 2, avg_day_price, avg_sell_price))
}

#' Calculates the realized gain/loss for each stock.
#' @param positions A dataframe containing information about stocks including
#' columns for Stock, Date, Quantity, Average Day Price, and Average Sell Price.
#' @param quantity A dataframe containing stock symbols and remaining quantity
#' of each stock.
#' @return A dataframe with columns for Stock, Quantity, and Realized gain/loss
calculate_realized <- function(
  positions,
  quantity
) {
  realized <- positions |>
    mutate(realized = case_when(
      avg_sell_price == 0 ~ 0,
      TRUE ~ avg_sell_price - avg_day_price
    )) |>
    group_by(stock_symbol) |>
    summarise(realized = sum(realized))

  realized <- left_join(quantity, realized, by = "stock_symbol") |>
    mutate(realized = case_when(
      is.na(realized) ~ 0,
      TRUE ~ true_round(realized, 2)
    ))

  colnames(realized) <- c("stock_symbol", "quantity", "realized")

  realized
}

#' Calculates the final positions of stocks.
#' @param initial_positions A dataframe containing information about stocks
#' including columns for Stock, Date, Quantity, Average Day Price, and Average
#' Sell Price.
#' @param realized A dataframe containing stock symbols and realized gain/loss
#' of each stock.
#' @param buys A dataframe containing buy transactions for stocks with columns
#' for Stock, Date, Quantity, Price, and other details.
#' @return A dataframe with columns for Stock, Quantity, Realized, and
#' Average Price for final positions of stocks.
calculate_final_positions <- function(
  initial_positions,
  realized,
  buys
) {
  buy_positions <- buys |>
    filter(!(stock_symbol %in% initial_positions$stock_symbol)) |>
    select(stock_symbol, transaction_date, avg_day_price) |>
    group_by(stock_symbol) |>
    summarise(avg_price = mean(avg_day_price))

  current_positions <- initial_positions |>
    filter(avg_sell_price == 0) |>
    select(stock_symbol, transaction_date, avg_day_price) |>
    group_by(stock_symbol) |>
    summarise(avg_price = mean(avg_day_price))

  positions <- rbind(buy_positions, current_positions) |>
    arrange(stock_symbol)

  positions <- merge(realized, positions) |>
    mutate(avg_price = true_round(avg_price, 2))

  positions
}

#' Process ticker data for calculation of portfolio
#' @param ticker_data the ticker data being fetched from the API
process_ticker_data <- function(
  ticker_data = NULL
) {
  ticker_data |> select(
    icici_code,
    nse_code,
    current_price,
    name,
    change_percent
  ) |>
    mutate_at(
      vars(
        c(
          "current_price",
          "change_percent"
        )
      ),
      as.numeric
    )
}

#' Process stocks data for calculation of portfolio
#' @param stocks_data the stocks data being fetched from the API
process_stocks_data <- function(
  stocks_data = NULL
) {
  stocks_data <- stocks_data |>
    select(
      -c(
        id,
        isin_code,
        remarks,
        exchange
      )
    )

  stocks_data <- stocks_data |>
    select(
      names(stocks_data)[
        order(names(stocks_data))
      ]
    ) |>
    mutate_at(
      vars(
        c(
          "brokerage",
          "quantity",
          "stamp_duty",
          "transaction_charges",
          "transaction_price"
        )
      ),
      as.numeric
    )
}

## Portfolio ----

#' Calculate the portfolio and return the final table
#' @param stocks_data the stocks data being fetched from the API
#' @param ticker_data the ticker data being fetched from the API
#' @export
calculate_portfolio <- function(
  stocks_data = NULL,
  ticker_data = NULL
) {

  stocks_data <- process_stocks_data(stocks_data)
  ticker_data <- process_ticker_data(ticker_data)

  buys <- calculate_avg_day_price(
    stocks_data |>
      filter(action == "Buy"),
    transaction_type = 1
  )

  sells <- calculate_avg_day_price(
    stocks_data |>
      filter(action == "Sell"),
    transaction_type = -1
  )

  colnames(sells)[3] <- "avg_sell_price"

  quantity <- calculate_quantity(buys, sells)

  initial_positions <- get_initial_positions(
    quantity,
    buys,
    sells
  )

  realized <- calculate_realized(
    initial_positions,
    quantity
  )

  positions <- calculate_final_positions(
    initial_positions,
    realized,
    buys
  )

  positions <- merge(
    ticker_data,
    positions,
    by.x = "icici_code",
    by.y = "stock_symbol"
  )

  names(positions)[1] <- "stock_symbol"

  positions |>
    group_by(stock_symbol) |>
    mutate(
      current_value = current_price * quantity,
      holding_value = avg_price * quantity,
      unrealized = current_value - holding_value
    ) |>
    ungroup() |>
    data.frame()
}

#' Calculate the portfolio and return the final table
#' @param stocks_data the stocks data being fetched from the API
#' @param ticker_data the ticker data being fetched from the API
#' @export
summarise_portfolio <- function(
  stocks_data,
  ticker_data
) {
  portfolio <- calculate_portfolio(
    stocks_data,
    ticker_data
  )

  list(
    "total_scrips" = portfolio |>
      summarise(scripts = n()) |>
      unname() |>
      unlist(),
    "total_realized" = portfolio |>
      summarise(realized = sum(realized)) |>
      unname() |>
      unlist(),
    "unrealized" = portfolio |>
      summarise(unrealized = sum(unrealized)) |>
      unname() |>
      unlist(),
    "invested" = portfolio |>
      summarise(invested = sum(holding_value)) |>
      unname() |>
      unlist(),
    "current" = portfolio |>
      mutate(current = unrealized + holding_value) |>
      summarise(current = sum(current)) |>
      unname() |>
      unlist()
  )
}

# Funds ----

#' Process the funds data and add some required columns
#' @param ticker_data the ticker data being fetched from the API
#' @param funds_data the stocks data being fetched from the API
#' @export
calculate_funds <- function(
  funds_data,
  ticker_data
) {
  funds_data |>
    rename(fund_name = name) |>
    left_join(ticker_data) |>
    mutate(
      current_value = true_round(
        as.numeric(unit_balance) * as.numeric(current_price),
        0
      ),
      profit = true_round(
        as.numeric(current_value) - as.numeric(total_invested),
        0
      ),
      change_percent = true_round(
        as.numeric(profit) * 100 / as.numeric(total_invested),
        2
      )
    ) |>
    rename(name = fund_name)
}

#' Process the funds data and add some required columns
#' @param ticker_data the ticker data being fetched from the API
#' @param funds_data the stocks data being fetched from the API
#' @export
summarise_funds <- function(
  funds_data,
  ticker_data
) {
  funds_data <- calculate_funds(
    funds_data,
    ticker_data
  )
  list(
    "invested" = sum(
      as.numeric(funds_data$total_invested)
    ),
    "current" = sum(
      as.numeric(funds_data$current_value)
    )
  )
}

# Deposits ----

#' Process the deposits data
#' @param deposits_data the deposits data being fetched from the API
process_deposits <- function(
  deposits_data
) {
  deposits_data$progress <- 1 - as.numeric(
    difftime(
      as.Date(
        deposits_data$maturity_date,
        format = "%d/%m/%y"
      ),
      as.Date(Sys.Date()),
      units = "days"
    )
  ) / (as.numeric(deposits_data$tenure_years) * 365.25)

  # Fix for negative values when the dates are the same - deposit creation

  deposits_data$progress <- ifelse(
    deposits_data$progress < 0,
    0.00,
    deposits_data$progress
  )

  deposits_data
}

#' Summarise the deposits data
#' @param deposits_data the deposits data being fetched from the API
#' @export
summarise_deposits <- function(
  deposits_data
) {
  deposits_data <- process_deposits(deposits_data)
  list(
    "invested" = sum(
      as.numeric(
        deposits_data$amount
      )
    ),
    "current" = sum(
      deposits_data$progress *
        (as.numeric(deposits_data$maturity) -
           as.numeric(deposits_data$amount)) +
        as.numeric(deposits_data$amount)
    )
  )
}

# Savings ----

#' Summarise the Savings data
#' @param savings_data the Savings data being fetched from the API
#' @export
summarise_savings <- function(
  savings_data
) {
  list(
    "invested" = sum(
      as.numeric(
        savings_data$account_balance
      )
    ),
    "current" = sum(
      as.numeric(
        savings_data$account_balance
      )
    )
  )
}

# MMTC ----

#' Process the MMTC data
#' @param mmtc_data the MMTC data being fetched from the API
#' @param mmtc_price the Midas price list for buy and sell for MMTC
process_mmtc <- function(
  mmtc_data,
  mmtc_price
) {
  mmtc_data$grams <- as.numeric(mmtc_data$milligrams) / 100
  mmtc_data$current_price <- as.numeric(
    mmtc_data$grams
  ) * as.numeric(
    mmtc_price$sell
  )
  mmtc_data$bought_price <- as.numeric(
    mmtc_data$total_invested
  ) / as.numeric(
    mmtc_data$grams
  )
  mmtc_data
}

#' Summarise the MMTC data
#' @param mmtc_data the MMTC data being fetched from the API
#' @param mmtc_price the Midas price list for buy and sell for MMTC
#' @export
summarise_mmtc <- function(
  mmtc_data,
  mmtc_price
) {
  mmtc_data <- process_mmtc(
    mmtc_data,
    mmtc_price
  )
  list(
    "invested" = as.numeric(mmtc_data$total_invested),
    "current" = as.numeric(mmtc_data$current_price)
  )
}

# SGBs ----

#' Process the SGBs data
#' @param sgbs_data the SGBs data being fetched from the API
process_sgbs <- function(
  sgbs_data
) {
  sgbs_data$progress <- 1 - as.numeric(
    difftime(
      as.Date(sgbs_data$maturity_date, format = "%d%m%y"),
      as.Date(Sys.Date()),
      units = "days"
    )
  ) / (8 * 365.25)

  sgbs_data
}

#' Summarise the SGBs data
#' @param sgbs_data the SGBs data being fetched from the API
#' @export
summarise_sgbs <- function(
  sgbs_data,
  bullions_price
) {
  sgbs_data <- process_sgbs(sgbs_data)
  list(
    "invested" = sum(
      as.numeric(
        sgbs_data$total
      )
    ),
    "current" = sum(
      as.numeric(
        as.numeric(
          sgbs_data$quantity
        ) *
          as.numeric(
            bullions_price
          )
      )
    )
  )
}
