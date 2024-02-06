box::use(
  dplyr[...]
)

## Utils ----

#' @description function to round a number correctly
#' @param number the number to round
#' @param digits the digits to round till
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
#'
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
#' @export
calculate_avg_day_price <- function(
  stock_vector,
  transaction_type
) {
  stock_vector <- stock_vector |>
    mutate(
      total_charge_rate =
        (transaction_type * (brokerage + charges + stamp_duty)) / quantity,
      price = price + total_charge_rate
    ) |>
    select(-c(brokerage, charges, stamp_duty, stt, name, total_charge_rate)) |>
    mutate(date = as.Date(date)) |>
    select(stock, date, quantity, price) |>
    mutate(
      quantity = quantity,
      cost = price * quantity
    ) |>
    group_by(stock, date) |>
    mutate(avg_day_price = (cost + lag(cost)) / (quantity + lag(quantity))) |>
    mutate(avg_day_price = case_when(
      is.na(avg_day_price) ~ price,
      TRUE ~ avg_day_price
    )) |>
    summarise(
      quantity = sum(quantity),
      avg_day_price = avg_day_price
    ) |>
    group_by(stock, date) |>
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
#' @export
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
#' @export
summarise_quantity <- function(
  subset,
  column_name = "buy_qty"
) {
  if (column_name %in% c("buy_qty", "sell_qty")) {
    subset |>
      group_by(stock) |>
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
#' @export
calculate_quantity <- function(
  buys,
  sells
) {
  buy_qty <- summarise_quantity(buys, "buy_qty")
  sell_qty <- summarise_quantity(sells, "sell_qty")

  buy_sell <- left_join(buy_qty, sell_qty, by = "Stock")
  buy_sell[is.na(buy_sell$sell_qty), "sell_qty"] <- 0
  buy_sell$remaining <- buy_sell$buy_qty - buy_sell$sell_qty

  buy_sell |>
    select(stock, remaining)
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
#' @export
get_initial_positions <- function(
  quantity,
  buys,
  sells
) {
  positions <- quantity |>
    filter(stock %in% sells$stock) |>
    group_by(stock) |>
    uncount(remaining) |>
    mutate(
      date = Sys.Date(),
      avg_sell_price = 0
    )

  sells <- rbind(sells, positions) |> arrange(stock, date)
  buys <- buys |> arrange(stock, date)
  trxns <- buys |> filter(stock %in% sells$stock)

  cbind(trxns, sells) |>
    select(c(1, 2, avg_day_price, avg_sell_price))
}

#' Calculates the realized gain/loss for each stock.
#' @param positions A dataframe containing information about stocks including
#' columns for Stock, Date, Quantity, Average Day Price, and Average Sell Price.
#' @param quantity A dataframe containing stock symbols and remaining quantity
#' of each stock.
#' @return A dataframe with columns for Stock, Quantity, and Realized gain/loss
#' @export
calculate_realized <- function(
  positions,
  quantity
) {
  realized <- positions |>
    mutate(realized = case_when(
      avg_sell_price == 0 ~ 0,
      TRUE ~ avg_sell_price - avg_day_price
    )) |>
    group_by(stock) |>
    summarise(realized = sum(realized))

  realized <- left_join(quantity, realized, by = "stock") |>
    mutate(realized = case_when(
      is.na(realized) ~ 0,
      TRUE ~ true_round(realized, 2)
    ))

  colnames(realized) <- c("stock", "quantity", "realized")

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
#' @export
calculate_final_positions <- function(
  initial_positions,
  realized,
  buys
) {
  buy_positions <- buys |>
    filter(!(stock %in% initial_positions$stock)) |>
    select(stock, date, avg_day_price) |>
    group_by(stock) |>
    summarise(avg_price = mean(avg_day_price))

  current_positions <- initial_positions |>
    filter(avg_sell_price == 0) |>
    select(stock, date, avg_day_price) |>
    group_by(stock) |>
    summarise(avg_price = mean(avg_day_price))

  positions <- rbind(buy_positions, current_positions) |>
    arrange(stock)

  positions <- merge(realized, positions) |>
    mutate(avg_price = true_round(avg_price, 2))

  positions
}
