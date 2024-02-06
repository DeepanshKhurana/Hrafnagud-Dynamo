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
#' including columns for Stock, Date, Quantity, Price, Brokerage, Charges,
#' StampDuty, and STT.
#' @param transaction_type A numeric value indicating the type of transaction
#' (buy or sell)
#' @return A data frame containing columns for Stock, Date, Quantity, and
#' Average Day Price
#' @export
calculate_avg_day_price <-
  function(stock_vector, transaction_type) {
    stock_vector <- stock_vector |>
      mutate(
        TotalChargeRate =
          (transaction_type * (Brokerage + Charges + StampDuty)) / Quantity,
        Price = Price + TotalChargeRate
      ) |>
      select(-c(Brokerage, Charges, StampDuty, STT, Name, TotalChargeRate)) |>
      mutate(Date = as.Date(Date)) |>
      select(Stock, Date, Quantity, Price) |>
      mutate(
        Quantity = Quantity,
        Cost = Price * Quantity
      ) |>
      group_by(Stock, Date) |>
      mutate(AvgDayPrice = (Cost + lag(Cost)) / (Quantity + lag(Quantity))) |>
      mutate(AvgDayPrice = case_when(
        is.na(AvgDayPrice) ~ Price,
        TRUE ~ AvgDayPrice
      )) |>
      summarise(
        Quantity = sum(Quantity),
        AvgDayPrice = AvgDayPrice
      ) |>
      group_by(Stock, Date) |>
      slice(n()) |>
      ungroup() |>
      uncount(Quantity) |>
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
get_subset <- function(stocks_processed, subset_type) {
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
summarise_quantity <- function(subset, column_name = "BuyQty") {
  if (column_name %in% c("BuyQty", "SellQty")) {
    subset |>
      group_by(Stock) |>
      summarise(!!(column_name) := n())
  } else {
    return("column_name can only be one of `BuyQty` or `SellQty`.")
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
calculate_quantity <- function(buys, sells) {
  buy_qty <- summarise_quantity(buys, "BuyQty")
  sell_qty <- summarise_quantity(sells, "SellQty")

  buy_sell <- left_join(buy_qty, sell_qty, by = "Stock")
  buy_sell[is.na(buy_sell$SellQty), "SellQty"] <- 0
  buy_sell$Remaining <- buy_sell$BuyQty - buy_sell$SellQty

  buy_sell |>
    select(Stock, Remaining)
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
get_initial_positions <- function(quantity, buys, sells) {
  positions <- quantity |>
    filter(Stock %in% sells$Stock) |>
    group_by(Stock) |>
    uncount(Remaining) |>
    mutate(
      Date = Sys.Date(),
      AvgSellPrice = 0
    )

  sells <- rbind(sells, positions) |> arrange(Stock, Date)
  buys <- buys |> arrange(Stock, Date)
  trxns <- buys |> filter(Stock %in% sells$Stock)

  cbind(trxns, sells) |>
    select(c(1, 2, AvgDayPrice, AvgSellPrice))
}

#' Calculates the realized gain/loss for each stock.
#' @param positions A dataframe containing information about stocks including
#' columns for Stock, Date, Quantity, Average Day Price, and Average Sell Price.
#' @param quantity A dataframe containing stock symbols and remaining quantity
#' of each stock.
#' @return A dataframe with columns for Stock, Quantity, and Realized gain/loss
#' @export
calculate_realized <- function(positions, quantity) {
  realized <- positions |>
    mutate(Realized = case_when(
      AvgSellPrice == 0 ~ 0,
      TRUE ~ AvgSellPrice - AvgDayPrice
    )) |>
    group_by(Stock) |>
    summarise(Realized = sum(Realized))

  realized <- left_join(quantity, realized, by = "Stock") |>
    mutate(Realized = case_when(
      is.na(Realized) ~ 0,
      TRUE ~ true_round(Realized, 2)
    ))

  colnames(realized) <- c("Stock", "Quantity", "Realized")

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
calculate_final_positions <-
  function(initial_positions, realized, buys) {
    buy_positions <- buys |>
      filter(!(Stock %in% initial_positions$Stock)) |>
      select(Stock, Date, AvgDayPrice) |>
      group_by(Stock) |>
      summarise(AvgPrice = mean(AvgDayPrice))

    current_positions <- initial_positions |>
      filter(AvgSellPrice == 0) |>
      select(Stock, Date, AvgDayPrice) |>
      group_by(Stock) |>
      summarise(AvgPrice = mean(AvgDayPrice))

    positions <- rbind(buy_positions, current_positions) |>
      arrange(Stock)

    positions <- merge(realized, positions) |>
      mutate(AvgPrice = true_round(AvgPrice, 2))

    positions
  }

