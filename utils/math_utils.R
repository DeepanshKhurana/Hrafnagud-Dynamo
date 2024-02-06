#' @export
#'
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

#' @export
#'
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
