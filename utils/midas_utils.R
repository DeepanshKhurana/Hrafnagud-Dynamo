box::use(
  rvest[
    read_html,
    html_text,
    html_node,
    html_nodes
  ],
  stringr[str_trim]
)

#' @export
#' @description Get MMTC Gold Prices
#' @param web_link a link for the MMTC website
#' @param type the type of data to get: buy, sell or both
#' @return A named list with gold prices
get_mmtc_price <- function(
  web_link = "https://www.mmtcpamp.com/gold-silver-rate-today"
) {
  tryCatch({
    html <- read_html(web_link)
    sell_price <- as.numeric(html_text(html_node(html, "#sellGoldPrice")))
    buy_price <- as.numeric(html_text(html_node(html, "#goldPrice")))
    list(
      "sell" = ifelse(is.na(sell_price), 0, sell_price),
      "buy" = ifelse(is.na(buy_price), 0, buy_price)
    )
  },
  error = function(e) {
    message("Failed to fetch prices from the provided link. \
            Setting prices to 0, 0.")
    list(
      sell = 0,
      buy = 0
    )
  })
}

#' @export
#' @description Get Bullions Gold Price
#' @param web_link a link for the Bullions India website
#' @return A named list with gold prices
get_bullions_price <- function(
  web_link = "https://bullions.co.in/"
) {
  tryCatch({
    html <- read_html(web_link)
    price <- html_text(html_nodes(html, ".data-box-half-value"))[1]
    price <- as.numeric(gsub(",", "", str_trim(price)))
    price / 10 # Bullion's rate is per 10gm
  },
  error = function(e) {
    message("Failed to fetch prices from the provided link. \
              Setting prices to 0, 0.")
    list(price = 0)
  })
}
