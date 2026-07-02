box::use(
  rvest[
    read_html,
    html_text,
    html_node,
    html_nodes
  ],
  stringr[str_trim],
  httr2[
    request,
    req_perform,
    req_user_agent,
    req_headers,
    resp_body_json
  ]
)

browser_user_agent <- paste(
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
  "AppleWebKit/537.36 (KHTML, like Gecko)",
  "Chrome/126.0.0.0 Safari/537.36"
)

browser_headers <- list(
  "sec-ch-ua" =
    '"Chromium";v="126", "Google Chrome";v="126", "Not-A.Brand";v="99"',
  "sec-ch-ua-mobile" = "?0",
  "sec-ch-ua-platform" = '"macOS"',
  "sec-fetch-dest" = "empty",
  "sec-fetch-mode" = "cors",
  "sec-fetch-site" = "same-site",
  "Accept" = "application/json, text/plain, */*",
  "Accept-Language" = "en-US,en;q=0.9",
  "Origin" = "https://www.mmtcpamp.com",
  "Referer" = "https://www.mmtcpamp.com/"
)

#' @export
#' @description Get MMTC Gold Prices
#' @param web_link a link for the MMTC website
#' @param type the type of data to get: buy, sell or both
#' @return A named list with gold prices
get_mmtc_price <- function(
  crawl = FALSE
) {
  if (crawl) {
    web_link <- "https://www.mmtcpamp.com/gold-silver-rate-today"
    prices <- tryCatch({
      html <- read_html(web_link)
      list(
        sell = as.numeric(html_text(html_node(html, "#sellGoldPrice"))),
        buy = as.numeric(html_text(html_node(html, "#goldPrice")))
      )
    },
    error = function(e) {
      message(
        "Failed to fetch prices from the provided link.
        \ Setting prices to 0, 0."
      )
      list(
        sell = 0,
        buy = 0
      )
    })
    sell_price <- prices$sell
    buy_price <- prices$buy
  } else {

    web_link <- "https://cem.mmtcpamp.com/cms/getTodaysPagePrice"

    prices <- tryCatch({
      response <- request(web_link) |>
        req_user_agent(browser_user_agent) |>
        req_headers(!!!browser_headers) |>
        req_perform() |>
        resp_body_json()

      buy <- response$data$attributes$TodaysPrice[[1]]
      sell <- response$data$attributes$TodaysPrice[[2]]

      list(
        sell = sell$live_prices$data[[1]]$attributes$price,
        buy = buy$live_prices$data[[1]]$attributes$price
      )
    },
    error = function(e) {
      message(
        "Failed to fetch prices from the provided link.
        \ Setting prices to 0, 0."
      )
      list(
        sell = 0,
        buy = 0
      )
    })
    sell_price <- prices$sell
    buy_price <- prices$buy
  }
  list(
    "sell" = ifelse(is.na(sell_price), 0, sell_price),
    "buy" = ifelse(is.na(buy_price), 0, buy_price)
  )
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
    0
  })
}
