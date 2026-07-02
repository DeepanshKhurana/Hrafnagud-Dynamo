box::use(
  httr2[
    request,
    req_headers,
    req_perform,
    resp_body_string
  ],
)

#' Get word of the day from Merriam-Webster's Word of the Day podcast feed
#' @param feed_url URL of the Merriam-Webster word of the day RSS feed
#' @return Word of the day
#' @export
get_word_of_the_day <- function(
  feed_url = "https://rss.art19.com/merriam-websters-word-of-the-day"
) {
  tryCatch({
    feed_head <- request(feed_url) |>
      req_headers(Range = "bytes=0-50000") |>
      req_perform() |>
      resp_body_string()

    first_item <- substr(
      feed_head,
      regexpr("<item>", feed_head, fixed = TRUE),
      nchar(feed_head)
    )

    word <- regmatches(
      first_item,
      regexpr("(?<=<title>)[^<]+(?=</title>)", first_item, perl = TRUE)
    )

    list(
      "word" = word,
      "link" = feed_url
    )
  },
  error = function(e) {
    message(
      "Failed to fetch word of the day: ",
      conditionMessage(e)
    )
  })
}
