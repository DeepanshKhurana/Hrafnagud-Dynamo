box::use(
  rvest[
    read_html,
    html_text,
    html_node
  ],
)

#' Get word of the day from Merriam-Webster
#' @param web_link URL of the Merriam-Webster word of the day page
#' @return Word of the day
#' @export
get_word_of_the_day <- function(
  web_link = "https://www.merriam-webster.com/word-of-the-day"
) {
  tryCatch({
    list(
      "word" = html_text(
        html_node(
          read_html(
            web_link
          ),
          ".word-header-txt"
        )
      ),
      "link" = web_link
    )
  },
  error = function(e) {
    message(
      "Failed to fetch word of the day."
    )
  })
}
