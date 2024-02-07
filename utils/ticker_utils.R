box::use(
  googlesheets4[
    read_sheet,
    gs4_auth
  ]
)

#' @export
#' @description a function to return the data from a sheet
#' @param sheet_name the name of the sheet to return
#' @param cache the cache directory; default = ".secrets"
#' @param path string path to json service_account
#' @return named list with dataframe and timestamp
#'
load_sheet <- function(
  sheet_name = NULL,
  cache = ".secrets",
  path = "service_account.json"
) {

  gs4_auth(path = file.path(cache, path))

  if (nchar(Sys.getenv("TICKER_SHEET")) > 1) {
    clean_colnames(
      data.frame(
        read_sheet(
          Sys.getenv("TICKER_SHEET"),
          sheet = sheet_name
        )
      )
    )
  } else {
    stop("TICKER_SHEET not set in environment")
  }
}

#' @description Clean the "ss." characters that are added by
#' googlesheets4r package
#'
#' @param dataset the dataframe object to fix
#'
clean_colnames <- function(dataframe) {
  names(dataframe) <-
    gsub(
      pattern = "ss.",
      replacement = "",
      x = names(dataframe)
    )
  dataframe
}
