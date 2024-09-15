box::use(
  dplyr[
    select
  ],
  httr2[
    request,
    req_auth_bearer_token,
    req_perform,
    resp_body_json,
    req_url_query
  ],
  glue[
    glue
  ],
  purrr[
    keep,
    pluck
  ],
)

#' Retrieve flight data from AviationStack API.
#'
#' @param flight_iata The IATA code of the flight.
#' @param flight_date The date of the flight.
#' @param access_key API access key for AviationStack.
#' @param url Base URL for the AviationStack API.
#' @return A list of flight data matching the specified flight and date.
#' @export
get_flight_data <- function(
  flight_iata = NULL,
  flight_date = as.Date(Sys.Date()),
  access_key = Sys.getenv("AVIATIONSTACK_API_KEY"),
  url = "https://api.aviationstack.com"
) {
  flight_data <- request(
    base_url = glue("{url}/v1/flights")
  ) |>
    req_url_query(
      access_key = access_key,
      flight_iata = flight_iata
    ) |>
    req_perform() |>
    resp_body_json()

  keep(
    flight_data$data,
    ~ .x$flight_date == flight_date
  ) |>
    pluck(1) |>
    unlist() |>
    as.list()
}
