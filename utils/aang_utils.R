box::use(
  httr2[
    request,
    req_headers,
    req_perform,
    req_timeout,
    req_url_query,
    resp_body_json,
    resp_body_string
  ],
  xml2[
    read_xml,
    xml_attr,
    xml_find_all,
    xml_find_first
  ],
)

# The CPCB feed is a ~380KB national dump refreshed hourly, so it is held in
# process rather than re-fetched for every reader.
cache <- new.env(parent = emptyenv())

FEED_TTL <- 900 # nolint: object_name_linter.
FEED_URL <- "https://airquality.cpcb.gov.in/caaqms/rss_feed" # nolint: object_name_linter, line_length_linter.
METEO_URL <- "https://air-quality-api.open-meteo.com/v1/air-quality" # nolint: object_name_linter, line_length_linter.

# Past this the nearest monitor is measuring somebody else's air. India has
# large unmonitored stretches, so this is a real case, not a safety net.
MAX_STATION_KM <- 100 # nolint: object_name_linter.

# Generous on purpose, so it catches the neighbours too. The distance check is
# what stops a wrong guess from becoming somebody else's air.
INDIA_BOX <- c(6, 37.6, 68, 97.5) # nolint: object_name_linter.

#' India's NAQI bands
#' @param aqi The index value
#' @return The band name
india_band <- function(aqi) {
  if (aqi <= 50) "Good"
  else if (aqi <= 100) "Satisfactory"
  else if (aqi <= 200) "Moderate"
  else if (aqi <= 300) "Poor"
  else if (aqi <= 400) "Very Poor"
  else "Severe"
}

#' The US AQI bands, which are not India's
#' @param aqi The index value
#' @return The band name
us_band <- function(aqi) {
  if (aqi <= 50) "Good"
  else if (aqi <= 100) "Moderate"
  else if (aqi <= 150) "Unhealthy for Sensitive Groups"
  else if (aqi <= 200) "Unhealthy"
  else if (aqi <= 300) "Very Unhealthy"
  else "Hazardous"
}

#' Great-circle distance in kilometres, vectorised over the second point
#' @param lat,lon The origin, in degrees
#' @param lats,lons Vectors of destination coordinates, in degrees
#' @return Numeric vector of distances in kilometres
haversine_km <- function(lat, lon, lats, lons) {
  to_rad <- pi / 180
  h <- sin((lats - lat) * to_rad / 2)^2 +
    cos(lat * to_rad) * cos(lats * to_rad) * sin((lons - lon) * to_rad / 2)^2
  2 * 6371 * asin(pmin(1, sqrt(h)))
}

#' Fetch and parse every reporting CPCB station
#'
#' Offline stations publish an empty Air_Quality_Index and are dropped, because
#' a dead monitor must never win the nearest-station search.
#'
#' @return A data.frame of reporting stations, or NULL if the feed is unusable
#' @export
get_cpcb_stations <- function() {
  fresh <- !is.null(cache$stations) &&
    difftime(Sys.time(), cache$at, units = "secs") < FEED_TTL
  if (fresh) {
    return(cache$stations)
  }

  tryCatch({
    doc <- request(FEED_URL) |>
      req_headers(accept = "application/xml") |>
      req_timeout(20) |>
      req_perform() |>
      resp_body_string() |>
      read_xml()

    nodes <- xml_find_all(doc, "//Station")
    # xml_parent() on a nodeset returns each distinct parent once, so columns
    # would stop lining up the moment two stations share a city. XPath from
    # each node keeps one result per station.
    stations <- data.frame(
      name = xml_attr(nodes, "id"),
      city = xml_attr(xml_find_first(nodes, ".."), "id"),
      lat = suppressWarnings(as.numeric(xml_attr(nodes, "latitude"))),
      lon = suppressWarnings(as.numeric(xml_attr(nodes, "longitude"))),
      aqi = suppressWarnings(
        as.numeric(
          xml_attr(xml_find_first(nodes, "./Air_Quality_Index"), "Value")
        )
      ),
      stringsAsFactors = FALSE
    )

    reporting <- stations[
      !is.na(stations$aqi) & !is.na(stations$lat) & !is.na(stations$lon),
    ]
    if (nrow(reporting) == 0) {
      return(NULL)
    }

    cache$stations <- reporting
    cache$at <- Sys.time()
    reporting
  },
  error = function(e) {
    message("Failed to fetch CPCB feed: ", conditionMessage(e))
    NULL
  })
}

#' Air quality for a coordinate
#'
#' CPCB's nearest reporting station inside India, Open-Meteo everywhere else.
#' The two report on different scales and are not comparable, so the response
#' names the scale its number is on.
#'
#' @param lat Latitude, -90 to 90
#' @param lon Longitude, -180 to 180
#' @return A list describing the reading, or NULL if no source answers
#' @export
get_air_quality <- function(lat, lon) {
  in_india <- lat >= INDIA_BOX[1] && lat <= INDIA_BOX[2] &&
    lon >= INDIA_BOX[3] && lon <= INDIA_BOX[4]

  if (in_india) {
    stations <- get_cpcb_stations()
    if (!is.null(stations)) {
      km <- haversine_km(lat, lon, stations$lat, stations$lon)
      near <- which.min(km)
      if (km[near] <= MAX_STATION_KM) {
        station <- stations[near, ]
        aqi <- round(station$aqi)
        return(
          list(
            aqi = aqi,
            band = india_band(aqi),
            scale = "NAQI (IN)",
            source = "cpcb",
            place = station$city,
            station = station$name,
            distance_km = round(km[near], 1)
          )
        )
      }
    }
  }

  tryCatch({
    body <- request(METEO_URL) |>
      req_url_query(
        latitude = lat,
        longitude = lon,
        current = "us_aqi",
        timezone = "auto"
      ) |>
      req_timeout(10) |>
      req_perform() |>
      resp_body_json()

    aqi <- body$current$us_aqi
    if (is.null(aqi)) {
      return(NULL)
    }
    aqi <- round(as.numeric(aqi))
    list(
      aqi = aqi,
      band = us_band(aqi),
      scale = "AQI (US)",
      source = "open-meteo",
      place = NA,
      station = NA,
      distance_km = NA
    )
  },
  error = function(e) {
    message("Failed to fetch Open-Meteo air quality: ", conditionMessage(e))
    NULL
  })
}
