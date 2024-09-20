box::use(
  dplyr[
    arrange,
    filter,
    bind_rows,
    case_when,
    distinct,
    group_by,
    if_else,
    mutate,
    n,
    select,
    slice_min,
    summarise,
    ungroup
  ],
  purrr[
    map,
    pmap
  ],
  ical[
    ical_parse_df
  ],
  tidyr[
    separate_rows
  ],
  utils[
    download.file
  ],
)

box::use(
  utils/supabase_utils[
    get_table_data
  ]
)

#' Get abbreviated weekday name.
#'
#' @param date A date object.
#' @return Abbreviated weekday name in uppercase.
get_parsed_weekday <- function(
  date
) {
  weekdays(date, abbreviate = TRUE) |>
    substr(start = 1, stop = 2) |>
    toupper()
}

#' Calculate day difference between two weekdays.
#'
#' @param from A weekday abbreviation.
#' @param to A weekday abbreviation.
#' @param days A vector of weekday abbreviations.
#' @return The number of days between the two weekdays.
get_day_difference <- function(
  from,
  to,
  days = c("MO", "TU", "WE", "TH", "FR", "SA", "SU")
) {
  (match(from, days) - match(to, days)) %% 7
}

#' Process calendar data frame.
#'
#' @param calendar_df A data frame with calendar events.
#' @param to_ignore A vector of event types to ignore.
#' @return A processed data frame with status labels.
process_calendar_df <- function(
  calendar_df,
  to_ignore = c(
    "stay",
    "vacation",
    "holidays",
    "optional",
    "coffee together"
  )
) {
  to_ignore <- paste(to_ignore, collapse = "|")
  calendar_df |>
    mutate(
      start = as.Date(dtstart),
      is_ignored = grepl(to_ignore, tolower(summary)),
      status = case_when(
        is_ignored ~ NA_character_,
        start == Sys.Date() ~ "TODAY",
        start == (Sys.Date() + 1) ~ "TOMORROW",
        TRUE ~ NA_character_
      )
    ) |>
    filter(!is.na(status))
}

#' Process weekly repeating events in calendar data.
#'
#' @param calendar_df A data frame with calendar events.
#' @return A data frame of weekly repeating events.
process_weekly_repetition <- function(
  calendar_df
) {
  calendar_df |>
    filter(
      rrule_freq == "WEEKLY",
      class == "PRIVATE"
    ) |>
    separate_rows(rrule_byday, sep = ",") |>
    mutate(
      rrule_byday = trimws(rrule_byday),
      start = Sys.Date(),
      weekday = get_parsed_weekday(start),
      day_diff = get_day_difference(rrule_byday, weekday),
      status = case_when(
        day_diff == 0 ~ "TODAY",
        day_diff == 1 ~ "TOMORROW",
        TRUE ~ NA_character_
      )
    ) |>
    filter(!is.na(status))
}

#' Combine calendar data frames, including weekly repetitions.
#'
#' @param calendar_df A data frame with calendar events.
#' @return A combined and filtered data frame of calendar events.
combine_calendar_df <- function(
  calendar_df
) {
  processed_df <- process_calendar_df(calendar_df)

  if ("rrule_freq" %in% names(calendar_df)) {
    processed_df <- bind_rows(
      processed_df,
      process_weekly_repetition(calendar_df)
    )
  }

  processed_df |>
    filter(
      status != "PAST",
      start >= Sys.Date()
    ) |>
    select(
      summary,
      start,
      status
    ) |>
    distinct() |>
    arrange(status)
}

#' Download calendar files from URLs.
#'
#' @param calendars A list of calendar objects with URLs and priorities.
#' @return A list of calendar files with labels and priorities.
download_calendars <- function(
  calendars = NULL
) {
  if (!dir.exists("calendars")) dir.create("calendars")
  pmap(
    list(calendars$url, calendars$name, calendars$priority),
    function(url, name, priority) {
      filename <- tempfile(fileext = ".ics", tmpdir = "./calendars")
      download.file(url, destfile = filename)
      list(
        filename = filename,
        label = toupper(name),
        priority = priority
      )
    }
  )
}

#' Combine and process calendar data.
#'
#' @param calendars A list of calendar objects with URLs and priorities.
#' @return A combined data frame of calendar events.
#' @export
get_combined_calendars <- function(
  calendars = get_table_data("chronos_calendars")
) {
  calendars <- download_calendars(calendars)
  calendars <- map(
    .x = calendars,
    .f = function(calendar) {
      ical_parse_df(calendar$filename) |>
        combine_calendar_df() |>
        mutate(
          label = calendar$label,
          priority = calendar$priority
        )
    }
  ) |>
    bind_rows() |>
    arrange(status) |>
    group_by(summary) |>
    mutate(
      summary = gsub("Deepansh's ", "", summary)
    ) |>
    slice_min(
      order_by = priority,
      with_ties = TRUE
    ) |>
    ungroup() |>
    distinct() |>
    arrange(status) |>
    select(-c(priority, start)) |>
    data.frame()
  list.files("./calendars", full.names = TRUE) |>
    file.remove()
  calendars
}
