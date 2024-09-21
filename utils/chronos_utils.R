box::use(
  dplyr[
    arrange,
    filter,
    bind_rows,
    case_when,
    distinct,
    group_by,
    mutate,
    select,
    slice_min,
    ungroup
  ],
  ical[
    ical_parse_df
  ],
  tidyr[
    separate_rows
  ],
  glue[
    glue
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

#' Read a calendar and parse it
#'
#' @param url The url for the calendar
#' @param name The name of the calendar
#' @param priority The priorty of the calendar
#' @return A data frame of events
read_calendar <- function(
  url,
  name,
  priority
) {
  print(glue("Processing: {name} | Priority: {priority} \n{url}"))
  ical_parse_df(
    text = readLines(
      url,
      warn = FALSE
    )
  ) |>
    combine_calendar_df() |>
    mutate(
      label = toupper(name),
      priority = priority
    )
}

#' Get Combined Calendars
#'
#' Downloads and combines calendar data from specified URLs.
#'
#' @param calendars A dataframe containing calendars.
#' @return A combined dataframe of parsed calendar events.
#' @export
get_combined_calendars <- function(
  calendars = get_table_data("chronos_calendars")
) {
  process_combined_calendars(
    lapply(
      seq_len(
        nrow(
          calendars
        )
      ),
      function(index) {
        read_calendar(
          url = calendars$url[index],
          name = calendars$name[index],
          priority = calendars$priority[index]
        )
      }
    )
  )
}

#' Process Combined Calendars
#'
#' Combines and processes a list of processed calendar dataframes.
#'
#' @param processed_calendars A list of dataframes from parsed calendar events.
#' @return A single dataframe containing distinct calendar events.
process_combined_calendars <- function(
  processed_calendars
) {
  do.call(
    rbind,
    processed_calendars
  ) |>
    arrange(status) |>
    group_by(summary) |>
    slice_min(
      order_by = priority,
      with_ties = TRUE
    ) |>
    ungroup() |>
    distinct() |>
    select(
      -c(
        priority,
        start
      )
    )
}
