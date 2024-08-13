box::use(
  config[
    get
  ],
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
    map2
  ],
  ical[
    ical_parse_df
  ],
  tidyr[
    separate_rows
  ],
)

download_calendars <- function(
  calendars = get("calendars")
) {
  labels <- names(calendars)
  filenames <- map2(
    .x = calendars,
    .y = labels,
    .f = function(calendar, label) {
      filename <- tempfile(fileext = ".ics")
      download.file(
        calendar$url,
        destfile = filename
      )
      list(
        "filename" = filename,
        "label" = toupper(label),
        "priority" = calendar$priority
      )
    }
  )
  filenames
}

#' @export
get_combined_calendars <- function(
  calendars = get("calendars")
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
    arrange(
      status
    )

  calendars |>
    group_by(summary) |>
    slice_min(
      order_by = priority,
      with_ties = TRUE
    ) |>
    ungroup() |>
    distinct() |>
    arrange(status) |>
    select(
      -c(
        priority,
        start
      )
    ) |>
    data.frame()
}

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
    arrange(
      status
    )
}

get_parsed_weekday <- function(
  date
) {
  weekdays(
    date,
    abbreviate = TRUE
  ) |>
    substr(
      start = 1,
      stop = 2
    ) |>
    toupper()
}

process_weekly_repetition <- function(
  calendar_df
) {
  calendar_df |>
    filter(
      rrule_freq == "WEEKLY",
      class == "PRIVATE"
    ) |>
    separate_rows(
      rrule_byday,
      sep = ","
    ) |>
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

get_day_difference <- function(
  from,
  to,
  days = c(
    "MO",
    "TU",
    "WE",
    "TH",
    "FR",
    "SA",
    "SU"
  )
) {
  from_ix <- match(from, days)
  to_ix <- match(to, days)
  (match(from, days) - match(to, days)) %% 7
}

calendars <- get_combined_calendars()
