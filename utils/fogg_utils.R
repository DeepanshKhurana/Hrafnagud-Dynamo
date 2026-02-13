box::use(
  dplyr[
    arrange,
    bind_rows,
    coalesce,
    filter,
    group_by,
    left_join,
    mutate,
    n,
    pull,
    rename,
    select,
    summarise,
    ungroup
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
    map
  ],
  tibble[
    as_tibble,
    tibble
  ],
)

#' Get Todoist projects
#'
#' @param todoist_token Todoist API token
#' @return A list of projects
get_todoist_projects <- function(
    todoist_token = Sys.getenv("TODOIST_API_TOKEN")
) {
  request(
    "https://api.todoist.com/rest/v2/projects"
  ) |>
    req_auth_bearer_token(
      todoist_token
    ) |>
    req_perform() |>
    resp_body_json()
}

#' Get Todoist tasks by filter (paginated)
#'
#' @param filter Todoist filter query
#' @param todoist_token Todoist API token
#' @return A list of tasks
get_tasks_by_filter <- function(
    filter,
    todoist_token = Sys.getenv("TODOIST_API_TOKEN")
) {
  res <- request(
    "https://api.todoist.com/api/v1/tasks/filter"
  ) |>
    req_auth_bearer_token(
      todoist_token
    ) |>
    req_url_query(
      query = filter
    ) |>
    req_perform() |>
    resp_body_json()

  res
}

#' Get labelled tasks due today
#'
#' @return A list of tasks
get_labelled_tasks <- function() {
  get_tasks_by_filter(
    filter = "@Intensity:* & today"
  )$results
}

#' Get labelled tasks as a tibble
#'
#' @return A tibble of labelled tasks
#' @export
get_labelled_tasks_df <- function() {
  tasks <- get_labelled_tasks()

  tasks <- tasks |>
    keep(
      ~ is.list(.) && !is.null(.$id)
    )

  if (length(tasks) == 0) {
    return(tibble())
  }

  tasks |>
    map(
      ~ map(.x, normalize_field) |>
        as_tibble()
    ) |>
    bind_rows() |>
    filter(
      responsible_uid == "24939805" | is.na(responsible_uid)
    ) |>
    mutate(
      intensity = purrr::map_chr(
        labels,
        ~ unlist(.x)[grepl("^Intensity:", unlist(.x))]
      )
    )
}

#' Get task summary
#'
#' @param tasks Output of get_labelled_tasks_df()
#' @return A data.frame summary
get_tasks_summary <- function(
  tasks = get_labelled_tasks_df()
) {
  if (nrow(tasks) == 0) {
    return(data.frame())
  }

  tasks |>
    group_by(
      intensity
    ) |>
    summarise(
      count = n(),
      .groups = "drop"
    ) |>
    arrange(
      intensity
    ) |>
    mutate(
      intensity = as.numeric(
        gsub(
          "Intensity:",
          "",
          intensity
        )
      ),
      count = as.numeric(
        count
      )
    ) |>
    data.frame()
}

#' Analyse task distribution
#'
#' @param task_summary Summary from get_tasks_summary()
#' @param ideal_tasks_distribution Named list of ideal counts
#' @return Analysis result
#' @export
get_tasks_analysis <- function(
  task_summary = get_tasks_summary(),
  ideal_tasks_distribution = list(
    "1" = 6,
    "2" = 5,
    "3" = 4,
    "4" = 3,
    "5" = 2
  )
) {
  if (nrow(task_summary) == 0) {
    return(list())
  }

  task_summary <- data.frame(
    intensity = 1:5,
    count = 0
  ) |>
    left_join(
      task_summary,
      by = "intensity"
    ) |>
    mutate(
      count = coalesce(
        count.y,
        count.x
      )
    ) |>
    select(
      intensity,
      count
    )

  total_tasks <- sum(
    task_summary$count
  )

  score <- cbind(
    task_summary,
    data.frame(
      ideal_count = unlist(
        ideal_tasks_distribution
      )
    )
  ) |>
    mutate(
      score = intensity * count * 0.1,
      ideal_score = intensity * ideal_count * 0.1
    ) |>
    summarise(
      score = sum(
        score - ideal_score
      )
    ) |>
    pull()

  c(
    list(
      summary = task_summary,
      total_tasks = total_tasks,
      mean_intensity =
        sum(task_summary$intensity * task_summary$count) /
        total_tasks,
      score = score
    ),
    get_recommendation(
      score
    )
  )
}

#' Get recommendation based on score
#'
#' @param score Numeric score
#' @param factor Threshold
#' @return Recommendation list
get_recommendation <- function(
  score,
  factor = 2.5
) {
  if (score <= -factor) {
    out <- list(5, "Better")
  } else if (score < 0) {
    out <- list(4, "Good")
  } else if (score == 0) {
    out <- list(3, "Ideal")
  } else if (score <= factor) {
    out <- list(2, "Bad")
  } else {
    out <- list(1, "Worse")
  }

  names(out) <- c(
    "recommendation_number",
    "recommendation_verbose"
  )

  out
}

#' Normalize a field to ensure it is either a single value or a list
#' @param x The input field to normalize
#' @return The normalized field, either as a single value or a list
normalize_field <- function(x) {
  if (is.null(x)) {
    NA
  } else if (is.atomic(x) && length(x) == 1) {
    x
  } else {
    list(x)
  }
}
