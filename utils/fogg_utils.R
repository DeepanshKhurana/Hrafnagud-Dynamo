box::use(
  dplyr[
    arrange,
    bind_rows,
    group_by,
    mutate,
    n,
    rename_with,
    summarise,
    ungroup
  ],
  httr2[
    request,
    req_auth_bearer_token,
    req_perform,
    resp_body_json
  ],
  glue[
    glue
  ],
  purrr[
    map,
    map2,
    map_dbl,
    map_df,
    flatten
  ],
  tibble[
    as_tibble
  ],
)

#' Get Todoist projects
#'
#' Retrieves a list of projects from the Todoist API.
#'
#' @param todoist_token The API token for Todoist.
#' @param url The base URL for the Todoist API.
#' @return A list of Todoist projects.
get_todoist_projects <- function(
  todoist_token = Sys.getenv("TODOIST_API_TOKEN"),
  url = "https://api.todoist.com/rest/v2"
) {
  request(
    base_url = glue("{url}/projects")
  ) |>
    req_auth_bearer_token(
      todoist_token
    ) |>
    req_perform() |>
    resp_body_json()
}

#' Get Todoist tasks
#'
#' Retrieves a list of tasks from the Todoist API.
#'
#' @param todoist_token The API token for Todoist.
#' @param url The base URL for the Todoist API.
#' @return A list of Todoist tasks.
get_todoist_tasks <- function(
  todoist_token = Sys.getenv("TODOIST_API_TOKEN"),
  url = "https://api.todoist.com/rest/v2"
) {
  request(
    base_url = glue("{url}/tasks")
  ) |>
    req_auth_bearer_token(
      todoist_token
    ) |>
    req_perform() |>
    resp_body_json()
}

#' Get tasks with labels
#'
#' Filters Todoist tasks to keep only those with labels.
#'
#' @return A list of tasks that have labels.
get_labelled_tasks <- function(
  all = FALSE
) {
  if (all) {
    tasks <- Filter(
      function(task) {
        length(task$labels) > 0
      },
      get_todoist_tasks()
    )
  } else {
    tasks <- Filter(
      function(task) {
        length(task$labels) > 0 && as.Date(task$due$date) == Sys.Date()
      },
      get_todoist_tasks()
    )
  }
}

#' Flatten nested tasks
#'
#' Recursively flattens tasks and replaces NULL with NA.
#'
#' @param nested_list A list to be flattened.
#' @return A flattened list with unique names.
flatten_task <- function(nested_list) {
  if (is.list(nested_list)) {
    if (length(nested_list) == 0) return(list(NA))
    flattened <- unlist(
      map(
        nested_list,
        flatten_task
      ),
      recursive = FALSE
    )
    names(flattened) <- make.names(
      names(flattened),
      unique = TRUE
    )
    flattened
  } else {
    nested_list
  }
}

#' Process a task
#'
#' Converts NULL to NA and flattens nested lists.
#'
#' @param task A list representing a task.
#' @return A processed and flattened list.
process_task <- function(task) {
  task <- map(
    task,
    ~ if (is.null(.)) {
      NA
      } else {
        .
      }
  )
  flattened_task <- flatten_task(task)
  names(flattened_task) <- make.names(
    names(flattened_task),
    unique = TRUE
  )
  flattened_task
}

#' Get labelled tasks dataframe
#'
#' Retrieves tasks with labels and converts them to a tibble.
#'
#' @return A tibble of labelled tasks.
#' @export
get_labelled_tasks_df <- function() {
  get_labelled_tasks() |>
    map(process_task) |>
    bind_rows(.id = "id") |>
    data.frame() |>
    rename_with(
      ~ "intensity", starts_with("labels")
    )
}

#' Get task summary
#'
#' Summarizes tasks by labels
#'
#' @param tasks A data.frame of tasks fetched from get_labelled_tasks_df()
#' @return A data.frame of summary count by intensity
get_tasks_summary <- function(
  tasks = get_labelled_tasks_df()
) {
  tasks |>
    group_by(intensity) |>
    summarise(
      count = n()
    ) |>
    ungroup() |>
    arrange(intensity) |>
    mutate(
      intensity = gsub(
        "Intensity:",
        "",
        .data$intensity
      ) |>
        as.numeric(),
      count = count |>
        as.numeric()
    ) |>
    data.frame()
}

#' Get task score
#'
#' Take the summary and calculate a score according to baseline
#'
#' @param task_summary A data.frame summary fetched from get_tasks_summary()
#' @param baseline_score A numeric baseline for the score. Default is 5
#' @param ideal_distribution A named list of ideal distribution of tasks
#' @return A list with the following components:
#'   task_summary: The input task summary
#'   score: A numeric score based on the actual task distribution
#'   baseline: The provided baseline score
#'   difference: The difference between the baseline and the actual
#'   skew: The average difference between ideal and actual counts
#'   recommendation: A string recommendation based on skew:
#'     - "Worse" if skew > 0,
#'     - "Better" if skew < 0,
#'     - "Ideal" if skew == 0
#' @export
get_tasks_analysis <- function(
  task_summary = get_tasks_summary(),
  baseline_score = 5,
  ideal_distribution = list(
    "1" = 8,
    "2" = 7,
    "3" = 4,
    "4" = 3,
    "5" = 2
  )
) {
  actual_score <- sum(task_summary$intensity * task_summary$count * 0.1)
  total_tasks <- sum(task_summary$count)
  total_ideal <- sum(unlist(ideal_distribution))

  # Adjust ideal distribution
  adjusted_ideal_distribution <- map2(
    ideal_distribution,
    names(ideal_distribution),
    ~ .x * (total_tasks / total_ideal)
  )

  ideal_counts <- map_dbl(
    names(adjusted_ideal_distribution),
    ~ adjusted_ideal_distribution[[.x]]
  )

  actual_counts <- map_dbl(
    names(adjusted_ideal_distribution),
    ~ if (.x %in% task_summary$intensity) {
      task_summary$count[task_summary$intensity == as.numeric(.x)]
    } else {
      0
    }
  )

  skew <- mean(ideal_counts - actual_counts)

  recommendation <- if (skew > 0) {
    "Worse"
  } else if (skew < 0) {
    "Better"
  } else {
    "Ideal"
  }

  list(
    "task_summary" = task_summary,
    "score" = actual_score,
    "baseline" = baseline_score,
    "difference" = baseline_score - actual_score,
    "skew" = skew,
    "recommendation" = recommendation
  )
}
