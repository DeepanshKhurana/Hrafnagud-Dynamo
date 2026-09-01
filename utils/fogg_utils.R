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
    map,
    map_chr
  ],
  tibble[
    as_tibble,
    tibble
  ],
)

#' Get Todoist tasks by filter (paginated)
#'
#' @param filter Todoist filter query
#' @param todoist_token Todoist API token
#' @return A list of tasks
get_tasks_by_filter <- function(
    filter,
    todoist_token = Sys.getenv("TODOIST_API_TOKEN")
) {
  request(
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
}

#' Get labelled tasks due today
#'
#' @return A list of tasks
get_labelled_tasks <- function() {
  get_tasks_by_filter(
    filter = "@Intensity:* & today"
  )$results
}

#' Get every Todoist project, paginated via the API's cursor
#'
#' @param todoist_token Todoist API token
#' @return A list of project objects
get_projects <- function(
    todoist_token = Sys.getenv("TODOIST_API_TOKEN")
) {
  projects <- list()
  cursor <- NULL

  repeat {
    req <- request(
      "https://api.todoist.com/api/v1/projects"
    ) |>
      req_auth_bearer_token(
        todoist_token
      )

    if (!is.null(cursor)) {
      req <- req |> req_url_query(cursor = cursor)
    }

    body <- req |> req_perform() |> resp_body_json()
    projects <- c(projects, body$results)
    cursor <- body$next_cursor

    if (is.null(cursor)) break
  }

  projects
}

#' Resolve Todoist Project names
#'
#' @return A named character vector, keyed by project id
get_project_names <- function() {
  projects <- get_projects()

  if (length(projects) == 0) {
    return(character())
  }

  names_by_id <- map_chr(projects, "name")
  names(names_by_id) <- map_chr(projects, "id")
  names_by_id
}

#' Get every Todoist section, paginated via the API's cursor
#'
#' @param todoist_token Todoist API token
#' @return A list of section objects
get_sections <- function(
    todoist_token = Sys.getenv("TODOIST_API_TOKEN")
) {
  sections <- list()
  cursor <- NULL

  repeat {
    req <- request(
      "https://api.todoist.com/api/v1/sections"
    ) |>
      req_auth_bearer_token(
        todoist_token
      )

    if (!is.null(cursor)) {
      req <- req |> req_url_query(cursor = cursor)
    }

    body <- req |> req_perform() |> resp_body_json()
    sections <- c(sections, body$results)
    cursor <- body$next_cursor

    if (is.null(cursor)) break
  }

  sections
}

#' Resolve Todoist section names
#'
#' @return A named character vector, keyed by section id
get_section_names <- function() {
  sections <- get_sections()

  if (length(sections) == 0) {
    return(character())
  }

  names_by_id <- map_chr(sections, "name")
  names(names_by_id) <- map_chr(sections, "id")
  names_by_id
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

  project_names <- get_project_names()
  section_names <- get_section_names()

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
      intensity = map_chr(
        labels,
        ~ unlist(.x)[grepl("^Intensity:", unlist(.x))]
      ),
      project_name = coalesce(
        unname(project_names[project_id]),
        "Unknown"
      ),
      section_name = coalesce(
        unname(section_names[section_id]),
        "No Section"
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
    list(recommendation_number = 5, recommendation_verbose = "Better")
  } else if (score < 0) {
    list(recommendation_number = 4, recommendation_verbose = "Good")
  } else if (score == 0) {
    list(recommendation_number = 3, recommendation_verbose = "Ideal")
  } else if (score <= factor) {
    list(recommendation_number = 2, recommendation_verbose = "Bad")
  } else {
    list(recommendation_number = 1, recommendation_verbose = "Worse")
  }
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
