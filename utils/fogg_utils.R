box::use(
  httr2[
    request,
    req_auth_bearer_token,
    req_perform,
    resp_body_json
  ],
  glue[
    glue
  ]
)

#' Get Todoist projects
#'
#' Retrieves a list of projects from the Todoist API.
#'
#' @param todoist_token The API token for Todoist. Defaults to the "TODOIST_API_TOKEN" environment variable.
#' @param url The base URL for the Todoist API. Defaults to "https://api.todoist.com/rest/v2".
#' @return A list of Todoist projects.
#' @export
get_todoist_projects <- function(
  todoist_token = Sys.getenv("TODOIST_API_TOKEN"),
  url = "https://api.todoist.com/rest/v2"
) {
  httr2::request(
    base_url = glue("{url}/projects")
  ) |>
    httr2::req_auth_bearer_token(
      todoist_token
    ) |>
    httr2::req_perform() |>
    httr2::resp_body_json()
}

#' Get Todoist tasks
#'
#' Retrieves a list of tasks from the Todoist API.
#'
#' @param todoist_token The API token for Todoist. Defaults to the "TODOIST_API_TOKEN" environment variable.
#' @param url The base URL for the Todoist API. Defaults to "https://api.todoist.com/rest/v2".
#' @return A list of Todoist tasks.
#' @export
get_todoist_tasks <- function(
  todoist_token = Sys.getenv("TODOIST_API_TOKEN"),
  url = "https://api.todoist.com/rest/v2"
) {
  httr2::request(
    base_url = glue::glue("{url}/tasks")
  ) |>
    httr2::req_auth_bearer_token(
      todoist_token
    ) |>
    httr2::req_perform() |>
    httr2::resp_body_json()
}

#' Get tasks with labels
#'
#' Filters Todoist tasks to keep only those with labels.
#'
#' @return A list of tasks that have labels.
#' @export
get_labelled_tasks <- function() {
  Filter(
    function(task) {
      length(task$labels) > 0
    },
    get_todoist_tasks()
  )
}
