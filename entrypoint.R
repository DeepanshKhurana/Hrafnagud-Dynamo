box::use(
  plumber[
    pr,
    pr_run,
    pr_set_api_spec
  ],
  here[
    here
  ]
)

add_auth <- function(
  api,
  paths = NULL
) {

  api[["components"]] <- list(
    securitySchemes = list(
      ApiKeyAuth = list(
        type = "apiKey",
        `in` = "header",
        name = "X-API-KEY",
        description = "Add API Key here"
      )
    )
  )

  if (is.null(paths)) paths <- names(api$paths)
  for (path in paths) {
    nn <- names(api$paths[[path]])
    for (p in intersect(nn, c("get", "head", "post", "put", "delete"))) {
      api$paths[[path]][[p]] <- c(
        api$paths[[path]][[p]],
        list(security = list(list(ApiKeyAuth = vector())))
      )
    }
  }

  api

}

# pr(here("Hrafnagud-Dynamo", "plumber.R")) |>
pr("plumber.R") |>
  pr_set_api_spec(add_auth) |>
  pr_run(
    port = 8008,
    host = "0.0.0.0"
  )
