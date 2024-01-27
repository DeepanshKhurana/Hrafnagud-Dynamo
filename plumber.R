box::use(
  plumber[...]
)

box::use(
  utils/dynamodb_utils[
    get_processed_table_data,
    get_table_schema,
    put_table_row,
    delete_table_row
  ]
)

#* Schema
#* @param table_name:chr The table name to fetch the schema for.
#* @get /schema
function(
  table_name
) {
  get_table_schema(
    table_name
  )
}

#* New row
#* @param table_name:chr The table name to add the row to.
#* @param input_list:[chr] The list of values to add in the row.
#* @param show_old:logical Show the last values of the row?
#* @put /create
function(
  table_name,
  input_list,
  show_old
) {
  put_table_row(
    table_name,
    as.list(input_list),
    as.logical(show_old)
  )
}

#* @apiTitle DynamoDb
#* @apiDescription API for CRUD operations

#* Table
#* @param table_name:chr The table name to fetch data from.
#* @param limit:numeric The number of rows to limit at. Use 0 for all rows.
#* @get /read
function(
  table_name,
  limit
) {
  get_processed_table_data(
    table_name,
    as.numeric(limit)
  )
}

#* Update row
#* @param table_name:chr The table name to modify the row in.
#* @param input_list:[chr] The list of values to add in the row.
#* @param show_old:logical Show the last values of the row?
#* @put /update
function(
  table_name,
  input_list,
  show_old
) {
  put_table_row(
    table_name,
    as.list(input_list),
    as.logical(show_old),
    is_update = TRUE
  )
}

#* Delete row
#* @param table_name:chr The table name to remove the row from.
#* @param row_key:numeric The index of the row to delete.
#* @param show_old:logical Show the last values of the row?
#* @delete /delete
function(
  table_name,
  row_key,
  show_old
) {
  delete_table_row(
    table_name,
    as.numeric(row_key),
    show_old = as.logical(show_old)
  )
}
