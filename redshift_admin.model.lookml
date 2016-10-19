- connection: mot-bi-looker-admin

- include: "*.view.lookml"       # include all the views
- include: "*.dashboard.lookml"  # include all the dashboards

# preliminaries #
- case_sensitive: false


# views to explore—i.e., "base views" #

- explore: data_loads

- explore: db_space
  label: 'DB Space'

- explore: etl_errors
  label: 'ETL Errors'

- explore: table_skew

- explore: view_definitions
  from: pg_views