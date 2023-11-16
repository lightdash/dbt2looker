connection: "databricks-sql-ugc-reporting"
include: "views/*"

explore: ugc__shopper_experience_store_aggregation {
  description: "<ENTER EXPOSURE DESCRIPTION>"

  join: ugc__shopper_experience_order_aggregation {
    type: left_outer
    relationship: one_to_many
    sql_on: ${ugc__shopper_experience_store_aggregation.date}  =  ${ugc__shopper_experience_order_aggregation.date} AND ${ugc__shopper_experience_store_aggregation.app_key}  =  ${ugc__shopper_experience_order_aggregation.app_key} ;;
  }

  join: ugc__shopper_experience_widget_engagement_aggregation {
    type: left_outer
    relationship: one_to_many
    sql_on: ${ref('ugc__shopper_experience_store_aggregation).date}  =  ${ugc__shopper_experience_widget_engagement_aggregation.date} AND ${ugc__shopper_experience_store_aggregation.app_key}  =  ${ref('ugc__shopper_experience_widget_engagement_aggregation).app_key} ;;
  }

  join: ugc__shopper_experience_widget_order_aggregation {
    type: left_outer
    relationship: one_to_many
    sql_on: ${'ugc__shopper_experience_store_aggregation').date}  =  ${ugc__shopper_experience_widget_order_aggregation.date} AND ${ugc__shopper_experience_store_aggregation.app_key}  =  ${ugc__shopper_experience_widget_order_aggregation.app_key} ;;
  }
}