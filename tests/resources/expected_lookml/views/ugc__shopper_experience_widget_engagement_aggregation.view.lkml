view: ugc__shopper_experience_widget_engagement_aggregation {
  sql_table_name: dev_dkruh1.ugc__shopper_experience_widget_engagement_aggregation ;;

  dimension_group: date {
    type: time
    sql: ${TABLE}.date ;;
    description: "Date"
    datatype: datetime
    timeframes: [
      raw,
      time,
      hour,
      date,
      week,
      month,
      quarter,
      year,
    ]
  }

  dimension: app_key {
    type: string
    sql: ${TABLE}.app_key ;;
    description: "App key"
  }

  dimension: widget {
    type: string
    sql: ${TABLE}.widget ;;
    description: "Widget name"
  }

  dimension: total_interacted_users {
    type: number
    sql: ${TABLE}.total_interacted_users ;;
    description: "Amount of interacted users per widget"
  }

  dimension: total_non_interacted_users {
    type: number
    sql: ${TABLE}.total_non_interacted_users ;;
    description: "Amount of non interacted users per widget"
  }

  measure: count {
    type: count
    description: "Default count measure"
  }
}