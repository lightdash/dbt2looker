view: ugc__shopper_experience_store_aggregation {
  sql_table_name: dev_dkruh1.ugc__shopper_experience_store_aggregation ;;

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

  dimension_group: duration_dimension_group {
    type: duration
    sql_start: {% date_start date_range_filter %} ;;
    sql_end: {% date_end date_range_filter %} ;;
    description: ""
    intervals: [day]
  }

  dimension: app_key {
    type: string
    sql: ${TABLE}.app_key ;;
    description: "App key"
  }

  dimension: total_users {
    type: number
    sql: ${TABLE}.total_users ;;
    description: "Amount of all unique users"
  }

  dimension: interacted_users {
    type: number
    sql: ${TABLE}.interacted_users ;;
    description: "Amount of unique users that interacted with at least one widget"
  }

  dimension: non_interacted_users {
    type: number
    sql: ${TABLE}.non_interacted_users ;;
    description: "Amount of unique users that did not interact with any widget"
  }

  dimension: interacted_users_with_orders {
    type: number
    sql: ${TABLE}.interacted_users_with_orders ;;
    description: "Amount of unique users that interacted with at least one widget and placed at least one order"
  }

  dimension: interacted_users_without_orders {
    type: number
    sql: ${TABLE}.interacted_users_without_orders ;;
    description: "Amount of unique users that interacted with at least one widget and did not place any order"
  }

  dimension: non_interacted_users_with_orders {
    type: number
    sql: ${TABLE}.non_interacted_users_with_orders ;;
    description: "Amount of unique users that did not interact with any widget and placed at least one order"
  }

  dimension: non_interacted_users_without_orders {
    type: number
    sql: ${TABLE}.non_interacted_users_without_orders ;;
    description: "Amount of unique users that did not interact with any widget and did not place any order"
  }

  dimension: custom_dimension {
    description: ""
    type: number
    sql: case when ${currency} is not null then ${orders_count} else 0 end ;;
  }

  measure: engagement {
    description: ""
    type: number
    sql: (SUM(${ugc__shopper_experience_store_aggregation.interacted_users} )/SUM( ${ugc__shopper_experience_store_aggregation.total_users})) ;;
  }

  measure: count {
    type: count
    description: "Default count measure"
  }

  parameter: selected_store_id {
    type: string
    description: ""
    label: "selected_store_id"
  }

  filter: date_range_filter {
    description: ""
    type: date
  }
}