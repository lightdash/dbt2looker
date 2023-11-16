view: ugc__shopper_experience_order_aggregation {
  sql_table_name: dev_dkruh1.ugc__shopper_experience_order_aggregation ;;

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

  dimension: currency {
    type: string
    sql: ${TABLE}.currency ;;
    description: "Currency"
  }

  dimension: orders_sum_of_interacted_users {
    type: number
    sql: ${TABLE}.orders_sum_of_interacted_users ;;
    description: "Sum of the orders of interacted users per currency"
  }

  dimension: orders_count_of_interacted_users {
    type: number
    sql: ${TABLE}.orders_count_of_interacted_users ;;
    description: "Total orders of interacted users per currency"
  }

  dimension: interacted_users_with_orders {
    type: number
    sql: ${TABLE}.interacted_users_with_orders ;;
    description: "Amount of interacted users that placed an order per currency"
  }

  dimension: interacted_users_without_orders {
    type: number
    sql: ${TABLE}.interacted_users_without_orders ;;
    description: "Amount of interacted users that did not place an order per currency"
  }

  dimension: orders_sum_of_non_interacted_users {
    type: number
    sql: ${TABLE}.orders_sum_of_non_interacted_users ;;
    description: "Sum of the orders of non interacted users per currency"
  }

  dimension: orders_count_of_non_interacted_users {
    type: number
    sql: ${TABLE}.orders_count_of_non_interacted_users ;;
    description: "Total orders of non interacted users per currency"
  }

  dimension: non_interacted_users_with_orders {
    type: number
    sql: ${TABLE}.non_interacted_users_with_orders ;;
    description: "Amount of non interacted users that placed an order per currency"
  }

  dimension: non_interacted_users_without_orders {
    type: number
    sql: ${TABLE}.non_interacted_users_without_orders ;;
    description: "Amount of non interacted users that did not place an order per currency"
  }

  measure: count {
    type: count
    description: "Default count measure"
  }
}