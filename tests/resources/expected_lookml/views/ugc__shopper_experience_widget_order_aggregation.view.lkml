view: ugc__shopper_experience_widget_order_aggregation {
  sql_table_name: dev_dkruh1.ugc__shopper_experience_widget_order_aggregation ;;

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

  dimension: currency {
    type: string
    sql: ${TABLE}.currency ;;
    description: "Currency"
  }

  dimension: orders_count {
    type: number
    sql: ${TABLE}.orders_count ;;
    description: "Amount of all orders per widget per currency"
  }

  dimension: orders_sum_interacted_users {
    type: number
    sql: ${TABLE}.orders_sum_interacted_users ;;
    description: "Orders sum of interacted users per widget per currency"
  }

  dimension: orders_count_of_interacted_users {
    type: number
    sql: ${TABLE}.orders_count_of_interacted_users ;;
    description: "Orders amount of interacted users per widget per currency"
  }

  dimension: orders_sum_non_interacted_users {
    type: number
    sql: ${TABLE}.orders_sum_non_interacted_users ;;
    description: "Orders sum of non interacted users per widget per currency"
  }

  dimension: orders_count_of_non_interacted_users {
    type: number
    sql: ${TABLE}.orders_count_of_non_interacted_users ;;
    description: "Orders amount of non interacted users per widget per currency"
  }

  measure: count {
    type: count
    description: "Default count measure"
  }
}