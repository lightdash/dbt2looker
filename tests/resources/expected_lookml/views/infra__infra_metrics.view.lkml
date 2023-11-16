view: infra__infra_metrics {
  sql_table_name: dev_dkruh1.infra__infra_metrics ;;

  dimension_group: ts {
    type: time
    sql: ${TABLE}.ts ;;
    description: "the timestamp the metric was taken"
    datatype: timestamp
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

  dimension: product_line {
    type: string
    sql: ${TABLE}.product_line ;;
    description: "metric value"
  }

  dimension: source {
    type: string
    sql: ${TABLE}.source ;;
    description: "the source of the metric (i.e github)"
  }

  dimension: component {
    type: string
    sql: ${TABLE}.component ;;
    description: "the component this metric refers to (i.e  git repo name)"
  }

  dimension: key {
    type: string
    sql: ${TABLE}.key ;;
    description: "metric key"
  }

  dimension: value {
    type: number
    sql: ${TABLE}.value ;;
    description: "the metric's value"
  }

  dimension: custom_properties {
    type: string
    sql: ${TABLE}.custom_properties ;;
    description: "any custom porerties in addition to the key value of the metrics"
  }

  dimension: row_number {
    type: number
    sql: ${TABLE}.row_number ;;
    description: "Row number basted on partitioning key and component"
  }

  measure: sum_of_values {
    type: sum
    description: "the metric's value"
    sql: ${TABLE}.value ;;
    value_format_name: decimal_0
  }

  measure: count {
    type: count
    description: "Default count measure"
  }
}