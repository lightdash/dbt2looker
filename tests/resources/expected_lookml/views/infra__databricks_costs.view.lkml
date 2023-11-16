view: infra__databricks_costs {
  sql_table_name: dev_dkruh1.infra__databricks_costs ;;

  dimension_group: date {
    type: time
    sql: ${TABLE}.date ;;
    description: "TODO: Update Column {col_name} Information"
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

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
    description: "TODO: Update Table Description"
  }

  dimension: product_line {
    type: string
    sql: ${TABLE}.product_line ;;
    description: "TODO: Update Table Description"
  }

  dimension: instance_type {
    type: string
    sql: ${TABLE}.instance_type ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: instance_count {
    type: number
    sql: ${TABLE}.instance_count ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: dbus {
    type: number
    sql: ${TABLE}.dbus ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: machinehours {
    type: number
    sql: ${TABLE}.machinehours ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: databricks_cost {
    type: number
    sql: ${TABLE}.databricks_cost ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: aws_cost {
    type: number
    sql: ${TABLE}.aws_cost ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: product_product_family {
    type: string
    sql: ${TABLE}.product_product_family ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: pricing_term {
    type: string
    sql: ${TABLE}.pricing_term ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: sku {
    type: string
    sql: ${TABLE}.sku ;;
    description: "TODO: Update Table Description"
  }

  dimension: is_photon {
    type: yesno
    sql: ${TABLE}.is_photon ;;
    description: "TODO: Update Table Description"
  }

  dimension: type {
    type: string
    sql: ${TABLE}.type ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: clustername {
    type: string
    sql: ${TABLE}.clustername ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: component {
    type: string
    sql: ${TABLE}.component ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: availability_zones {
    type: string
    sql: ${TABLE}.availability_zones ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: resource_tags_user_group {
    type: string
    sql: ${TABLE}.resource_tags_user_group ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: resource_tags_user_team {
    type: string
    sql: ${TABLE}.resource_tags_user_team ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: resource_tags_user_feature {
    type: string
    sql: ${TABLE}.resource_tags_user_feature ;;
    description: "TODO: Update Column {col_name} Information"
  }

  measure: count {
    type: count
    description: "Default count measure"
  }
}