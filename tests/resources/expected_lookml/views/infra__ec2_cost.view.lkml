view: infra__ec2_cost {
  sql_table_name: dev_dkruh1.infra__ec2_cost ;;

  dimension_group: billing_period {
    type: time
    sql: ${TABLE}.billing_period ;;
    description: "Billing start period (monthly)"
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

  dimension_group: bill_start_date {
    type: time
    sql: ${TABLE}.bill_start_date ;;
    description: "Billing start time (daily)"
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

  dimension: bill_payer_account_id {
    type: string
    sql: ${TABLE}.bill_payer_account_id ;;
    description: "Account ID (identical to line_item_usage_account_id)"
  }

  dimension: account_name {
    type: string
    sql: ${TABLE}.account_name ;;
    description: "AWS account"
  }

  dimension: product_line {
    type: string
    sql: ${TABLE}.product_line ;;
    description: "Product line attached to the ec2"
  }

  dimension: line_item_line_item_type {
    type: string
    sql: ${TABLE}.line_item_line_item_type ;;
    description: "Cost type (Credit, Usage, Tax)"
  }

  dimension: product_product_family {
    type: string
    sql: ${TABLE}.product_product_family ;;
    description: "Type of the charged service (Storage, Compute, etc)"
  }

  dimension: product_region {
    type: string
    sql: ${TABLE}.product_region ;;
    description: "AWS Region"
  }

  dimension: pricing_term {
    type: string
    sql: ${TABLE}.pricing_term ;;
    description: "On demand, spot or reserved"
  }

  dimension: product_instance_type {
    type: string
    sql: ${TABLE}.product_instance_type ;;
    description: "Instance type"
  }

  dimension: resource_tags_user_sub_component {
    type: string
    sql: ${TABLE}.resource_tags_user_sub_component ;;
    description: "Custom tag for sub component"
  }

  dimension: resource_tags_user_component {
    type: string
    sql: ${TABLE}.resource_tags_user_component ;;
    description: "Custom tag for component"
  }

  dimension: resource_tags_user_environment {
    type: string
    sql: ${TABLE}.resource_tags_user_environment ;;
    description: "Custom tag for environment"
  }

  dimension: unblended_cost {
    type: number
    sql: ${TABLE}.unblended_cost ;;
    description: "Unblended cost (includes the discounts in a single period and not across the period it was given)"
  }

  dimension: amortized_cost {
    type: number
    sql: ${TABLE}.amortized_cost ;;
    description: "Amortized cost equals usage plus the portion of upfront fees applicable to the period (both used and unused)"
  }

  dimension: resource_tags_user_group {
    type: string
    sql: ${TABLE}.resource_tags_user_group ;;
    description: "User tag"
  }

  dimension: resource_tags_user_team {
    type: string
    sql: ${TABLE}.resource_tags_user_team ;;
    description: "Team tag"
  }

  dimension: resource_tags_user_feature {
    type: string
    sql: ${TABLE}.resource_tags_user_feature ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: ags {
    type: string
    sql: ${TABLE}.ags ;;
    description: "Auto-Scaling Group (typo)"
  }

  dimension: is_managed_by_cast_ai {
    type: yesno
    sql: ${TABLE}.is_managed_by_cast_ai ;;
    description: "Whether the EC2 is managed by Cast.AI"
  }

  measure: count {
    type: count
    description: "Default count measure"
  }
}