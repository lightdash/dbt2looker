view: infra__kubernetes_cost {
  sql_table_name: dev_dkruh1.infra__kubernetes_cost ;;

  dimension_group: billing_day {
    type: time
    sql: ${TABLE}.billing_day ;;
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

  dimension: ec2_id {
    type: string
    sql: ${TABLE}.ec2_id ;;
    description: "EC Instace id that ran the pod"
  }

  dimension: service {
    type: string
    sql: ${TABLE}.service ;;
    description: "Service name (e.g. spark, airflow, flink...). The default is taken from kubecost -> properties.container"
  }

  dimension: productline {
    type: string
    sql: ${TABLE}.productline ;;
    description: "Product line using the service. The default is taken from kubecost -> properties.labels.productline/product_line"
  }

  dimension: sub_service {
    type: string
    sql: ${TABLE}.sub_service ;;
    description: "Sub service (e.g. worker, executor). The default is taken from kubecost -> properties.container"
  }

  dimension: release_name {
    type: string
    sql: ${TABLE}.release_name ;;
    description: "Application running the service. The default is taken from kubecost -> properties.labels.app"
  }

  dimension: cluster {
    type: string
    sql: ${TABLE}.cluster ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: namespace {
    type: string
    sql: ${TABLE}.namespace ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: run_id {
    type: string
    sql: ${TABLE}.run_id ;;
    description: "Pod name"
  }

  dimension: cpucost {
    type: number
    sql: ${TABLE}.cpucost ;;
    description: "Total CPU cost of the pod"
  }

  dimension: pvcost {
    type: number
    sql: ${TABLE}.pvcost ;;
    description: "Total Storage cost for a given service"
  }

  dimension: networkcost {
    type: number
    sql: ${TABLE}.networkcost ;;
    description: "Total network cost for a given service"
  }

  dimension: ramcost {
    type: number
    sql: ${TABLE}.ramcost ;;
    description: "Total Memory cost for a given service"
  }

  dimension: totalcost {
    type: number
    sql: ${TABLE}.totalcost ;;
    description: "Total service cost"
  }

  dimension: totalefficiency {
    type: number
    sql: ${TABLE}.totalefficiency ;;
    description: "Memory and CPU efficiency"
  }

  dimension: cpuefficiency {
    type: number
    sql: ${TABLE}.cpuefficiency ;;
    description: "CPU average usage divided by the requested"
  }

  dimension: cpucorehours {
    type: number
    sql: ${TABLE}.cpucorehours ;;
    description: "Total CPU hours of the pod"
  }

  dimension: cpucorerequestaverage {
    type: number
    sql: ${TABLE}.cpucorerequestaverage ;;
    description: "Average Cores requested by the pod"
  }

  dimension: cpucoreusageaverage {
    type: number
    sql: ${TABLE}.cpucoreusageaverage ;;
    description: "Average Cores used by the pod"
  }

  dimension: cpucores {
    type: number
    sql: ${TABLE}.cpucores ;;
    description: "Total Cores requested by the pod"
  }

  dimension: cpucoreusagemax {
    type: number
    sql: ${TABLE}.cpucoreusagemax ;;
    description: "Maximum CPU usage of the pod"
  }

  dimension: networktransferbytes {
    type: number
    sql: ${TABLE}.networktransferbytes ;;
    description: "Total amount of transferred bytes from the pod"
  }

  dimension: networkreceivebytes {
    type: number
    sql: ${TABLE}.networkreceivebytes ;;
    description: "Total amount of received bytes by the pod"
  }

  dimension: ramefficiency {
    type: number
    sql: ${TABLE}.ramefficiency ;;
    description: "Average amount of used bytes divided by bytes requested"
  }

  dimension: rambyteusagemax {
    type: number
    sql: ${TABLE}.rambyteusagemax ;;
    description: "Maximum amount of actual memory usage"
  }

  dimension: rambytehours {
    type: number
    sql: ${TABLE}.rambytehours ;;
    description: "Hourly amount of memory used"
  }

  dimension: rambyteusageaverage {
    type: number
    sql: ${TABLE}.rambyteusageaverage ;;
    description: "Average amount of bytes used by the pod"
  }

  dimension: rambyterequestaverage {
    type: number
    sql: ${TABLE}.rambyterequestaverage ;;
    description: "Average amount of memory used by the pod"
  }

  dimension: rambytes {
    type: number
    sql: ${TABLE}.rambytes ;;
    description: "Memory requested in bytes"
  }

  dimension: minutes {
    type: number
    sql: ${TABLE}.minutes ;;
    description: "Amount of minutes the pod ran"
  }

  dimension: year {
    type: number
    sql: ${TABLE}.year ;;
    description: "Year"
  }

  dimension: month {
    type: number
    sql: ${TABLE}.month ;;
    description: "Month"
  }

  dimension: day {
    type: number
    sql: ${TABLE}.day ;;
    description: "Day of the week"
  }

  dimension: name {
    type: string
    sql: ${TABLE}.name ;;
    description: "Name of the continaer (for debugging)"
  }

  dimension: group {
    type: string
    sql: ${TABLE}.group ;;
    description: "TODO: Update Column {col_name} Information"
  }

  dimension: team {
    type: string
    sql: ${TABLE}.team ;;
    description: "TODO: Update Column {col_name} Information"
  }

  measure: count {
    type: count
    description: "Default count measure"
  }
}