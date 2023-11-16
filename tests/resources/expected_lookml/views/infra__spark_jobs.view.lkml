view: infra__spark_jobs {
  sql_table_name: dev_dkruh1.infra__spark_jobs ;;

  dimension_group: run_date {
    type: time
    sql: ${TABLE}.run_date ;;
    description: "Date format of the job execution"
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

  dimension: run_id {
    type: string
    sql: ${TABLE}.run_id ;;
    description: "The unique job id"
  }

  dimension: year {
    type: number
    sql: ${TABLE}.year ;;
    description: "The year the job ran on"
  }

  dimension: month {
    type: number
    sql: ${TABLE}.month ;;
    description: "The month the job ran on"
  }

  dimension: day {
    type: number
    sql: ${TABLE}.day ;;
    description: "Day of the spark job"
  }

  dimension: product_line {
    type: string
    sql: ${TABLE}.product_line ;;
    description: "Product line"
  }

  dimension: group {
    type: string
    sql: ${TABLE}.group ;;
    description: "Group name"
  }

  dimension: team {
    type: string
    sql: ${TABLE}.team ;;
    description: "Team"
  }

  dimension: total_network_transfer_in_bytes {
    type: number
    sql: ${TABLE}.total_network_transfer_in_bytes ;;
    description: "Total amounts of bytes transferred to the different components of the job"
  }

  dimension: total_network_received_in_bytes {
    type: number
    sql: ${TABLE}.total_network_received_in_bytes ;;
    description: "Total amounts of bytes received from the different components of the job"
  }

  dimension: cost {
    type: number
    sql: ${TABLE}.cost ;;
    description: "Total cost for a given spark job (computation resources only)"
  }

  dimension: cpucost {
    type: number
    sql: ${TABLE}.cpucost ;;
    description: "Total cpu cost for a given run"
  }

  dimension: pvcost {
    type: number
    sql: ${TABLE}.pvcost ;;
    description: "The amount of kubernetes storage (PV) cost for the specific job"
  }

  dimension: networkcost {
    type: number
    sql: ${TABLE}.networkcost ;;
    description: "The amount of network cost for the specific job"
  }

  dimension: ramcost {
    type: number
    sql: ${TABLE}.ramcost ;;
    description: "The amount of memory cost for the specific job"
  }

  dimension: cpucorehours {
    type: number
    sql: ${TABLE}.cpucorehours ;;
    description: "Total cpu hours for a given run"
  }

  dimension: rambytehours {
    type: number
    sql: ${TABLE}.rambytehours ;;
    description: "The amount of ram hours the job has consumed"
  }

  dimension: productline_instance_tag {
    type: string
    sql: ${TABLE}.productline_instance_tag ;;
    description: "The product line the EC2 instance is tagged to"
  }

  dimension: is_multi_az_cost {
    type: yesno
    sql: ${TABLE}.is_multi_az_cost ;;
    description: "Whether the job ran on mutiple AZ (and therefore will intorduce data transfer cost)"
  }

  dimension: region {
    type: string
    sql: ${TABLE}.region ;;
    description: "The region the job ran on"
  }

  dimension: release_name {
    type: string
    sql: ${TABLE}.release_name ;;
    description: "The name of the job"
  }

  dimension: duration {
    type: number
    sql: ${TABLE}.duration ;;
    description: "Job duration in minutes"
  }

  dimension: driver_cores {
    type: number
    sql: ${TABLE}.driver_cores ;;
    description: "Amount of cores the driver requested"
  }

  dimension: driver_memory_in_bytes {
    type: number
    sql: ${TABLE}.driver_memory_in_bytes ;;
    description: "The amount of memory the driver has requested"
  }

  dimension: driver_avg_network_transfer_in_bytes {
    type: number
    sql: ${TABLE}.driver_avg_network_transfer_in_bytes ;;
    description: "Total average amount of bytes trasferred from the driver for a given job"
  }

  dimension: driver_avg_network_received_in_bytes {
    type: number
    sql: ${TABLE}.driver_avg_network_received_in_bytes ;;
    description: "Total average amount of bytes received by the driver for a given job"
  }

  dimension: driver_max_core_usage {
    type: number
    sql: ${TABLE}.driver_max_core_usage ;;
    description: "The maximum amount of cores which was used for a give job"
  }

  dimension: driver_max_memory_usage {
    type: number
    sql: ${TABLE}.driver_max_memory_usage ;;
    description: "The maximum amount of memory which was used for a give job"
  }

  dimension: driver_instance_types {
    type: string
    sql: ${TABLE}.driver_instance_types ;;
    description: "EC2 Instance type which the driver ran on"
  }

  dimension: driver_instance_vcpu {
    type: string
    sql: ${TABLE}.driver_instance_vcpu ;;
    description: "The amount of virtual cpus the EC2 instance which ran the driver has"
  }

  dimension: driver_instance_memory {
    type: string
    sql: ${TABLE}.driver_instance_memory ;;
    description: "The amount of memory the EC2 instance which ran the driver has"
  }

  dimension: driver_instance_network_performance {
    type: string
    sql: ${TABLE}.driver_instance_network_performance ;;
    description: "The network performance the EC2 instance which ran the driver has"
  }

  dimension: driver_instance_pricing_term {
    type: string
    sql: ${TABLE}.driver_instance_pricing_term ;;
    description: "Whether the EC2 instance that ran the driver is spot, on demand or reserved"
  }

  dimension: driver_availability_zone {
    type: string
    sql: ${TABLE}.driver_availability_zone ;;
    description: "The AZ which the driver was provisioned in"
  }

  dimension: driver_product_storage {
    type: string
    sql: ${TABLE}.driver_product_storage ;;
    description: "Whether the instance provisioned has storage or requires EBS"
  }

  dimension: driver_ram_efficiency {
    type: number
    sql: ${TABLE}.driver_ram_efficiency ;;
    description: "The average amount of memory divided by the memory requested by the driver"
  }

  dimension: driver_ram_efficiency_by_max {
    type: number
    sql: ${TABLE}.driver_ram_efficiency_by_max ;;
    description: "The maximum amount of memory divided by the memory requested by the driver"
  }

  dimension: driver_cpu_efficiency {
    type: number
    sql: ${TABLE}.driver_cpu_efficiency ;;
    description: "Driver CPU average usage devided by the requested CPU"
  }

  dimension: driver_cpu_efficiency_by_max {
    type: number
    sql: ${TABLE}.driver_cpu_efficiency_by_max ;;
    description: "Driver CPU max usage devided by the requested CPU"
  }

  dimension: driver_cost {
    type: number
    sql: ${TABLE}.driver_cost ;;
    description: "Total cost only for the driver"
  }

  dimension: is_driver_od_candidate {
    type: yesno
    sql: ${TABLE}.is_driver_od_candidate ;;
    description: "Whether the driver is a candidate to be replaced with spot (based on the amount of minutes the job ran)"
  }

  dimension: avg_executor_duration {
    type: number
    sql: ${TABLE}.avg_executor_duration ;;
    description: "Average executors duration in minutes"
  }

  dimension: max_executor_duration {
    type: number
    sql: ${TABLE}.max_executor_duration ;;
    description: "The maximum amount of executor duration"
  }

  dimension: min_executor_duration {
    type: number
    sql: ${TABLE}.min_executor_duration ;;
    description: "The minimum amount of executor duration"
  }

  dimension: total_executors_duration {
    type: number
    sql: ${TABLE}.total_executors_duration ;;
    description: "The total amounts of minutes of the executors"
  }

  dimension: executors_count {
    type: number
    sql: ${TABLE}.executors_count ;;
    description: "Total amount of executors which participated in the job"
  }

  dimension: executor_cores {
    type: number
    sql: ${TABLE}.executor_cores ;;
    description: "Cores requested by the executor"
  }

  dimension: executor_memory_in_bytes {
    type: number
    sql: ${TABLE}.executor_memory_in_bytes ;;
    description: "Memory requested by the executor in bytes"
  }

  dimension: executor_max_core_usage {
    type: number
    sql: ${TABLE}.executor_max_core_usage ;;
    description: "Maximum used core across all executors"
  }

  dimension: executor_max_memory_usage {
    type: number
    sql: ${TABLE}.executor_max_memory_usage ;;
    description: "Maximum used memory across all executors"
  }

  dimension: executor_avg_network_transfer_in_bytes {
    type: number
    sql: ${TABLE}.executor_avg_network_transfer_in_bytes ;;
    description: "Total average amount of bytes transferred from the driver for a given job"
  }

  dimension: executor_avg_network_received_in_bytes {
    type: number
    sql: ${TABLE}.executor_avg_network_received_in_bytes ;;
    description: "Total average amount of bytes received by the execotors for a given job"
  }

  dimension: executors_instance_types {
    type: string
    sql: ${TABLE}.executors_instance_types ;;
    description: "Collection of instance types across all executors"
  }

  dimension: executors_vcpu {
    type: string
    sql: ${TABLE}.executors_vcpu ;;
    description: "The amount of virtual cpus the instaces that ran the executor has"
  }

  dimension: executors_memory {
    type: string
    sql: ${TABLE}.executors_memory ;;
    description: "The amount of memory the executed request"
  }

  dimension: executor_network_performance {
    type: string
    sql: ${TABLE}.executor_network_performance ;;
    description: "The network performance of the EC2 instance which ran the driver"
  }

  dimension: executor_pricing_term {
    type: string
    sql: ${TABLE}.executor_pricing_term ;;
    description: "Whether the EC2 instance that ran the executor is spot, on demand or reserved"
  }

  dimension: executors_availability_zone {
    type: string
    sql: ${TABLE}.executors_availability_zone ;;
    description: "Collection of all the AZ across all executors"
  }

  dimension: executors_product_storage {
    type: string
    sql: ${TABLE}.executors_product_storage ;;
    description: "Whether the instance comes with storage or if requires an EBS"
  }

  dimension: executor_ram_efficiency {
    type: number
    sql: ${TABLE}.executor_ram_efficiency ;;
    description: "The average amount of memory the executor used"
  }

  dimension: executor_ram_efficiency_by_max {
    type: number
    sql: ${TABLE}.executor_ram_efficiency_by_max ;;
    description: "The maximum amount of memory the executor used"
  }

  dimension: executor_cpu_efficiency {
    type: number
    sql: ${TABLE}.executor_cpu_efficiency ;;
    description: "Executor CPU average usage devided by the requested CPU"
  }

  dimension: executor_cpu_efficiency_by_max {
    type: number
    sql: ${TABLE}.executor_cpu_efficiency_by_max ;;
    description: "Executor CPU max usage devided by the requested CPU"
  }

  dimension: executors_cost {
    type: number
    sql: ${TABLE}.executors_cost ;;
    description: "Total cost of all executors"
  }

  measure: count {
    type: count
    description: "Default count measure"
  }
}