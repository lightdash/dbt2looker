view: pages {
  sql_table_name: "postgres"."public"."pages" ;;

  dimension_group: viewed_at {
    type: time
    sql: ${TABLE}.viewed_at ;;
    description: "Timestamp that page was viewed at"
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

  dimension_group: date {
    type: time
    sql: ${TABLE}.date ;;
    description: "Date that the page was viewed at"
    datatype: date
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year,
    ]
  }

  dimension: path {
    type: string
    sql: ${TABLE}.path ;;
    description: "The page path. Everything after https://gethubble.io."
  }

  dimension: unique_page_view_id {
    type: string
    sql: ${TABLE}.id ;;
    description: "The primary key for this table"
  }

  dimension: referring_domain {
    type: string
    sql: ${TABLE}.referring_domain ;;
    description: "Website domain of the referrer. e.g. google.com"
  }

  dimension: user_id {
    type: string
    sql: ${TABLE}.user_id ;;
    description: "persistent id of the user viewing the page"
  }

  measure: total_page_views {
    type: count
    sql: ${TABLE}.id ;;
    description: "The primary key for this table"
  }

  measure: blog_views {
    type: count
    sql: ${TABLE}.id ;;
    description: "The primary key for this table"
    filters: [
      path: "/blog%",
    ]
    value_format_name: decimal_0
    group_label: "Blog Info"
  }
}