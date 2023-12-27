connection: "yoda"
include: "views/*"

explore: email__daily_total_email_campaigns {
  description: "Email Marketing total campaigns per day in utc time"
  sql_always_where: orders_count = 1 ;;
}