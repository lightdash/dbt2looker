connection: "yoda"
include: "views/*"

explore: email__daily_total_email_campaigns {
  description: "Email Marketing total campaigns per day in utc time"
  sql_always_where: email__daily_total_email_campaigns.count_campaigns = {% parameter email__daily_total_email_campaigns.past_num_days %} ;;
}