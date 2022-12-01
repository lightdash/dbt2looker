connection: "hubble_analytics"
include: "/views/*"

explore: pages {
  description: "Page views for Hubble landing page"

  join: users {
    type: left_outer
    relationship: many_to_one
    sql_on: ${users.id} = ${pages.user_id} ;;
  }
}