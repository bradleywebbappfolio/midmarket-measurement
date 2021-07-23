select turn_users.VHOST, active_user_id, users.EMAIL, users.FIRST_NAME, roles.NAME, sum(turn_users.high_value_active_use) as user_engagement
from (
select
    versions.vhost,
    versions.party_id as TURN_ID,
    versions.whodunnit as active_user_id,
    activities.updated_at as use_timestamp,
    case
        when description like 'Completed Unit Turn' then 1
        when description like 'Added%' then 1
        when description like 'Updated%' then 1
        when description like 'Changed%' then 1
        when description like 'Deleted Target Date%' then 1
        when description like 'Removed%' then 1
        when description like 'Completed%Category' then 1
        else 0
    end high_value_active_use
from property_maintenance_versions versions
join property_property_auditing_auditing_activities activities
    on versions.vhost = activities.vhost
    and versions.id = activities.describable_id
where activities.describable_type = 'PaperTrail::MaintenanceVersion'
  and party_type = 'Maintenance::UnitTurn'
  and versions.whodunnit is not null
  and high_value_active_use = 1
  and activities.UPDATED_AT between '2021-05-01' and current_date()
    ) as turn_users
join property_users users on users.VHOST = turn_users.VHOST and users.ID = turn_users.active_user_id
join property_user_roles roles on roles.vhost = users.vhost and users.USER_ROLE_ID = roles.ID
where users.EMAIL not like 'hidden_at%'
group by 1, 2, 3, 4, 5
order by user_engagement desc