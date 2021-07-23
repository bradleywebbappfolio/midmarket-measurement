CREATE TEMP TABLE IF NOT EXISTS turn_usage as  
(select turns.vhost, turns.id, turns.move_out_occupancy_id, active_user_usage_timestamp, total_interactions, last_updated_at
from property_maintenance_unit_turns turns
left outer join (select 
    vhost,
    turn_id,
    ARRAY_AGG(
        ARRAY_CONSTRUCT(
            active_user_id,
            use_timestamp
        )
    ) within group (order by use_timestamp desc) as active_user_usage_timestamp,
    array_size(active_user_usage_timestamp) as total_interactions,
    get(get(active_user_usage_timestamp, 0),1)::timestamp as last_updated_at
from
(select 
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
  and high_value_active_use = 1)
group by 1, 2) active_usage on turns.vhost = active_usage.vhost and turns.id = active_usage.turn_id);

-- select
--     -- last_updated_at::date as DS_last_active,
--     customer_segment,
--     count_if(turn_id is not null) as turns,
--     count_if(turn_id is not null and active_user_usage_timestamp is not null) as active_turns,
--     active_turns / turns as percent_active,
--     sum(total_interactions)
-- from
-- (
SELECT
    vpp.customer_segment,
    u.vhost,
    p.id AS property_id,
    ug.id AS unit_group_id,
    u.id AS unit_id,
    u.uuid AS unit_UUID,
    occ.id AS occupancy_id,
    occ.move_out as move_out_date,
    turns.move_out_occupancy_id as move_out_occupancy_id,
    turns.id as turn_ID,
    turns.active_user_usage_timestamp,
    turns.total_interactions,
    turns.last_updated_at
FROM property_units u
JOIN vhost_property_portfolio vpp ON u.vhost = vpp.vhost --and vpp.customer_segment not like '%SMB%'
JOIN property_unit_groups ug ON u.vhost = ug.vhost AND u.unit_group_id = ug.id AND ug.deleted_at IS NULL
JOIN property_unit_details unitd ON u.vhost = unitd.vhost AND u.unit_detail_id = unitd.id AND unitd.exclude = FALSE
JOIN property_properties p ON u.vhost = p.vhost AND ug.property_id = p.id AND u.deleted_at IS NULL AND p.deleted_at IS NULL
JOIN property_occupancies occ on u.vhost = occ.vhost and u.id = occ.unit_id and occ.move_out between date('2021-02-03') and current_date()
LEFT OUTER JOIN turn_usage turns on u.vhost = turns.vhost and occ.id = turns.move_out_occupancy_id
-- )
-- group by 1