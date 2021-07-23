CREATE OR REPLACE TEMP TABLE unit_inspections as (
    select occ.VHOST,
           occ.PROPERTY_ID,
           occ.UNIT_ID,
           occ.ID                                                                   as OCCUPANCY_ID,
           occ.MOVE_IN,
           occ.MOVE_OUT,
           lead(occ.move_in)
                over (partition by occ.VHOST, occ.UNIT_ID order by occ.MOVE_IN asc) as NEXT_MOVE_IN,
           iff((insp.CREATED_AT >= occ.MOVE_OUT and
                insp.CREATED_AT < iff(NEXT_MOVE_IN is not null, NEXT_MOVE_IN, current_date())),
               insp.ID, Null)                                                       as INSPECTION_ID
    from property_occupancies occ
             left outer join PROPERTY_INSPECTIONS_INSPECTIONS as insp
             on insp.INSPECTABLE_TYPE = 'Unit' and occ.VHOST = insp.VHOST and
                                occ.UNIT_ID = insp.INSPECTABLE_ID
    where occ.DELETED_AT is null
      -- Excluding deleted occupancies excludes some inspections.
      -- Most deleted occupancies shouldn't have an inspection.
      -- Better to exclude some inspections than include all occupancies.
      and occ.move_in is not null
      and occ.move_in
        < current_date () + interval '180 days'
        qualify inspection_id is not null
    order by occ.VHOST, occ.UNIT_ID, occ.ID asc
);

CREATE OR REPLACE TEMP TABLE occupancy_inspections as (
    select occ.VHOST,
           occ.PROPERTY_ID,
           occ.UNIT_ID,
           occ.ID                                                                   as OCCUPANCY_ID,
           occ.MOVE_IN,
           occ.MOVE_OUT,
           lead(occ.move_in)
                over (partition by occ.VHOST, occ.UNIT_ID order by occ.MOVE_IN asc) as NEXT_MOVE_IN,
           insp.ID                                                                  as INSPECTION_ID
    from PROPERTY_OCCUPANCIES as occ
             LEFT OUTER JOIN PROPERTY_INSPECTIONS_INSPECTIONS as insp
             on insp.INSPECTABLE_TYPE = 'Occupancy' and
                                occ.VHOST = insp.VHOST and
                                insp.INSPECTABLE_ID = occ.ID
    where occ.DELETED_AT is null
      -- Excluding deleted occupancies excludes some inspections.
      -- Most deleted occupancies shouldn't have an inspection.
      -- Better to exclude some inspections than include all occupancies.
      and occ.move_in is not null
      and occ.move_in
        < current_date () + interval '180 days'
    order by occ.VHOST, occ.UNIT_ID, occ.ID asc
);

INSERT INTO occupancy_inspections
select *
from unit_inspections
where unit_inspections.INSPECTION_ID is not null;

select occupancy_inspections.*
     , inspection_engagement.ITEMS_CHECKED_ARRAY
     , inspection_engagement.ITEMS_FLAGGED_ARRAY
     , inspection_engagement.ITEMS_NOTE_ARRAY
     , inspection_engagement.ITEMS_IMAGES_ARRAY
from occupancy_inspections
         left outer join (
    select insp.VHOST
         , insp.ID                                            as INSPECTION_ID
         , array_agg(
            case when items.CHECKED = True then 1 end
        ) within group ( order by insp.ID)                    as ITEMS_CHECKED_ARRAY
         , array_agg(
            case when items.FLAGGED = True then 1 end
        ) within group ( order by insp.ID)                    as ITEMS_FLAGGED_ARRAY
         , array_agg(
            case when len(items.NOTE) > 0 then items.NOTE end
        ) within group ( order by insp.ID)                    as ITEMS_NOTE_ARRAY
         , array_agg(img.ID) within group ( order by insp.ID) as ITEMS_IMAGES_ARRAY
    from PROPERTY_INSPECTIONS_INSPECTIONS insp
             join PROPERTY_INSPECTIONS_AREAS areas on insp.VHOST = areas.VHOST and insp.ID = areas.INSPECTION_ID
             join PROPERTY_INSPECTIONS_ITEMS items on areas.VHOST = items.VHOST and areas.ID = items.AREA_ID
             left outer join PROPERTY_INSPECTIONS_ITEM_IMAGE_MAPPINGS img
                             on items.VHOST = img.VHOST and items.ID = img.ITEM_ID
    group by 1, 2
) as inspection_engagement on occupancy_inspections.VHOST = inspection_engagement.VHOST and
                              occupancy_inspections.INSPECTION_ID = inspection_engagement.INSPECTION_ID
-- use this clause to limit the final data run, otherwise you'll return millions of occupancies unnecessarily
WHERE MOVE_IN > current_date() - interval '3 years'