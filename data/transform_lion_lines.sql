create materialized view if not exists street_mapper as (
select
	distinct on
	(b.shortstreet,
	b.street) b.shortstreet,
	b.street as full_stname
from
	(
	select
		concat_ws(' ',
			a.pdir,
			a.sname,
			a.stype,
			a.sdir) shortstreet,
		a.street
	from
		altnames a) b) with data;
	
create materialized view if not exists node_streets as (
select
	st.*,
	n.shape
from
	(
	select
		n.nodeid,
		array_agg(s.full_stname) streets
	from
		node n
	join (
		select
			nst.*,
			sm.full_stname
		from
			node_stname nst
		join street_mapper sm on
			TRIM(nst.stname) = sm.shortstreet) s
on
		n.nodeid = cast(s.nodeid as numeric)
	group by
		n.nodeid) st
join node n
on
	st.nodeid = n.nodeid
) with data;


SELECT jsonb_build_object('type','FeatureCollection',
'features', array_agg(ST_AsGeoJSON(ns.*)::json))
FROM   node_streets ns 
WHERE NOT EXISTS (
    SELECT -- SELECT list can be empty for this purpose
    FROM   unnest(ns.streets) street
    WHERE  street LIKE '%BOUNDARY'
    or street like '%BNDY'
    or street like '%SHORELINE'
    or street like '%FOOTBRIDGE'
  )
  and array_length(ns.streets, 1) > 1 

