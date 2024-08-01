SELECT
   uuid,
   full_name,
   modified,
   COALESCE (json_agg(DISTINCT jsonb_build_object(
      'id', film_work_id,
      'roles', roles,
      'title', title,
      'rating', rating
   )), '[]') as "films"
FROM (
    SELECT
        p.uuid,
        p.full_name,
        p.modified,
        pf.film_work_id,
        fw.title,
        fw.rating,
        ARRAY_AGG(pf.role) as roles
    FROM (
        SELECT
            p.id uuid,
            p.full_name,
            p.created,
            p.modified
        FROM content.person p
        WHERE p.modified > %(dttm)s
        LIMIT 100
    ) p
    JOIN content.person_film_work pf
    ON p.uuid=pf.person_id
    JOIN content.film_work fw
    ON pf.film_work_id=fw.id
    GROUP BY p.uuid, p.full_name, pf.film_work_id, fw.title, fw.rating, p.modified
)
GROUP BY uuid, full_name, modified
