SELECT
   fw.id,
   fw.title,
   fw.description,
   fw.rating,
   fw.type,
   fw.created,
   fw.modified,
   ARRAY_AGG(DISTINCT CONCAT(g.id, ': ', g.name)) AS "genres",
    ARRAY_AGG(DISTINCT CONCAT(p.id, ': ', p.full_name))
        FILTER (WHERE pfw.role = 'actor') AS "actors",
    ARRAY_AGG(DISTINCT CONCAT(p.id, ': ', p.full_name))
        FILTER (WHERE pfw.role = 'director') AS "directors",
    ARRAY_AGG(DISTINCT CONCAT(p.id, ': ', p.full_name))
        FILTER (WHERE pfw.role = 'writer') AS "writers"
FROM content.film_work fw
LEFT JOIN content.person_film_work pfw ON pfw.film_work_id = fw.id
LEFT JOIN content.person p ON p.id = pfw.person_id
LEFT JOIN content.genre_film_work gfw ON gfw.film_work_id = fw.id
LEFT JOIN content.genre g ON g.id = gfw.genre_id
WHERE fw.modified > %(dttm)s
GROUP BY fw.id
ORDER BY fw.modified
LIMIT 100;
