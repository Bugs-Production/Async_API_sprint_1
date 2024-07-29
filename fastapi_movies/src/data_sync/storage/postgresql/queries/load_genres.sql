SELECT
   g.id,
   g.name,
   g.modified
FROM content.genre g
WHERE g.modified > %(dttm)s
GROUP BY g.id
ORDER BY g.modified
LIMIT 100;