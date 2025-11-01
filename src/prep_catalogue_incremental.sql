MERGE `prep_steam.catalogue` P
USING `raw_steam.catalogue` R
ON P.appid = R.appid
WHEN MATCHED AND P.data_extr_date < R.data_extr_date THEN
  UPDATE SET
    name = R.name,
    data_extr_date = R.data_extr_date
WHEN NOT MATCHED THEN
  INSERT (appid, name, data_extr_date)
  VALUES (R.appid, R.name, R.data_extr_date)


-- INSERT INTO `prep_steam.catalogue` (appid, name, data_extr_date)
-- SELECT
--     appid,
--     name,
--     data_extr_date
-- FROM `raw_steam.catalogue`
-- WHERE appid not in
--     (
--         SELECT
--         distinct appid
--         FROM `prep_steam.catalogue`
--     )

-- SELECT *
-- FROM `raw_steam.catalogue`
-- WHERE data_extr_date > NOW() - INTERVAL 1 DAY