WITH ht AS
(
SELECT 
    c.subject_id, c.stay_id, c.charttime,
    -- Ensure that all heights are in centimeters, and fix data as needed
    CASE
        -- rule for neonates
        WHEN pt.anchor_age = 0
        AND (c.valuenum * 2.54) < 80
        THEN c.valuenum * 2.54
        -- rule for adults
        WHEN pt.anchor_age > 0
        AND (c.valuenum * 2.54) > 120
        AND (c.valuenum * 2.54) < 230
        THEN c.valuenum * 2.54
        -- set bad data to NULL
        ELSE NULL
    END AS height
    , ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY charttime DESC) AS rn
FROM __schema_name__.chartevents c
INNER JOIN __schema_name__.patients pt
    ON c.subject_id = pt.subject_id
WHERE c.valuenum IS NOT NULL
AND c.valuenum != 0
AND c.itemid IN
(
    226707 -- Height (measured in inches)
    -- note we intentionally ignore the below ITEMID in metavision
    -- these are duplicate data in a different unit
    -- , 226730 -- Height (cm)
)
)
, wt AS
(
    SELECT
        c.stay_id
    , c.charttime
    -- TODO: eliminate obvious outliers if there is a reasonable weight
    , c.valuenum as weight
    , ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY charttime DESC) AS rn
    FROM __schema_name__.chartevents c
    WHERE c.valuenum IS NOT NULL
    AND c.itemid = 226512 -- Admit Wt
    AND c.stay_id IS NOT NULL
    AND c.valuenum > 0
)
SELECT
ie.subject_id, ie.hadm_id, ie.stay_id
, pat.gender AS gender
, FLOOR(DATE_PART('day', adm.admittime - make_timestamp(pat.anchor_year, 1, 1, 0, 0, 0))/365.0) + pat.anchor_age as age
, adm.ethnicity AS ethnicity
, ht.height as height
, wt.weight as weight
, make_timestamp(pat.anchor_year, 1, 1, 0, 0, 0) as dob
, pat.dod as dod
FROM __schema_name__.icustays ie
INNER JOIN __schema_name__.admissions adm
ON ie.hadm_id = adm.hadm_id
INNER JOIN __schema_name__.patients pat
ON ie.subject_id = pat.subject_id
LEFT JOIN ht
ON ie.stay_id = ht.stay_id AND ht.rn = 1
LEFT JOIN wt
ON ie.stay_id = wt.stay_id AND wt.rn = 1
