WITH labs_stg_1 AS
(
    SELECT adm.hadm_id, lev.charttime, lev.valuenum
    , CASE
            WHEN itemid = 50809 THEN 'glucose'
            WHEN itemid = 50811 THEN 'hemoglobin'
            WHEN itemid = 50868 THEN 'anion_gap'
            WHEN itemid = 50882 THEN 'bicarbonate'
            WHEN itemid = 50893 THEN 'calcium_total'
            WHEN itemid = 50902 THEN 'chloride'
            WHEN itemid = 50912 THEN 'creatinine'
            WHEN itemid = 50931 THEN 'glucose'
            WHEN itemid = 50960 THEN 'magnesium'
            WHEN itemid = 50970 THEN 'phosphate'
            WHEN itemid = 50971 THEN 'potassium'
            WHEN itemid = 50983 THEN 'sodium'
            WHEN itemid = 51006 THEN 'urea_nitrogen'
            WHEN itemid = 51221 THEN 'hematocrit'
            WHEN itemid = 51222 THEN 'hemoglobin'
            WHEN itemid = 51248 THEN 'mch'
            WHEN itemid = 51249 THEN 'mchc'
            WHEN itemid = 51250 THEN 'mcv'
            WHEN itemid = 51265 THEN 'platelet_count'
            WHEN itemid = 51277 THEN 'rdw'
            WHEN itemid = 51279 THEN 'red_blood_cells'
            WHEN itemid = 51301 THEN 'white_blood_cells'
            WHEN itemid = 51478 THEN 'glucose'
            WHEN itemid = 51480 THEN 'hematocrit'
            WHEN itemid = 51638 THEN 'hematocrit'
            WHEN itemid = 51755 THEN 'white_blood_cells'
            WHEN itemid = 51981 THEN 'glucose'
            WHEN itemid = 52500 THEN 'anion_gap'
            WHEN itemid = 52535 THEN 'chloride'
            WHEN itemid = 52546 THEN 'creatinine'
            WHEN itemid = 52569 THEN 'glucose'
            WHEN itemid = 52610 THEN 'potassium'
            WHEN itemid = 52623 THEN 'sodium'
            WHEN itemid = 52647 THEN 'urea_nitrogen'
        ELSE null
        END AS label
    FROM __schema_name__.admissions adm
    INNER JOIN __schema_name__.icustays icu
    ON icu.hadm_id = adm.hadm_id
    INNER JOIN __schema_name__.labevents lev
    ON lev.hadm_id = adm.hadm_id
    AND lev.charttime >= adm.admittime
    AND lev.charttime <= adm.admittime + interval '48 hour'
    WHERE lev.itemid IN
    (
        50809,
        50811,
        50868,
        50882,
        50893,
        50902,
        50912,
        50931,
        50960,
        50970,
        50971,
        50983,
        51006,
        51221,
        51222,
        51248,
        51249,
        51250,
        51265,
        51277,
        51279,
        51301,
        51478,
        51480,
        51638,
        51755,
        51981,
        52500,
        52535,
        52546,
        52569,
        52610,
        52623,
        52647
    )
    AND valuenum IS NOT null
)
, labs_stg_2 AS
(
SELECT
    hadm_id, valuenum, label
    , ROW_NUMBER() OVER (PARTITION BY hadm_id, label ORDER BY charttime) AS rn
FROM labs_stg_1
)
, labs_stg_3 AS
(
SELECT
    hadm_id
    , rn
    , COALESCE(MAX(CASE WHEN label = 'glucose' THEN valuenum ELSE null END)) AS glucose
    , COALESCE(MAX(CASE WHEN label = 'hemoglobin' THEN valuenum ELSE null END)) AS hemoglobin
    , COALESCE(MAX(CASE WHEN label = 'anion_gap' THEN valuenum ELSE null END)) AS anion_gap
    , COALESCE(MAX(CASE WHEN label = 'bicarbonate' THEN valuenum ELSE null END)) AS bicarbonate
    , COALESCE(MAX(CASE WHEN label = 'calcium_total' THEN valuenum ELSE null END)) AS calcium_total
    , COALESCE(MAX(CASE WHEN label = 'chloride' THEN valuenum ELSE null END)) AS chloride
    , COALESCE(MAX(CASE WHEN label = 'creatinine' THEN valuenum ELSE null END)) AS creatinine
    , COALESCE(MAX(CASE WHEN label = 'magnesium' THEN valuenum ELSE null END)) AS magnesium
    , COALESCE(MAX(CASE WHEN label = 'phosphate' THEN valuenum ELSE null END)) AS phosphate
    , COALESCE(MAX(CASE WHEN label = 'potassium' THEN valuenum ELSE null END)) AS potassium
    , COALESCE(MAX(CASE WHEN label = 'sodium' THEN valuenum ELSE null END)) AS sodium
    , COALESCE(MAX(CASE WHEN label = 'urea_nitrogen' THEN valuenum ELSE null END)) AS urea_nitrogen
    , COALESCE(MAX(CASE WHEN label = 'hematocrit' THEN valuenum ELSE null END)) AS hematocrit
    , COALESCE(MAX(CASE WHEN label = 'mch' THEN valuenum ELSE null END)) AS mch
    , COALESCE(MAX(CASE WHEN label = 'mchc' THEN valuenum ELSE null END)) AS mchc
    , COALESCE(MAX(CASE WHEN label = 'mcv' THEN valuenum ELSE null END)) AS mcv
    , COALESCE(MAX(CASE WHEN label = 'platelet_count' THEN valuenum ELSE null END)) AS platelet_count
    , COALESCE(MAX(CASE WHEN label = 'rdw' THEN valuenum ELSE null END)) AS rdw
    , COALESCE(MAX(CASE WHEN label = 'red_blood_cells' THEN valuenum ELSE null END)) AS red_blood_cells
    , COALESCE(MAX(CASE WHEN label = 'white_blood_cells' THEN valuenum ELSE null END)) AS white_blood_cells
FROM labs_stg_2
GROUP BY hadm_id, rn
)
, labs_stg_4 AS
(
    SELECT
    hadm_id,
    AVG(glucose) AS glucose,
    AVG(hemoglobin) AS hemoglobin,
    AVG(anion_gap) AS anion_gap,
    AVG(bicarbonate) AS bicarbonate,
    AVG(calcium_total) AS calcium_total,
    AVG(chloride) AS chloride,
    AVG(creatinine) AS creatinine,
    AVG(magnesium) AS magnesium,
    AVG(phosphate) AS phosphate,
    AVG(potassium) AS potassium,
    AVG(sodium) AS sodium,
    AVG(urea_nitrogen) AS urea_nitrogen,
    AVG(hematocrit) AS hematocrit,
    AVG(mch) AS mch,
    AVG(mchc) AS mchc,
    AVG(mcv) AS mcv,
    AVG(platelet_count) AS platelet_count,
    AVG(rdw) AS rdw,
    AVG(red_blood_cells) AS red_blood_cells,
    AVG(white_blood_cells) AS white_blood_cells
    FROM labs_stg_3
    GROUP BY hadm_id
)
SELECT * FROM labs_stg_4
