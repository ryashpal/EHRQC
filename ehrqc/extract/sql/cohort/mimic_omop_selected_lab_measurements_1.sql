with labs_stg_1 as
	(
		select
		mmt.person_id AS person_id
		, measurement_datetime as measurement_datetime
		, value_as_number as value_as_number
		, CASE
				WHEN measurement_concept_id = 3047181 THEN 'lactate'
				WHEN measurement_concept_id = 3013290 THEN 'carbondioxide_blood'
				WHEN measurement_concept_id = 3024561 THEN 'albumin'
				WHEN measurement_concept_id = 3024629 THEN 'glucose_urine'
				WHEN measurement_concept_id = 3008939 THEN 'band_form_neutrophils'
				WHEN measurement_concept_id = 3012501 THEN 'base_excess_in_blood'
				WHEN measurement_concept_id = 3005456 THEN 'potassium_blood'
				WHEN measurement_concept_id = 3010421 THEN 'ph_blood'
				WHEN measurement_concept_id = 3014576 THEN 'chloride_serum'
				WHEN measurement_concept_id = 3031147 THEN 'carbondioxide_serum'
				WHEN measurement_concept_id = 3024128 THEN 'bilirubin'
				WHEN measurement_concept_id = 3000905 THEN 'leukocytes_blood_auto'
				WHEN measurement_concept_id = 3016723 THEN 'creatinine'
				WHEN measurement_concept_id = 3022217 THEN 'inr'
				WHEN measurement_concept_id = 3019550 THEN 'sodium_serum'
				WHEN measurement_concept_id = 3000285 THEN 'sodium_blood'
				WHEN measurement_concept_id = 3000963 THEN 'hemoglobin'
				WHEN measurement_concept_id = 3018672 THEN 'ph_bodyfluid'
				WHEN measurement_concept_id = 3024929 THEN 'platelet_count'
				WHEN measurement_concept_id = 3013682 THEN 'urea_nitrogen'
				WHEN measurement_concept_id = 3004501 THEN 'glucose_serum'
				WHEN measurement_concept_id = 3018572 THEN 'chloride_blood'
				WHEN measurement_concept_id = 3027315 THEN 'oxygen'
				WHEN measurement_concept_id = 3016293 THEN 'bicarbonate'
				WHEN measurement_concept_id = 3023103 THEN 'potassium_serum'
				WHEN measurement_concept_id = 3037278 THEN 'anion_gap'
				WHEN measurement_concept_id = 3003282 THEN 'leukocytes_blood_manual'
				WHEN measurement_concept_id = 3023314 THEN 'hematocrit'
				WHEN measurement_concept_id = 3013466 THEN 'aptt'
			ELSE null
			END AS label
		from
		__schema_name__.measurement mmt
		INNER JOIN __schema_name__.cohort co
			ON co.person_id = mmt.person_id
			AND mmt.measurement_datetime >= co.anchor_time - interval '__before_time__ hour'
			AND mmt.measurement_datetime <= co.anchor_time + interval '__after_time__ hour'
		inner join __schema_name__.concept cpt
		on cpt.concept_id = mmt.measurement_concept_id
		where
		measurement_concept_id in (
		3047181	-- Lactate [Moles/volume] in Blood
		, 3013290	-- Carbon dioxide [Partial pressure] in Blood
		, 3024561	-- Albumin [Mass/volume] in Serum or Plasma
		, 3024629	-- Glucose [Mass/volume] in Urine by Test strip
		, 3008939	-- Band form neutrophils [#/volume] in Blood by Manual count
		, 3012501	-- Base excess in Blood by calculation
		, 3005456	-- Potassium [Moles/volume] in Blood
		, 3010421	-- pH of Blood
		, 3014576	-- Chloride [Moles/volume] in Serum or Plasma
		, 3031147	-- Carbon dioxide, total [Moles/volume] in Blood by calculation
		, 3024128	-- Bilirubin.total [Mass/volume] in Serum or Plasma
		, 3000905	-- Leukocytes [#/volume] in Blood by Automated count
		, 3016723	-- Creatinine [Mass/volume] in Serum or Plasma
		, 3022217	-- INR in Platelet poor plasma by Coagulation assay
		, 3019550	-- Sodium [Moles/volume] in Serum or Plasma
		, 3000285	-- Sodium [Moles/volume] in Blood
		, 3000963	-- Hemoglobin [Mass/volume] in Blood
		, 3000963	-- Hemoglobin [Mass/volume] in Blood
		, 3018672	-- pH of Body fluid
		, 3024929	-- Platelets [#/volume] in Blood by Automated count
		, 3013682	-- Urea nitrogen [Mass/volume] in Serum or Plasma
		, 3004501	-- Glucose [Mass/volume] in Serum or Plasma
		, 3018572	-- Chloride [Moles/volume] in Blood
		, 3027315	-- Oxygen [Partial pressure] in Blood
		, 3016293	-- Bicarbonate [Moles/volume] in Serum or Plasma
		, 3023103	-- Potassium [Moles/volume] in Serum or Plasma
		, 3037278	-- Anion gap 4 in Serum or Plasma
		, 3003282	-- Leukocytes [#/volume] in Blood by Manual count
		, 3023314	-- Hematocrit [Volume Fraction] of Blood by Automated count
		, 3013466	-- aPTT in Blood by Coagulation assay
		)
		and value_as_number is not null
	)
, labs_stg_2 as
	(
	  select
		person_id,
		value_as_number,
		label,
		row_number() over (partition by person_id, label order by measurement_datetime) as rn
	  from labs_stg_1
	)
, labs_stg_3 AS
(
SELECT
	person_id
    , rn
    , COALESCE(MAX(CASE WHEN label = 'lactate' THEN value_as_number ELSE null END)) AS lactate
    , COALESCE(MAX(CASE WHEN label = 'carbondioxide_blood' THEN value_as_number ELSE null END)) AS carbondioxide_blood
    , COALESCE(MAX(CASE WHEN label = 'albumin' THEN value_as_number ELSE null END)) AS albumin
    , COALESCE(MAX(CASE WHEN label = 'glucose_urine' THEN value_as_number ELSE null END)) AS glucose_urine
    , COALESCE(MAX(CASE WHEN label = 'band_form_neutrophils' THEN value_as_number ELSE null END)) AS band_form_neutrophils
    , COALESCE(MAX(CASE WHEN label = 'base_excess_in_blood' THEN value_as_number ELSE null END)) AS base_excess_in_blood
    , COALESCE(MAX(CASE WHEN label = 'potassium_blood' THEN value_as_number ELSE null END)) AS potassium_blood
    , COALESCE(MAX(CASE WHEN label = 'ph_blood' THEN value_as_number ELSE null END)) AS ph_blood
    , COALESCE(MAX(CASE WHEN label = 'chloride_serum' THEN value_as_number ELSE null END)) AS chloride_serum
    , COALESCE(MAX(CASE WHEN label = 'carbondioxide_serum' THEN value_as_number ELSE null END)) AS carbondioxide_serum
    , COALESCE(MAX(CASE WHEN label = 'bilirubin' THEN value_as_number ELSE null END)) AS bilirubin
    , COALESCE(MAX(CASE WHEN label = 'leukocytes_blood_auto' THEN value_as_number ELSE null END)) AS leukocytes_blood_auto
    , COALESCE(MAX(CASE WHEN label = 'creatinine' THEN value_as_number ELSE null END)) AS creatinine
    , COALESCE(MAX(CASE WHEN label = 'inr' THEN value_as_number ELSE null END)) AS inr
    , COALESCE(MAX(CASE WHEN label = 'sodium_serum' THEN value_as_number ELSE null END)) AS sodium_serum
    , COALESCE(MAX(CASE WHEN label = 'sodium_blood' THEN value_as_number ELSE null END)) AS sodium_blood
    , COALESCE(MAX(CASE WHEN label = 'hemoglobin' THEN value_as_number ELSE null END)) AS hemoglobin
    , COALESCE(MAX(CASE WHEN label = 'ph_bodyfluid' THEN value_as_number ELSE null END)) AS ph_bodyfluid
    , COALESCE(MAX(CASE WHEN label = 'platelet_count' THEN value_as_number ELSE null END)) AS platelet_count
    , COALESCE(MAX(CASE WHEN label = 'urea_nitrogen' THEN value_as_number ELSE null END)) AS urea_nitrogen
    , COALESCE(MAX(CASE WHEN label = 'glucose_serum' THEN value_as_number ELSE null END)) AS glucose_serum
    , COALESCE(MAX(CASE WHEN label = 'chloride_blood' THEN value_as_number ELSE null END)) AS chloride_blood
    , COALESCE(MAX(CASE WHEN label = 'oxygen' THEN value_as_number ELSE null END)) AS oxygen
    , COALESCE(MAX(CASE WHEN label = 'bicarbonate' THEN value_as_number ELSE null END)) AS bicarbonate
    , COALESCE(MAX(CASE WHEN label = 'potassium_serum' THEN value_as_number ELSE null END)) AS potassium_serum
    , COALESCE(MAX(CASE WHEN label = 'anion_gap' THEN value_as_number ELSE null END)) AS anion_gap
    , COALESCE(MAX(CASE WHEN label = 'leukocytes_blood_manual' THEN value_as_number ELSE null END)) AS leukocytes_blood_manual
    , COALESCE(MAX(CASE WHEN label = 'hematocrit' THEN value_as_number ELSE null END)) AS hematocrit
    , COALESCE(MAX(CASE WHEN label = 'aptt' THEN value_as_number ELSE null END)) AS aptt
FROM labs_stg_2
GROUP BY person_id, rn
)
, labs_stg_4 AS
(
    SELECT
    person_id,
    __agg_function__(lactate) AS lactate,
    __agg_function__(carbondioxide_blood) AS carbondioxide_blood,
    __agg_function__(albumin) AS albumin,
    __agg_function__(glucose_urine) AS glucose_urine,
    __agg_function__(band_form_neutrophils) AS band_form_neutrophils,
    __agg_function__(base_excess_in_blood) AS base_excess_in_blood,
    __agg_function__(potassium_blood) AS potassium_blood,
    __agg_function__(ph_blood) AS ph_blood,
    __agg_function__(chloride_serum) AS chloride_serum,
    __agg_function__(carbondioxide_serum) AS carbondioxide_serum,
    __agg_function__(bilirubin) AS bilirubin,
    __agg_function__(leukocytes_blood_auto) AS leukocytes_blood_auto,
    __agg_function__(creatinine) AS creatinine,
    __agg_function__(inr) AS inr,
    __agg_function__(sodium_serum) AS sodium_serum,
    __agg_function__(sodium_blood) AS sodium_blood,
    __agg_function__(hemoglobin)  AS hemoglobin,
    __agg_function__(ph_bodyfluid) AS ph_bodyfluid,
    __agg_function__(platelet_count) AS platelet_count,
    __agg_function__(urea_nitrogen) AS urea_nitrogen,
    __agg_function__(glucose_serum) AS glucose_serum,
    __agg_function__(chloride_blood) AS chloride_blood,
    __agg_function__(oxygen) AS oxygen,
    __agg_function__(bicarbonate) AS bicarbonate,
    __agg_function__(potassium_serum) AS potassium_serum,
    __agg_function__(anion_gap) AS anion_gap,
    __agg_function__(leukocytes_blood_manual) AS leukocytes_blood_manual,
    __agg_function__(hematocrit) AS hematocrit,
    __agg_function__(aptt) AS aptt
    FROM labs_stg_3
	GROUP BY person_id
)
SELECT * FROM labs_stg_4
