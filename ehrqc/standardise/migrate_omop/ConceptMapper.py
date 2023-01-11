import logging

from whoosh.index import create_in
from whoosh import scoring
from whoosh.fields import *

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from sentence_transformers import SentenceTransformer, util

log = logging.getLogger("Standardise")


def fetchMatchingConcept(searchPhrase, standardConcepts, algorithm='semantic'):

    if algorithm == 'fuzzy':

        matchingConcept = process.extract(searchPhrase, standardConcepts, limit=1, scorer=fuzz.token_set_ratio)

        return matchingConcept[0][0]

    elif algorithm == 'semantic':

        model = SentenceTransformer('pritamdeka/S-BioBert-snli-multinli-stsb')
        searchPhraseEmbedding = model.encode(searchPhrase, convert_to_tensor=True)
        matchingConcept = None
        highestScore = 0

        for standardConcept in standardConcepts:
            standardConceptEmbedding = model.encode(standardConcept, convert_to_tensor=True)
            similarityScore = util.pytorch_cos_sim(searchPhraseEmbedding, standardConceptEmbedding)
            if similarityScore > highestScore:
                highestScore = similarityScore
                matchingConcept = standardConcept

        return matchingConcept

    elif algorithm == 'reverse_index':

        matchingConcept = None

        schema = Schema(concept=TEXT(stored=True))
        ix = create_in("temp/indexdir", schema)

        writer = ix.writer()
        for standardConcept in standardConcepts:
            writer.add_document(concept=standardConcept)
        writer.commit()

        from whoosh.qparser import QueryParser
        with ix.searcher() as searcher:
            query = QueryParser("concept", ix.schema).parse(searchPhrase)
            results = searcher.search(query)
            if len(results) > 0:
                matchingConcept = results[0]['concept']

        return matchingConcept


def createCustomMapping(
    con
    , etlSchemaName: str
    , fieldName: str
    , tableName: str
    , whereCondition: str
    , sourceVocabularyId: str
    , domainId: str
    , vocabularyId: str
    , conceptClassId: str
    , keyPhrase: str
    , algorithm='semantic'
    ):

    import pandas as pd
    import datetime

    sourceConceptsQuery =  """select distinct(""" + fieldName + """) from """ + etlSchemaName + """.""" + tableName + """ where """ + ('true' if (whereCondition == '') else whereCondition)
    sourceConceptsDf = pd.read_sql_query(
       sourceConceptsQuery
        , con
        )


    standardConceptsQuery = """
    select
    *
    from
    """ + etlSchemaName + """.voc_concept
    where true
    """
    if domainId:
        standardConceptsQuery += """
        and domain_id = '""" + domainId + """'
        """
    if vocabularyId:
        standardConceptsQuery += """
        and vocabulary_id = '""" + vocabularyId + """'
        """
    if conceptClassId:
        standardConceptsQuery += """
        and concept_class_id = '""" + conceptClassId + """'
        """
    """;
    """

    standardConceptsDf = pd.read_sql_query(standardConceptsQuery, con)

    from tqdm import tqdm
    import re

    df = None
    for index, row in tqdm(sourceConceptsDf.iterrows(), total = sourceConceptsDf.shape [0]):
        matchingConcept = fetchMatchingConcept(
            searchPhrase= re.sub(r'[\$\{\(\[\^\}\]\+\)\/]+', ' ', row[fieldName]) + keyPhrase,
            standardConcepts = standardConceptsDf['concept_name'],
            algorithm=algorithm
            )
        standardRow = standardConceptsDf[standardConceptsDf['concept_name'] == matchingConcept].head(1)
        standardRow['concept_name'] = row[fieldName]
        currentConceptIdQuery = """select max(source_concept_id) from """ + etlSchemaName + """.tmp_custom_mapping where source_concept_id > 2100000000"""
        currentConceptIdDf = pd.read_sql_query(currentConceptIdQuery, con)
        standardRow['source_concept_id'] = 1 + max(2100000000, 0 if (currentConceptIdDf['max'][0] is None) else currentConceptIdDf['max'][0], (0 if (df is None) else max(df['source_concept_id'].values)))
        standardRow['source_vocabulary_id'] = sourceVocabularyId
        standardRow['source_domain_id'] = standardRow['domain_id']
        standardRow['source_concept_class_id'] = standardRow['concept_class_id']
        standardRow['standard_concept'] = None
        standardRow['concept_code'] = row[fieldName]
        standardRow['valid_start_date'] = datetime.datetime(1970, 1, 1)
        standardRow['valid_end_date'] = datetime.datetime(2099, 12, 31)
        standardRow['invalid_reason'] = None
        standardRow['relationship_id'] = 'Maps to'
        standardRow['reverese_relationship_id'] = 'Mapped from'
        standardRow['invalid_reason_cr'] = None
        standardRow['relationship_valid_start_date'] = datetime.datetime(1970, 1, 1)
        standardRow['relationship_end_date'] = datetime.datetime(2099, 12, 31)
        if df is None:
            df = standardRow
        else:
            df = pd.concat([df, standardRow], axis=0, ignore_index=True)

    df.rename(
        columns={
            'concept_id': 'target_concept_id'
            }
        , inplace=True
        )
    return df


def performMajorityVoting(searchPhrase, standardConcepts):
    semanticConcept = fetchMatchingConcept(searchPhrase=searchPhrase, standardConcepts=standardConcepts, algorithm='semantic')
    fuzzyConcept = fetchMatchingConcept(searchPhrase=searchPhrase, standardConcepts=standardConcepts, algorithm='fuzzy')
    reverseIndexConcept = fetchMatchingConcept(searchPhrase=searchPhrase, standardConcepts=standardConcepts, algorithm='reverse_index')
    if (semanticConcept == fuzzyConcept == reverseIndexConcept):
        return [(searchPhrase, semanticConcept, 'High')]
    elif (semanticConcept == fuzzyConcept != reverseIndexConcept):
        return [(searchPhrase, semanticConcept, 'Medium'), (searchPhrase, reverseIndexConcept, 'Low')]
    elif (semanticConcept == reverseIndexConcept != fuzzyConcept):
        return [(searchPhrase, semanticConcept, 'Medium'), (searchPhrase, fuzzyConcept, 'Low')]
    elif (reverseIndexConcept == fuzzyConcept != semanticConcept):
        return [(searchPhrase, reverseIndexConcept, 'Medium'), (searchPhrase, semanticConcept, 'Low')]
    elif (semanticConcept != fuzzyConcept != reverseIndexConcept):
        return [(searchPhrase, semanticConcept, 'Low'), (searchPhrase, fuzzyConcept, 'Low'), (searchPhrase, reverseIndexConcept, 'Low')]


if __name__ == "__main__":

    # from ehrqc.Utils import getConnection
    # import pandas as pd

    # con = getConnection()
    # df = createCustomMapping(
    #     con
    #     , 'omop_migration_etl_20220817'
    #     , 'admission_type'
    #     , 'src_admissions'
    #     , ''
    #     , 'mimiciv_vis_admission_type'
    #     , 'Visit'
    #     , 'CMS Place of Service'
    #     , 'Visit'
    #     , ''
    #     , algorithm='semantic'
    #     # , ''
    #     )
    # df = createCustomMapping(
    #     con
    #     , 'omop_migration_etl_20220906'
    #     , 'org_name'
    #     , 'src_microbiologyevents'
    #     , """org_name not in (
    #         select
    #         concept_name
    #         from
    #         omop_migration_etl_20220906.tmp_custom_mapping
    #     )
    #     ;
    #     """
    #     , 'mimiciv_micro_organism'
    #     , 'Observation'
    #     , 'SNOMED'
    #     , 'Organism'
    #     , ''
    #     , algorithm='semantic'
    #     # , ''
    #     )
    # df = createCustomMapping(
    #     con
    #     , 'omop_migration_etl_20220817'
    #     , 'drug'
    #     , 'src_prescriptions'
    #     , """ndc not in (
    #         select con.concept_code from omop_migration_etl_20220817.voc_concept con where con.domain_id = 'Drug' and con.vocabulary_id = 'NFC'
    #         )
    #         and gsn not in (
    #             select con.concept_code from omop_migration_etl_20220817.voc_concept con where con.domain_id = 'Drug' and con.vocabulary_id = 'GCN_SEQNO'
    #         ) limit 5"""
    #     , 'mimiciv_drug_ndc'
    #     , 'Drug'
    #     , ''
    #     , ''
    #     , ''
    #     # , ''
    #     )

    # outDf = None
    # for index, row in df.iterrows():
    #     resRow = pd.read_sql_query('select concept_id, concept_name as "Automatic Mapping" from omop_migration_etl_20220817.voc_concept where concept_id = ' + str(row['target_concept_id']), con)
    #     resRow['Concept'] = row['concept_name']
    #     if outDf is None:
    #         outDf = resRow
    #     else:
    #         outDf = pd.concat([outDf, resRow], axis=0, ignore_index=True)
    # outDf.to_csv('/tmp/org_name_semantic_mapping.csv')

    # matchingConcept = fetchMatchingConcept(
    #     searchPhrase='Even More', 
    #     standardConcepts=[
    #         'This is the first document Even More we\'ve added!',
    #         'The second one is even moore Interesting!',
    #         'This is the third Even more document we\'ve added!',
    #         'The fourth one is even more interesting!',
    #         ], 
    #     algorithm='reverse_index')
    # print('matchingConcept: ', matchingConcept)

    matchingConcepts = performMajorityVoting(
        searchPhrase='interesting',
        standardConcepts=[
            'This is the first document Even More we\'ve added!',
            'The second one is even moore Interesting!',
            'This is the third Even more document we\'ve added!',
            'The fourth one is even more interesting!',
            ], 
        )
    print(matchingConcepts)
