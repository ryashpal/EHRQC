import argparse

import logging

from whoosh.index import create_in
from whoosh import scoring
from whoosh.fields import *

from fuzzywuzzy import fuzz
from fuzzywuzzy import process

from sentence_transformers import SentenceTransformer, util

log = logging.getLogger("Standardise")


def fetchMatchingConceptFuzzy(searchPhrase, standardConcepts):
    matchingConcept = process.extract(searchPhrase, standardConcepts, limit=1, scorer=fuzz.token_set_ratio)
    return matchingConcept[0][0]


def fetchMatchingConceptSemantic(searchPhrase, standardConcepts):
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


def fetchMatchingConceptMedcat(searchPhrase, vocab, cdb, mc_status):
    from medcat.cat import CAT

    cat = CAT(cdb=cdb, config=cdb.config, vocab=vocab, meta_cats=[mc_status])

    entities = cat.get_entities(searchPhrase)['entities']
    matchingConceptPrettyName = None
    maxContextSimilarityScore = 0
    for key in entities.keys():
        entity = entities[key]
        contextSimilarityScore = float(entity['context_similarity'])
        if contextSimilarityScore > maxContextSimilarityScore:
            matchingConceptPrettyName = entity['pretty_name']
            maxContextSimilarityScore = contextSimilarityScore
    return matchingConceptPrettyName


def fetchMatchingConceptFromReverseIndex(searchPhrase, ix):
        from whoosh.qparser import QueryParser
        matchingConcept = None
        with ix.searcher() as searcher:
            query = QueryParser("concept", ix.schema).parse(searchPhrase)
            results = searcher.search(query)
            if len(results) > 0:
                matchingConcept = results[0]['concept']

        return matchingConcept


def fetchMatchingConceptCreatingReverseIndex(searchPhrase, standardConcepts):
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
    , algorithm='reverse_index'
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
        matchingConcept = None
        if algorithm == 'fuzzy':
            matchingConcept = fetchMatchingConceptFuzzy(
                searchPhrase= re.sub(r'[\$\{\(\[\^\}\]\+\)\/]+', ' ', row[fieldName]) + keyPhrase,
                standardConcepts = standardConceptsDf['concept_name'],
                algorithm=algorithm
                )
        elif algorithm == 'semantic':
            matchingConcept = fetchMatchingConceptSemantic(
                searchPhrase= re.sub(r'[\$\{\(\[\^\}\]\+\)\/]+', ' ', row[fieldName]) + keyPhrase,
                standardConcepts = standardConceptsDf['concept_name'],
                algorithm=algorithm
                )
        elif algorithm == 'reverse_index':
            matchingConcept = fetchMatchingConceptCreatingReverseIndex(
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


def fetchMatchingConceptFromMajorityVoting(searchPhrase, standardConcepts, ix, vocab, cdb, mc_status):

    medcatConcept = fetchMatchingConceptMedcat(searchPhrase=searchPhrase, vocab=vocab, cdb=cdb, mc_status=mc_status)
    fuzzyConcept = fetchMatchingConceptFuzzy(searchPhrase=searchPhrase, standardConcepts=standardConcepts)
    reverseIndexConcept = None
    if ix:
        reverseIndexConcept = fetchMatchingConceptFromReverseIndex(searchPhrase=searchPhrase, ix=ix)
    else:
        reverseIndexConcept = fetchMatchingConceptCreatingReverseIndex(searchPhrase=searchPhrase, standardConcepts=standardConcepts)
    if (medcatConcept == fuzzyConcept == reverseIndexConcept):
        return [(searchPhrase, medcatConcept, fuzzyConcept, reverseIndexConcept, medcatConcept, 'High')]
    elif (medcatConcept == fuzzyConcept != reverseIndexConcept):
        return [(searchPhrase, medcatConcept, fuzzyConcept, reverseIndexConcept, medcatConcept, 'Medium'), (searchPhrase, medcatConcept, fuzzyConcept, reverseIndexConcept, reverseIndexConcept, 'Low')]
    elif (medcatConcept == reverseIndexConcept != fuzzyConcept):
        return [(searchPhrase, medcatConcept, fuzzyConcept, reverseIndexConcept, medcatConcept, 'Medium'), (searchPhrase, medcatConcept, fuzzyConcept, reverseIndexConcept, fuzzyConcept, 'Low')]
    elif (reverseIndexConcept == fuzzyConcept != medcatConcept):
        return [(searchPhrase, medcatConcept, fuzzyConcept, reverseIndexConcept, reverseIndexConcept, 'Medium'), (searchPhrase, medcatConcept, fuzzyConcept, reverseIndexConcept, medcatConcept, 'Low')]
    elif (medcatConcept != fuzzyConcept != reverseIndexConcept):
        return [(searchPhrase, medcatConcept, fuzzyConcept, reverseIndexConcept, medcatConcept, 'Low'), (searchPhrase, medcatConcept, fuzzyConcept, reverseIndexConcept, fuzzyConcept, 'Low'), (searchPhrase, medcatConcept, fuzzyConcept, reverseIndexConcept, reverseIndexConcept, 'Low')]


def generateCustomMappingsForReview(domainId, vocabularyId, conceptClassId, vocabPath, cdbPath, mc_statusPath, conceptsPath, conceptNameRow, mappedConceptSavePath):

    from ehrqc.Utils import getConnection
    import pandas as pd

    con = getConnection()
    standardConceptsQuery = """
    select
    *
    from
    omop_migration_etl_20220817.voc_concept
    where
    domain_id = '""" + domainId + """'
    and vocabulary_id = '""" + vocabularyId + """'
    and concept_class_id = '""" + conceptClassId + """'
    """

    standardConceptsDf = pd.read_sql_query(standardConceptsQuery, con)

    schema = Schema(concept=TEXT(stored=True, analyzer=analysis.StemmingAnalyzer()))
    
    import os
    
    if not os.path.isdir("/tmp/indexdir"):
        os.makedirs("/tmp/indexdir")

    ix = create_in("/tmp/indexdir", schema)

    writer = ix.writer()
    for standardConcept in standardConceptsDf.concept_name:
        writer.add_document(concept=standardConcept)
    writer.commit()

    from medcat.vocab import Vocab
    from medcat.cdb import CDB
    from medcat.meta_cat import MetaCAT

    # Load the vocab model you downloaded
    vocab = Vocab.load(vocabPath)
    # Load the cdb model you downloaded
    cdb = CDB.load(cdbPath)
    # Download the mc_status model from the models section below and unzip it
    mc_status = MetaCAT.load(mc_statusPath)

    conceptsDf = pd.read_csv(conceptsPath)

    outRows = []
    
    from tqdm import tqdm

    for i, row in tqdm(conceptsDf.iterrows(), total=conceptsDf.shape[0]):

        if(pd.isna(row[conceptNameRow])):
            continue

        matchingConcepts = fetchMatchingConceptFromMajorityVoting(
            searchPhrase=row[conceptNameRow]
            , standardConcepts=standardConceptsDf.concept_name
            , ix=ix
            , vocab=vocab
            , cdb=cdb
            , mc_status=mc_status
            )

        for matchingConcept in matchingConcepts:
            matchingConceptList = list(matchingConcept)
            outRows.append(matchingConceptList)

    matchingConceptsDf = pd.DataFrame(outRows, columns=['searchPhrase', 'medcatConcept', 'fuzzyConcept', 'reverseIndexConcept', 'majorityVoting', 'confidence'])
    matchingConceptsDf.to_csv(mappedConceptSavePath)


if __name__ == "__main__":

    print("Parsing command line arguments")

    parser = argparse.ArgumentParser(description='Perform concept mapping')

    parser.add_argument("domain_id", help="Domain ID of the standard vocabulary to be mapped")
    parser.add_argument("vocabulary_id", help="Vocabulary ID of the standard vocabulary to be mapped")
    parser.add_argument("concept_class_id", help="Concept class ID of the standard vocabulary to be mapped")
    parser.add_argument("vocab_path", help="Path for the Medcat vocab file")
    parser.add_argument("cdb_path", help="Path for the Medcat cdb file")
    parser.add_argument("mc_status_path", help="Path for the Medcat mc_status folder")
    parser.add_argument("concepts_path", help="Path for the concepts csv file")
    parser.add_argument("concept_name_row", help="Name of the concept name row in the concepts csv file")
    parser.add_argument("mapped_concepts_save_path", help="Path for saving the mapped concepts csv file")

    args = parser.parse_args()

    generateCustomMappingsForReview(
        domainId=args.domain_id
        , vocabularyId=args.vocabulary_id
        , conceptClassId=args.concept_class_id
        , vocabPath=args.vocab_path
        , cdbPath=args.cdb_path
        , mc_statusPath=args.mc_status_path
        , conceptsPath=args.concepts_path
        , conceptNameRow=args.concept_name_row
        , mappedConceptSavePath=args.mapped_concepts_save_path
        )

    # baseDir = '/superbugai-data/yash/temp/'
    # generateCustomMappingsForReview(
    #     domainId='Procedure'
    #     , vocabularyId='SNOMED'
    #     , conceptClassId='Procedure'
    #     , vocabPath=baseDir + 'trained_vocs/shared/vocab.dat'
    #     , cdbPath=baseDir + 'Athena_SNOMED_Procedure_Procedure_cdb.dat'
    #     , mc_statusPath=baseDir + 'trained_vocs/shared/mc_status'
    #     , conceptsPath='/tmp/20004_operation.csv'
    #     , conceptNameRow='sourceName'
    #     , mappedConceptSavePath='/tmp/20004_operation_mapped.csv'
    #     )

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

    # standardConceptsQuery = """
    # select
    # *
    # from
    # omop_migration_etl_20220817.voc_concept
    # where domain_id = 'Procedure' and vocabulary_id = 'SNOMED' and concept_class_id = 'Procedure'
    # """

    # # and vocabulary_id = '' and concept_class_id = ''

    # standardConceptsDf = pd.read_sql_query(standardConceptsQuery, con)

    # print(standardConceptsDf)
    
    # import time

    # print('building index: ', time.strftime("%Y-%m-%d %H:%M"))

    # schema = Schema(concept=TEXT(stored=True, analyzer=analysis.StemmingAnalyzer()))
    # ix = create_in("temp/indexdir", schema)

    # writer = ix.writer()
    # for standardConcept in standardConceptsDf.concept_name:
    #     writer.add_document(concept=standardConcept)
    # writer.commit()

    # matchingConcepts = []

    # print('matching concept 1: ', time.strftime("%Y-%m-%d %H:%M"))

    # matchingConcepts1 = performMajorityVoting(
    #     searchPhrase='Escherichia coli', standardConcepts=standardConceptsDf.concept_name, ix=ix
    #     )

    # matchingConcepts.append(matchingConcepts1)

    # print('matching concept 2: ', time.strftime("%Y-%m-%d %H:%M"))

    # matchingConcepts2 = performMajorityVoting(
    #     searchPhrase='salmonella aeruginosa', standardConcepts=standardConceptsDf.concept_name, ix=ix
    #     )

    # matchingConcepts.append(matchingConcepts2)

    # print('matching concept 3: ', time.strftime("%Y-%m-%d %H:%M"))

    # matchingConcepts3 = performMajorityVoting(
    #     searchPhrase='pseudomonas aeruginosa', standardConcepts=standardConceptsDf.concept_name, ix=ix
    #     )

    # matchingConcepts.append(matchingConcepts3)

    # print('matching concept 4: ', time.strftime("%Y-%m-%d %H:%M"))

    # matchingConcepts4 = performMajorityVoting(
    #     searchPhrase='klebsiella pneumoniae', standardConcepts=standardConceptsDf.concept_name, ix=ix
    #     )

    # matchingConcepts.append(matchingConcepts4)

    # for matchingConcept in matchingConcepts:
    #     print(matchingConcept)

    # print('completed: ', time.strftime("%Y-%m-%d %H:%M"))

    # from medcat.vocab import Vocab
    # from medcat.cdb import CDB
    # from medcat.meta_cat import MetaCAT

    # baseDir = '/superbugai-data/yash/temp/'
    # vocabPath = baseDir + 'trained_vocs/shared/vocab.dat'
    # cdbPath = baseDir + 'Athena_SNOMED_Procedure_Procedure_cdb.dat'
    # mc_statusPath = baseDir + 'trained_vocs/shared/mc_status'

    # # Load the vocab model you downloaded
    # vocab = Vocab.load(vocabPath)
    # # Load the cdb model you downloaded
    # cdb = CDB.load(cdbPath)
    # # Download the mc_status model from the models section below and unzip it
    # mc_status = MetaCAT.load(mc_statusPath)

    # conceptsDf = pd.read_csv('/superbugai-data/yash/chapter_1/workspace/ETL-UK-Biobank/resources/baseline_field_mapping/20004_operation.csv')

    # outRows = []

    # for i, row in conceptsDf.iterrows():

    #     print(row['sourceName'])

    #     if(pd.isna(row['sourceName'])):
    #         continue

        # matchingConcepts = fetchMatchingConceptFromMajorityVoting(
        #     searchPhrase=row['sourceName']
        #     , standardConcepts=standardConceptsDf.concept_name
        #     , ix=ix
        #     , vocab=vocab
        #     , cdb=cdb
        #     , mc_status=mc_status
        #     )

        # for matchingConcept in matchingConcepts:
        #     matchingConceptList = list(matchingConcept)
        #     matchingConceptList.append(row['sourceValueCode'])
        #     outRows.append(matchingConceptList)

        # if (i % 10) == 0:
        #     print('i: ', i)
        # #     break

        # if i == 7:
        #     break

    # matchingConceptsDf = pd.DataFrame(outRows, columns=['searchPhrase', 'medcatConcept', 'fuzzyConcept', 'reverseIndexConcept', 'majorityVoting', 'confidence', 'id'])
    # matchingConceptsDf.to_csv(baseDir + '20004_operation_mapped.csv')


    # baseDir = '/superbugai-data/yash/temp/'
    # generateCustomMappingsForReview(
    #     domainId='Procedure'
    #     , vocabularyId='SNOMED'
    #     , conceptClassId='Procedure'
    #     , vocabPath=baseDir + 'trained_vocs/shared/vocab.dat'
    #     , cdbPath=baseDir + 'Athena_SNOMED_Procedure_Procedure_cdb.dat'
    #     , mc_statusPath=baseDir + 'trained_vocs/shared/mc_status'
    #     , conceptsPath='/tmp/20004_operation.csv'
    #     , conceptRow='sourceName'
    #     , mappedConceptSavePath='/tmp/20004_operation_mapped.csv'
    #     )

    # domain_id = 'Procedure' and vocabulary_id = 'SNOMED' and concept_class_id = 'Procedure'
    # vocabPath = baseDir + 'trained_vocs/shared/vocab.dat'
    # cdbPath = baseDir + 'Athena_SNOMED_Procedure_Procedure_cdb.dat'
    # mc_statusPath = baseDir + 'trained_vocs/shared/mc_status'
