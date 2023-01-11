baseDir = '../data'
import pandas as pd

conceptsDf = pd.read_csv(baseDir + '/' + 'concept_names.txt', sep='\t')


import pandas as pd

snomedDf = pd.read_csv(baseDir + '/preprocessed_snomed.csv')
snomedDf = snomedDf[snomedDf.name_status == 'P']


from fuzzywuzzy import fuzz
from fuzzywuzzy import process

def performFuzzyMatching(conceptId, conceptName, conceptVocabularyId):
    matchingConcept = process.extract(conceptName,  snomedDf.name, limit=1, scorer=fuzz.token_sort_ratio)
    return conceptId, conceptName, conceptVocabularyId, matchingConcept

from multiprocessing import Pool
matchingOutputFuzzy = []
with Pool() as p:
    matchingOutputFuzzy = p.starmap(
        performFuzzyMatching
        , zip(
            conceptsDf.source_concept_id
            , conceptsDf.concept_name
            , conceptsDf.source_vocabulary_id
            )
    )

matchingOutputFuzzyDf = pd.DataFrame(matchingOutputFuzzy, columns=['concept_id', 'concept_name', 'concept_vocabulary_id', 'matching_concept'])

matchingOutputFuzzyDf.to_csv(baseDir + '/' + 'mapped_concept_names_fuzzy_1.csv', index=False)
