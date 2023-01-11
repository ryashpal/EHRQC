if __name__ == '__main__':

    import pandas as pd
    from pathlib import Path

    from medcat.config import Config
    from medcat.cdb_maker import CDBMaker

    baseDir = '/superbugai-data/yash/temp'
    rxnormDir = Path(baseDir, 'trained_vocs/shared/RxNorm/rrf')
    fileName = Path(rxnormDir, 'RXNCONSO.RRF')

    conceptsDf = pd.read_csv(Path(baseDir, fileName), sep='|', header=None)
    cdbConceptDf = conceptsDf.iloc[:, [0, 14]]
    cdbConceptDf.columns = ['cui', 'name']
    cdbConceptDf['cui'] = cdbConceptDf['cui'].apply(str)
    # print(conceptsDf.iloc[:, 0])
    # conceptsDf.iloc[:, [0, 14]].to_csv(Path('/tmp/cdb_concepts.csv'))

    # Build the concept databse from the DataFrame
    config = Config()
    config.general['spacy_model'] = 'en_core_web_md'
    cdbMaker = CDBMaker(config)
    cdb = cdbMaker.prepare_csvs(csv_paths=[cdbConceptDf])
    cdb.save(Path(baseDir, 'RxNorm_cdb.dat'))
