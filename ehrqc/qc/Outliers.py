import os, tempfile, subprocess
import pandas as pd


def irt_ensemble(data):

    inFile = tempfile.NamedTemporaryFile(delete=False)
    outFile = tempfile.NamedTemporaryFile(delete=False)
    try:
        data.to_csv(inFile, index=False)
        subprocess.call (["/usr/bin/Rscript", "--vanilla", "ehrqc/qc/script.r", inFile.name, outFile.name])

        if outFile:
            try:
                out = pd.read_csv(outFile)
            except:
                out = None
        else:
            out = None

    finally:
        inFile.close()
        outFile.close()
        os.unlink(inFile.name)
        os.unlink(outFile.name)

    return out


if __name__ == '__main__':
    import pandas as pd
    data = pd.read_csv('/home/yram0006/phd/chapter_1/workspace/EHRQC/temp/mimic_vitals_imputed.csv')
    df = pd.DataFrame(data, columns=['id' ,'stay_id' ,'heartrate' ,'sysbp' ,'diabp' ,'meanbp' ,'resprate' ,'tempc' ,'spo2' ,'gcseye' ,'gcsverbal' ,'gcsmotor'])

    out = irt_ensemble(df[['heartrate' ,'sysbp' ,'diabp' ,'meanbp' ,'resprate' ,'tempc' ,'spo2' ,'gcseye' ,'gcsverbal' ,'gcsmotor']])
    out.to_csv('/home/yram0006/phd/chapter_1/workspace/EHRQC/temp/mimic_vitals_irt.csv')
