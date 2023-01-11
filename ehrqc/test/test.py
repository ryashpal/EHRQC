import os
import pandas as pd
import numpy as np
from context import demographicsGraphs
from context import vitalsGraphs
from context import vitalsAnomalies
from context import labMeasurementsGraphs
from context import Anomaly
from datetime import date

import unittest

import dbUtils

class TestPlots(unittest.TestCase):


    def testDemographicsPlots(self):
        if os.path.exists('./demographics.html'):
            os.remove('./demographics.html')
        data = [
            [0, 1, 2, 'male', 'white', date.fromisoformat('2020-09-13'), date.fromisoformat('2021-09-13')], 
            [2, 3, 4, np.nan, 'white', date.fromisoformat('2020-09-14'), date.fromisoformat('2021-09-13')], 
            [4, 5, 6, 'female', 'black', date.fromisoformat('2020-09-15'), date.fromisoformat('2021-09-13')], 
            [6, 7, 8, np.nan, 'asian', date.fromisoformat('2020-09-14'), date.fromisoformat('2021-09-13')]]
        demographicsGraphs.plot(pd.DataFrame(data, columns=['age', 'weight', 'height', 'gender', 'ethnicity', 'dob', 'dod']))
        assert os.path.exists('./demographics.html')


    def testDemographicsMimicPlots(self):
        if os.path.exists('./demographics.html'):
            os.remove('./demographics.html')
        df = dbUtils._getDemographics()
        demographicsGraphs.plot(df)
        assert os.path.exists('./demographics.html')


    def testVitalsPlots(self):
        if os.path.exists('./vitals.html'):
            os.remove('./vitals.html')
        data = [
            [0, 1, 2], 
            [2, np.nan, 4], 
            [4, 5, np.nan], 
            [0, 1, 2], 
            [2, 3, 4], 
            [4, 5, np.nan], 
            [0, 1, 2], 
            [2, 3, 4], 
            [4, 5, 6], 
            [6, 7, np.nan]]
        vitalsGraphs.plot(pd.DataFrame(data, columns=['heartrate', 'sysbp', 'diabp']))
        assert os.path.exists('./vitals.html')


    def testVitalsMimicPlots(self):
        if os.path.exists('./vitals.html'):
            os.remove('./vitals.html')
        df = dbUtils._getVitals()
        vitalsGraphs.plot(df)
        assert os.path.exists('./vitals.html')


    def testLabMeasurementsPlots(self):
        if os.path.exists('./lab_measurements.html'):
            os.remove('./lab_measurements.html')
        data = [
            [0, 1, 2], 
            [2, np.nan, 4], 
            [4, 5, np.nan], 
            [0, 1, 2], 
            [2, 3, 4], 
            [4, 5, np.nan], 
            [0, 1, 2], 
            [2, 3, 4], 
            [4, 5, 6], 
            [6, 7, np.nan]]
        labMeasurementsGraphs.plot(pd.DataFrame(data, columns=['glucose', 'hemoglobin', 'anion_gap']))
        assert os.path.exists('./lab_measurements.html')


    def testLabMeasurementsMimicPlots(self):
        if os.path.exists('./lab_measurements.html'):
            os.remove('./lab_measurements.html')
        df = dbUtils._getLabMeasurements()
        labMeasurementsGraphs.plot(df)
        assert os.path.exists('./lab_measurements.html')


    def testAnomalies(self):
        data = [
            [1, 2], 
            [2, 3], 
            [3, 4], 
            [4, 5], 
            [5, 6], 
            [6, 7], 
            [7, 8], 
            [8, 9]]
        df = pd.DataFrame(data, columns=['x', 'y'])
        out = Anomaly.irt_ensemble(df)
        assert out.x.sum() == 36


    def testVitalsAnomalies(self):
        if os.path.exists('./vitals_anomalies.html'):
            os.remove('./vitals_anomalies.html')
        df = dbUtils._getVitals()
        vitalsAnomalies.plot(df)
        assert os.path.exists('./vitals_anomalies.html')
