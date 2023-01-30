import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import qc.demographicsGraphs as demographicsGraphs
import qc.vitalsGraphs as vitalsGraphs
import ehrqc.qc.vitalsOutliers as vitalsOutliers
import qc.labMeasurementsGraphs as labMeasurementsGraphs
import ehrqc.qc.Outliers as Outliers
