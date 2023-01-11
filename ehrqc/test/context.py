import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import qc.demographicsGraphs as demographicsGraphs
import qc.vitalsGraphs as vitalsGraphs
import qc.vitalsAnomalies as vitalsAnomalies
import qc.labMeasurementsGraphs as labMeasurementsGraphs
import ehrqc.qc.Anomaly as Anomaly
