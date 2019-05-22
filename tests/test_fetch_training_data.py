# Run some simple tests to fetch training data.
# Note that these tests can be very long running!

from adl_func_client.event_dataset import EventDataset
from calratio_perjet_training.fetch_training_data import fetch_perjet_data
import glob

local_file = "G:\\GRIDDocker\\mc16_13TeV.311309.MadGraphPythia8EvtGen_A14NNPDF31LO_HSS_LLP_mH125_mS5_ltlow.deriv.DAOD_EXOT15.e7270_e5984_s3234_r10724_r10726_p3795\\DAOD_EXOT15.17545510._000001.pool.root.1"
local_files = "G:\\GRIDDocker\\mc16_13TeV.311309.MadGraphPythia8EvtGen_A14NNPDF31LO_HSS_LLP_mH125_mS5_ltlow.deriv.DAOD_EXOT15.e7270_e5984_s3234_r10724_r10726_p3795\\*.root.*"
def test_fetch_from_file():
    df = EventDataset(f"file://{local_file}")
    ds = fetch_perjet_data(df, "test_1")
    print (ds)

def get_all_files(wildcard):
    return [f"file://{f}" for f in glob.glob(wildcard)]

def test_fetch_from_glob():
    df = EventDataset(get_all_files(local_files))
    ds = fetch_perjet_data(df, "test_2")
    print (ds)