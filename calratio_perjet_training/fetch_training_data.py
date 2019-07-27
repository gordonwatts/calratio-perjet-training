# Contains code needed to fetch the jet data training sets
import sys
import os
from adl_func_client.event_dataset import EventDataset
from adl_func_backend.cpplib.math_utils import DeltaR
from pandas import DataFrame
import pandas as pd

from adl_func_client.use_exe_func_adl_server import use_exe_func_adl_server
import adl_func_backend.xAODlib.atlas_xaod_executor as xaod
xaod.dump_cpp = True

class FetchDataException(BaseException):
    'Thrown when there is a config or similar error getting the data'
    def __init__(self, msg):
        BaseException.__init__(self, msg)

class track_columns:
    r'''
    Helper class to track columns that we are going to request
    '''
    def __init__(self):
        self.col_names = []
        self.col_expr = []

    def add_col (self, name, expr):
        self.col_names += [name]
        self.col_expr += [expr]

    def col_index(self, name):
        'Return the name of a column'
        for i, n in enumerate(self.col_names):
            if n == name:
                return i
        raise BaseException('Unable to find column "{0}"!'.format(name))

    def gen_tuple(self):
        'Return the tuple string'
        return '({0})'.format(','.join(self.col_expr))

def add_sampling_layer(name, index, tc):
    'Add a column accessing the energy sampling guy'
    tc.add_col(name, 'ji[1].getAttributeVectorFloat("EnergyPerSampling")[{0}]/(ji[1].getAttributeVectorFloat("EnergyPerSampling").Sum())'.format(index))


async def fetch_perjet_data (events: EventDataset, ds_name: str) -> DataFrame:
    r'''
    Return back the training data for a particular dataset

    Arguments:
        events             Event Dataset to process
        ds_name         The name of the dataset (used for caching)
    '''

    # Track basic event info, jets, and LLP particles.
    event_info = events \
        .Select("lambda e: (e.EventInfo('EventInfo'), e.Jets('AntiKt4EMTopoJets'), e.TruthParticles('TruthParticles').Where(lambda tp1: tp1.pdgId() == 35))")
    # For the jet, grab the event info and the first LLP that is within 0.4 of the guy.
    jet_info = event_info \
        .SelectMany('lambda ev: ev[1].Select(lambda j1: (ev[0], j1, ev[2].Where(lambda tp2: DeltaR(tp2.eta(), tp2.phi(), j1.eta(), j1.phi()) < 0.4)))')

    # Build us a list of columns
    tc = track_columns()
    tc.add_col('RunNumber', 'ji[0].runNumber()')
    tc.add_col('EventNumber', 'ji[0].eventNumber()')

    tc.add_col('JetPt', 'ji[1].pt()/1000.0')
    tc.add_col('JetEta', 'ji[1].eta()')
    tc.add_col('JetPhi', 'ji[1].phi()')

    # If it is signal, we can add a bunch of extra info
    tc.add_col('IsLLP', 'ji[2].Count() > 0')
    tc.add_col('nLLPs_Near_Jets', 'ji[2].Count()')
    for c in ['x', 'y', 'z']:
        tc.add_col('L{0}'.format(c), f'0 if ji[2].Count() == 0 else abs(ji[2].First().prodVtx().{c}()-ji[2].First().decayVtx().{c}())')
    for c in ['eta', 'phi']:
        tc.add_col('L{0}'.format(c), f'0 if ji[2].Count() == 0 else ji[2].First().{c}()')


    # The basic moments for the layer weights.
    add_sampling_layer ('EMM_BL0', 0, tc)
    add_sampling_layer ('EMM_BL1', 1, tc)
    add_sampling_layer ('EMM_BL2', 2, tc)
    add_sampling_layer ('EMM_BL3', 3, tc)

    add_sampling_layer ('EMM_EL0', 4, tc)
    add_sampling_layer ('EMM_EL1', 5, tc)
    add_sampling_layer ('EMM_EL2', 6, tc)
    add_sampling_layer ('EMM_EL3', 7, tc)

    add_sampling_layer ('EH_EL0', 8, tc)
    add_sampling_layer ('EH_EL1', 9, tc)
    add_sampling_layer ('EH_EL2', 10, tc)
    add_sampling_layer ('EH_EL3', 11, tc)

    add_sampling_layer ('EH_CBL0', 12, tc)
    add_sampling_layer ('EH_CBL1', 13, tc)
    add_sampling_layer ('EH_CVL2', 14, tc)

    add_sampling_layer ('EH_TGL0', 15, tc)
    add_sampling_layer ('EH_TGL1', 16, tc)
    add_sampling_layer ('EH_TGL2', 17, tc)

    add_sampling_layer ('EH_EBL0', 18, tc)
    add_sampling_layer ('EH_EBL1', 19, tc)
    add_sampling_layer ('EH_EBL2', 20, tc)

    add_sampling_layer ('FC_L0', 21, tc)
    add_sampling_layer ('FC_L1', 22, tc)
    add_sampling_layer ('FC_L2', 23, tc)

    # The MC information for the particle
    # TODO: Add the mc information

    # Most of the mlp stuff is going to come from a bunch of jet moments.
    # TODO: Add cut on clean LLP jet
    tuple_data = jet_info \
        .Select('lambda ji: ' + tc.gen_tuple()) \
        .Where('lambda jc: (jc[{0}] > 40.0) and (abs(jc[{1}]) < 2.5)'.format(tc.col_index('JetPt'), tc.col_index('JetEta')))

    # Put it all together and turn it into a set of ROOT files (for now):
    ds = await tuple_data \
        .AsPandasDF(tc.col_names) \
        .future_value(executor=lambda a: use_exe_func_adl_server(a))

    return ds