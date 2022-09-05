from __future__ import division, print_function
from rucio.client.client import Client
import pandas as pd
import gfal2
import argparse

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='is_corrupt')
        self.parser.add_argument('--filename', '-f', help="specify filename as <scope:file>", action='store', dest='filename')
        
def is_corrupt(args):
    client = Client()
    dids=[]
    scope, name = args.filename.split(':')
    dids.append({'scope':scope,'name':name})
    
    df = pd.DataFrame(client.list_replicas(dids, all_states=True))
    checksum = df['adler32'][0]
    
    rse = []
    state = []
    
    for i, replica  in enumerate(list(df['pfns'][0].keys())):
        rse.append(df['pfns'][0][replica]['rse'])
        state.append(df['states'][0][rse[i]])
        
    dit = {'replica':list(df['pfns'][0].keys()),
           'RSE': rse,
           'state': state,
          }
    
    df = pd.DataFrame(dit)
    
    ctxt = gfal2.creat_context()
    
    diag = {'diag': [], 'reason':[]}

    for i in range(len(df)):
        try:
            ctxt.checksum(df['replica'][i],'adler32')
            print('Replica found on ' +  df['RSE'][i]) 
            print('Check sum: ' + ctxt.checksum(df['replica'][i],'adler32'))
            if checksum == ctxt.checksum(df['replica'][i],'adler32'):
                print('Checksum is correct!')
                diag['diag'].append('none')
                diag['reason'].append('replica is ok')
            else:
                print('ERROR! Checksum incorrect!')
                print('Expected ' + str(checksum) + ' and got ' + str(ctxt.checksum(df['replica'][i],'adler32')))
                diag['diag'].append('replica corrupt')
                diag['reason'].append('replica checksum mismatch')
        except:
            print('File not found on ' + df['RSE'][i])
            diag['diag'].append('replica missing')
            diag['reason'].append('replica not found')
            
        
    df['diag'] = diag['diag']
    df['reasoning'] = diag['reason']
    
    print(df)

if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    is_corrupt(args)