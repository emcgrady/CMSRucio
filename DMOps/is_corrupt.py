from __future__ import division, print_function
from rucio.client.client import Client
import pandas as pd
import gfal2
import argparse
import subprocess
import os

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

    for i in df.index:
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
                diag['reason'].append('replica checksum mismatch at RSE')
        except:
            print('File not found on ' + df['RSE'][i])
            diag['diag'].append('replica missing')
            diag['reason'].append('replica not found')
            
        
    df['diag'] = diag['diag']
    df['reasoning'] = diag['reason']
    
    for i in df.loc[df['diag'] == 'none'].index:
        file = df['replica'][i]
        os.system('gfal-copy ' + file + ' /tmp/temp.root')
        test = subprocess.check_output('gfal-sum /tmp/temp.root adler32', shell=True)
        if str(test).split()[1][:8] == checksum:
            print('Replica from ' + df['RSE'][i] + ' is not corrupt')
        else:
            print('Replica from ' + df['RSE'][i] + ' is corrupt!')
            df['diag'][i] = 'replica corrupt'
            df['reason'][i] = 'file checksum mismatch after local copy'
        os.system('rm /tmp/temp.root')

    print(df)

if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    is_corrupt(args)