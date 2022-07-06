from rucio.client.client import Client
import pandas as pd
import argparse

client = Client()
client.whoami()

class ArgumentParser():
    def __init__(self):
        self.parser = argparse.ArgumentParser(prog='get_stuck_files')
        self.parser.add_argument('--id', help='specify rule id', required=True, dest='rule_id')
        self.parser.add_argument('--file_name', help='location to save the txt file', default='stuck_files.txt', dest='file_name')
        
def get_stuck_files(args):
    rule_id = args.rule_id
    temp = client.get_replication_rule(rule_id)
    df = pd.DataFrame(client.list_content(temp['scope'], temp['name']))
    final = pd.DataFrame()
    for i in range(len(df['scope'])):
        if df['name'].str.endswith('.file0001')[i]:
            final = pd.concat([final, df['name']], ignore_index=True)
        else:
            temp = pd.DataFrame(client.list_dataset_replicas(df['scope'][i], df['name'][i]))
            temp = temp.loc[temp['state'] != 'AVAILABLE']
            if len(temp != 0):
                final = pd.concat([final, temp['name']], ignore_index=True)
    final.to_csv(args.file_name, header=None, index=None, sep=' ')
    
if __name__ == '__main__':
    optmgr = ArgumentParser()
    args = optmgr.parser.parse_args()
    get_stuck_files(args)