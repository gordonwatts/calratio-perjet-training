# Change a dump from rucio into a csv file that contains hss info for our samples
import sys
import re


def parse (file_name):
    'Parse data in file_name'

    with open(file_name, 'r') as f_in:
        # Print out csv header:
        print ('mH,mS,Lifetime,MCCampaign,RucioDSName')

        # Read in the file
        for line in f_in:
            if line[0] == '#':
                continue
            line = line.strip()
            if len(line) == 0:
                continue
            info = line.split(' ')

            # Strip of the scope. Not needed when talking to rucio later on.
            full_ds_name = info[1]
            i_colon = full_ds_name.index(':')
            if i_colon < 0:
                raise BaseException(f'Dataset {full_ds_name} has no rucio scope!')
            ds_name = full_ds_name[i_colon+1:]

            # Get the container name from that
            i_tid = ds_name.index('_tid')
            container_name = ds_name[:i_tid]

            # Figure out what MC campaign it is from.
            campaign = ''
            if 'r9364' in container_name:
                campaign='mc16a'
            elif 'r10201' in container_name:
                campaign='mc16d'
            elif 'r10724' in container_name:
                campaign='mc16e'
            else:
                raise BaseException(f'Cannot figure out what camptain {container_name} is in.')

            # Extract the masses and lifetimes.
            f = re.compile('_mH([0-9]+)_mS([0-9]+)(_[^.]+)?\\.')
            m = f.search(container_name)                

            if m:
                lt = 0
                if m[3] == '_ltlow':
                    lt = 5
                elif m[3] == '_lthigh':
                    lt = 9
                elif m[3] == '_lt5m':
                    lt = 5
                elif m[3] is None:
                    lt = 5
                else:
                    raise BaseException(f'Unable to determine lifetime for dataset {container_name}')

                print (f'{m[1]},{m[2]},{lt},{campaign},{container_name}')
            else:
                raise BaseException(f'Unable to extract masses and lifetime from {container_name}')

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print ('Usage: python rucio_dump_parser.py <rucio_data_filename>')
        exit(1)

    parse(sys.argv[1])
