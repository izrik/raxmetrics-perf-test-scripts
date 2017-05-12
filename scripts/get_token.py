
import argparse
import os.environ

parser = argparse.ArgumentParser()
parser.add_argument('username')
parser.add_argument('password', default=os.environ.get('BF_PASSWORD'))
parser.add_argument('--auth-url')
args = parser.parse_args()

print('username: {}'.format(args.username))
print('password: {}'.format(args.password))
