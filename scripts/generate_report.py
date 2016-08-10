#!/usr/bin/env python2.7

import argparse
import os
import sys


class Averager(object):
    def __init__(self):
        self.sum = 0
        self.count = 0

    def add_value(self, value):
        self.sum += value
        self.count += 1

    def get_mean(self):
        return self.sum / float(self.count)

    def get_variance(self):
        mean = self.get_mean()


def run(input, output):
    with open(input, 'r') as f:
        num_samples = 0
        while True:
            line = f.readline()
            if num_samples == 0 and line.startswith('Thread'):
                # skip the header line
                continue
            (thread, run, test, start_time, test_time, errors, response_code,
             response_length, response_errors, time_to_resolve_host,
             time_to_establish_connection, time_to_first_byte,
             new_connections) = line.split(',')
            num_samples += 1
    pass

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Convert Grinder data logs to html files')
    parser.add_argument('input', default=None, type=str, required=False)
    parser.add_argument('output', default=None, type=str, required=False)
    args = parser.parse_args()
    input = args.input
    if not input:
        folder = 'resources/logs'
        print('Searching for input files in the "{}" folder...'.format(folder))
        contents = os.listdir(folder)
        for item in contents:
            if os.path.isdir(item):
                continue
            if item.lower().endswith('data.log'):
                print('Selected "{}"'.format(item))
                input = item
                break
        else:
            sys.exit('No suitable input file found.')
    output = args.output
    if not output:
        output = input + '.html'

    run(input, output)




