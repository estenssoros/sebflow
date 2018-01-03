#!/usr/bin env python
# -*- coding: utf-8 -*-
import sys
import_dir = '/Users/estenssoros/sebflow'
if import_dir not in sys.path:
    sys.path.append(import_dir)
from sebflow.bin.cli import CLIFactory

if __name__ == '__main__':
    parser = CLIFactory.get_parser()
    args = parser.parse_args()
    args.func(args)
