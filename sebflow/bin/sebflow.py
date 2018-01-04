#!/usr/bin/env python
# -*- coding: utf-8 -*-

from cli import CLIFactory


def entrypoint():
    parser = CLIFactory.get_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == '__main__':
    entrypoint()
