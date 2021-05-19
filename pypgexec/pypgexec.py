#!/usr/bin/python3
# -*- coding: utf-8 -*-

import os
import argparse
import logging
import psycopg2
from configparser import ConfigParser

BASE_DIR = os.path.abspath(os.path.dirname(__file__))

about = {}
with open(os.path.join(BASE_DIR, '__version__.py')) as f:
    exec(f.read(), about)

logging.basicConfig(filename='pypgexec.log',
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                    level=logging.DEBUG)
logger = logging.getLogger(__name__)

# create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)


def parse_args():
    __version__ = about['__version__']

    parser = argparse.ArgumentParser(prog=about['__title__'], description=about['__description__'])

    parser.add_argument('-v',
                        '--version',
                        action='version',
                        version=f'%(prog)s {__version__}',)
    parser.add_argument('-a',
                        '--author',
                        action='version',
                        version='%(prog)s was created by software developer Alexis Torres Valdes <alexis89.dev@gmail.com>',
                        help="show program's author and exit")

    parser.add_argument('--config',
                        required=True,
                        help='Database configuration file')
    parser.add_argument('--script',
                        required=True,
                        help='Script to execute')
    parser.add_argument('-a',
                        '--atomic',
                        action='store_true',
                        help='Atomic operation. Significantly improves performance and ensures that all queries are performed.')

    return parser.parse_args()


def config(filename, section):
    if not os.path.exists(filename):
        raise ValueError('File does not exist')

    parser = ConfigParser()
    parser.read(filename)

    if not parser.has_section(section):
        raise Exception(f'Section {section} not found in file {filename}')

    cfg = {}
    params = parser.items(section)
    for param in params:
        cfg[param[0]] = param[1]

    return cfg


def pg_exec(params: dict, queries: tuple, atomic=False):
    conn = None
    try:
        conn = psycopg2.connect(**params)
        logger.info('Connection created')

        cur = conn.cursor()
        for query in queries:
            try:
                cur.execute(query)
                if not atomic:
                    conn.commit()
                logger.info(f'{query} OK')
            except (Exception, psycopg2.DatabaseError) as err:
                logger.error(f'{query} - {err}')
                continue
        cur.close()
        if atomic:
            conn.commit()
    except (Exception, psycopg2.DatabaseError) as err:
        logger.error(err)
    finally:
        if conn is not None:
            conn.close()
            logger.info('Connection closed')


def main():
    # args = parse_args()
    # config_arg = args.config
    # script_arg = args.script
    # atomic_arg = args.atomic

    config_arg = '/home/dev/PycharmProjects/pypgexec/pypgexec.conf'
    script_arg = '/home/dev/PycharmProjects/pypgexec/script.sql'
    atomic_arg = True

    params = config(config_arg, 'postgresql')
    with open(script_arg) as fr:
        uncomment = filter(lambda ln: not ln.startswith('#'), fr.readlines())
        no_line_breaks = map(lambda ln: ln.replace('\n', ''), uncomment)
        queries = tuple(no_line_breaks)

    pg_exec(params, queries, atomic_arg)


if __name__ == '__main__':
    main()
