#!/usr/bin/env python
# -*- coding: utf-8 -*-

from config import get_config
from fabric.api import env
from fabric.api import put
from fabric.api import roles
from fabric.api import run
from fabric.api import sudo
from fabric.context_managers import show
from fabric.state import connections
from fabric.tasks import execute
from helpers import get_abs_path
from helpers import is_fabricable
import logging
from multiprocessing import Pool
import os
from providers.amazon import get_master_ip_address
from providers.amazon import get_master_reservations
from providers.amazon import get_slave_reservations
from providers.amazon import create_master
from providers.amazon import create_slave
from providers.amazon import update_master_security_group
import time

__all__ = [
    'swarm_down_master',
    'swarm_down_slaves',
    'swarm_down',
    'swarm_up_master',
    'swarm_up_slaves',
    'swarm_up'
]

pool = Pool(processes=4)


def swarm_down_master(args):
    _swarm_down_impl(args, 'master', get_master_reservations)


def swarm_down_slaves(args):
    _swarm_down_impl(args, 'slaves', get_slave_reservations)


def swarm_down(args):
    swarm_down_master(args)
    swarm_down_slaves(args)


def _swarm_down_impl(args, role_name, get_reservation_func):
    logging.info("Bringing down the swarm {0}".format(role_name))
    cfg = get_config(args.config)
    reservations = get_reservation_func(cfg)
    if reservations:
        for reservation in reservations:
            for instance in reservation.instances:
                instance.terminate()


def swarm_up_master(args):
    logging.info("Bringing up the swarm master")

    _validate_dirs(args)

    cfg = get_config(args.config)
    create_master(cfg)

    _update_role_defs(get_master_reservations(cfg), 'master')
    env.user = cfg.get('fabric', 'user', None)
    env.key_filename = cfg.get('fabric', 'key_filename', None)

    is_fabricable(env.roledefs['master'][0])

    execute(_bootstrap_master, args.directory)
    _disconnect_fabric()


def swarm_up_slaves(args):
    logging.info("Bringing up {0} swarm slaves".format(args.num_slaves))

    _validate_dirs(args)

    cfg = get_config(args.config)

    master_ip_address = get_master_ip_address(cfg)

    if not master_ip_address:
        raise Exception("Unable to start slaves without a master. Please "
                        "bring up a master first.")

    for i in xrange(args.num_slaves):
        pool.apply_async(create_slave, (cfg,))

    pool.close()
    pool.join()

    
    update_master_security_group(cfg)

    # TODO: Hack for now, should check for ssh-ability
    time.sleep(15)

    slave_reservations = get_slave_reservations(cfg)

    _create_host_id_mapping(slave_reservations)
    print slave_reservations
    print 'Host mapping created'
    print env.host_id_mapping

    _update_role_defs(slave_reservations, 'slave')
    env.user = cfg.get('fabric', 'user', None)
    env.key_filename = cfg.get('fabric', 'key_filename', None)
    env.parallel = True

    execute(_bootstrap_slave, args.directory, master_ip_address)
    _disconnect_fabric()


def swarm_up(args):
    swarm_up_master(args)
    swarm_up_slaves(args)


@roles('master')
def _bootstrap_master(bootstrap_dir_path):
    abs_bootstrap_dir_path = get_abs_path(bootstrap_dir_path)
    _bootstrap(abs_bootstrap_dir_path)

    dir_name = os.path.basename(abs_bootstrap_dir_path)
    run("nohup locust -f /tmp/locust/{0}/locustfile.py \
        --master >~/locust-log.txt 2>&1 < /dev/null &".format(dir_name), pty=False)


@roles('slave')
def _bootstrap_slave(bootstrap_dir_path, master_ip_address):
    abs_bootstrap_dir_path = get_abs_path(bootstrap_dir_path)

    print 'Slave sleeping'
    time.sleep(60)
    _bootstrap(abs_bootstrap_dir_path)

    dir_name = os.path.basename(abs_bootstrap_dir_path)
    run('echo "{0}" > /tmp/locust/{1}/host_id_mapping.txt'.format(env.host_id_mapping, dir_name))

    run("nohup locust -f /tmp/locust/{0}/locustfile.py --slave \
        --master-host={1} >~/locust-log.txt 2>&1 < /dev/null &".
        format(dir_name, master_ip_address), pty=False)


def _bootstrap(abs_bootstrap_dir_path):
    dir_name = os.path.basename(abs_bootstrap_dir_path)
    
    print 'SETTING UP!!!!!!!!!!!!!!!!!!!!!!!!!'
    with show('debug'):
        print 'Making directory'
        run('mkdir -p /tmp/locust/')
        print 'Transferring directory'
        put(abs_bootstrap_dir_path, '/tmp/locust/')
        print 'Chmodding'
        sudo("chmod +x /tmp/locust/{0}/bootstrap.sh".format(dir_name))
        print 'Calling bootstrap'
        sudo("/tmp/locust/{0}/bootstrap.sh".format(dir_name))


def _create_host_id_mapping(reservations):
    """Creates the mapping from host IP address to the data ID they should run."""
    host_id_mapping = {}
    next_id = 0
    if reservations:
        for reservation in reservations:
            for instance in reservation.instances:
                if instance.private_ip_address:
                    host_id_mapping[instance.private_ip_address] = str(next_id)
                    next_id += 1
    env.host_id_mapping = ','.join(['|'.join(node) for node in host_id_mapping.items()])


# TODO: Shouldn't leak these calls through
def _update_role_defs(reservations, role_key):
    env.roledefs[role_key] = []
    if reservations:
        for reservation in reservations:
            for instance in reservation.instances:
                if instance.ip_address:
                    env.roledefs[role_key].append(instance.ip_address)


def _disconnect_fabric():
    for key in connections.keys():
        connections[key].close()
        del connections[key]


def _validate_dirs(args):
    logging.info("Validating proper directories are present")

    bootstrap_dir_path = get_abs_path(args.directory)

    if os.path.exists(bootstrap_dir_path):
        bootstrap_file = os.path.join(bootstrap_dir_path, 'bootstrap.sh')
        if os.path.exists(bootstrap_file):
            locustfile = os.path.join(bootstrap_dir_path, 'locustfile.py')
            if os.path.exists(locustfile):
                logging.info("All proper directories present")
                return

    raise Exception("Unable to validate bootstrap.sh and locustfile.py are "
                    "present in the directory {0}".format(bootstrap_dir_path))


# vim: filetype=python
