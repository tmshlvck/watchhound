#!/usr/bin/env python3

import click
import yaml
import subprocess
import time
import datetime
import sys
import os

debug = False
#debug = True
retrytime = 2

def d(msg):
    if debug:
        print(msg, file=sys.stderr)

def lima(t):
    return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')

def ping(host):
    d("Ping host %s" % host)
    try:
        subprocess.check_output(["ping", "-c", "1", host])
        d("... ok")
        return True                      
    except:
        pass
    d("... FAILED")
    return False

def testping(host):
    for i in [0,1,2,3,4]:
        if i:
            d("Retry #%d after %d sec" % (i, i*retrytime))
            time.sleep(i*retrytime)
        if ping(host):
            return True
    return False

def notify(email, subj, body):
    d("Sending notify subj=%s body=%s to %s" % (subj, body, email))

    bbody = body.encode()
    subprocess.run(["mail", "-s '%s'" % subj, email], input=bbody)


def exec(command):
    d("Running shell command: %s" % command)
    os.system(command)


def is_scheduled(hdata, delay, lastaction, t):
    return ((int(hdata['lastchange'])+delay) < t and lastaction < int(hdata['lastchange']))

def handle_down_notifies(hconf, hdata, t):
    ntf = hconf.get('notify', [])
    delay = hconf.get('notify_delay', 0)
    last = int(hdata.get('lastnotify', 0))
    
    if is_scheduled(hdata, delay, last, t):
        for n in ntf:
            notify(n, 'host % is down for %d seconds' % (hconf['hostname'], delay), str(dta[hn]))
        hdata['lastnotify'] = t
        hdata['lastnotify_lima'] = lima(t)

def handle_hostdown(hconf, hdata, t):
    exe = hconf.get('exec', None)
    delay = int(hconf.get('exec_delay', 0))
    last = int(hdata.get('lastexec', 0))

    d("Consider exec=%s lastchange=%d, delay=%d lastexec=%d" % (str(exe), int(hdata['lastchange']), delay, last))
    if exe and is_scheduled(hdata, delay, last, t):
        exec(exe)
        hdata['lastexec'] = t
        hdata['lastexec_lima'] = lima(t)
    else:
        d("... no execution.")

    handle_down_notifies(hconf, hdata, t)


def handle_hostup(hconf, hdata, t):
    pass


@click.command()
@click.option('-d', '--debug', 'debugparam', is_flag=True)
@click.argument('config', type=click.File('r'))
@click.argument('datafile', type=click.Path())
def main(debugparam, config, datafile):
    """
    config:::
    ---
    hosts:
      - hostname: mamut.d.taaa.eu
      - hostname: mamut.d.taaa.eu
      - hostname: chapadlo
        notify_down:
          - tmshlvck@gmail.com
      - hostname: lkko
        notify_all:
          - tmshlvck@gmail.com
        notify_delay: 600
        exec: "nohup reboot"
        exec_delay: 3600
    """
    global debug
    if debugparam:
        debug=True

    cfg = yaml.load(config, Loader=yaml.Loader)
    try:
        with open(datafile, 'r') as dfh:
            dta = yaml.load(dfh, Loader=yaml.Loader)
    except:
        dta = None

    if not dta:
        dta = {}

    d("Startup done. Working on %d hosts" % len(cfg['hosts']))
    for h in cfg['hosts']:
        hn = h['hostname']
        p = testping(hn)
        t = time.time()

        if not hn in dta:
            dta[hn] = {}

        change = (p != dta[hn].get('laststate', False))
        if change:
            dta[hn]['lastchange'] = t
            dta[hn]['lastchange_lima'] = lima(t)

        dta[hn]['laststate'] = p
        dta[hn]['lastcheck'] = t
        dta[hn]['lastcheck_lima'] = lima(t)

        if not p:
            handle_hostdown(h, dta[hn], t)
        else:
            handle_hostup(h, dta[hn], t)

    d("Save results...")
    with open(datafile, 'w') as dfh:
        dfh.write(yaml.dump(dta, Dumper=yaml.Dumper))

    return 0


if __name__ == '__main__':
    main()

