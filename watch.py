#!/usr/bin/env python3

import click
import yaml
import subprocess
import time
import datetime
import sys
import os
import asyncio
import sqlite3
import logging


bin_ping = 'ping'      # the program to run in order to ping the hosts - it has to return 0 when ping is OK
min_points = 6         # how many bad results in a row are needed to trigger
retention_time = 36000 # how long to keep the data in SQLite


def lima(t):
  return datetime.datetime.fromtimestamp(t).strftime('%Y-%m-%d %H:%M:%S')

async def ping(host):
  logging.debug("Ping host %s" % host)
  try:
    proc = await asyncio.create_subprocess_exec(bin_ping, "-c", "1", host,
      stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()

    if proc.returncode == 0:
      logging.debug(f"host {host}... ok, stdout:\n{stdout.decode()}\nstderr:{stderr.decode()}")
      return True
    else:
      logging.debug(f"host {host}... FAILED, stdout:\n{stdout.decode()}\nstderr:{stderr.decode()}")
      return False
  except Exception as e:
    logging.debug(f"host {host}... FAILED, exception: {e}")
  return False

async def pingtest(host, retrytime=1):
  for i in [0,1,2,3,4]:
    if i:
      logging.debug("Retry #%d after %d sec" % (i, i*retrytime))
      await asyncio.sleep(i*retrytime)
    if await ping(host):
      return True
  return False


async def test(hostname):
  return (hostname, await pingtest(hostname))



async def doexec(command):
  logging.debug("Running shell command: %s" % command)
  try:
    proc = await asyncio.create_subprocess_shell(command,
      stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE)
    stdout, stderr = await proc.communicate()
    logging.debug(f"Finish command {command} retcode:{proc.returncode}, stdout:\n{stdout.decode()}\nstderr:{stderr.decode()}")
  except Exception as e:
    logging.debug(f"Command {command} FAILED, exception: {e}")
    raise


async def sendmail(email, subj, body):
    logging.debug("Sending notify subj=%s body=%s to %s" % (subj, body, email))

    bbody = body.encode()
    subprocess.run(["mail", "-s '%s'" % subj, email], input=bbody)
    #subprocess.run(["echo", "-s '%s'" % subj, email], input=bbody)


def check_failseries(ser, expect=0):
  for v in ser:
    if v == expect:
      pass
    else:
      return False
  return True


async def hosts_process(cfg, sqlitefile):
  t = time.time()
  conn = sqlite3.connect(sqlitefile)
  conn.execute('''CREATE TABLE IF NOT EXISTS notifies
             (timestamp int, host text, laststate int)''')

  for h in cfg.get('hosts', {}):
    if 'notify' in h:
      score = [r for _,r in conn.execute('SELECT timestamp, result FROM tests WHERE host = ? AND timestamp >= (? - ?) ORDER BY timestamp DESC', (h['hostname'],int(t), int(h.get('notify_delay', 3600))))]
      if len(score) >= min_points: # test if the results are relevant
        if check_failseries(score): # line of errors longer than minimum
          logging.debug(f"Notify trigger hit for {h['hostname']} with {len(score)} data points.")
          laststate, lastts = next(conn.execute('SELECT laststate,max(timestamp) FROM notifies WHERE host = ?', (h['hostname'],)))
          if laststate == 1 or laststate == None:
            logging.debug(f"Sending notifies for {h['hostname']}")
            for n in h.get('notify', []):
              if not lastts:
                lastts = 0
              await sendmail(n, f"Host {h['hostname']} is down", f"Host {h['hostname']} is down at {lima(t)}. Previous status change were at {lima(lastts)}.")
            conn.execute("INSERT INTO notifies VALUES (?,?,0)", (int(t), h['hostname']))
            conn.commit()
          else:
            logging.debug(f"Not sending notify: Notify for {h['hostname']} has been already sent.")
        else:
          laststate, _ = next(conn.execute('SELECT laststate,max(timestamp) FROM notifies WHERE host = ?', (h['hostname'],)))
          if laststate == 0:
            logging.debug(f"Reseting notify for {h['hostname']}. Host up.")
            conn.execute("INSERT INTO notifies VALUES (?,?,1)", (int(t), h['hostname']))
            conn.commit()
      else: # not enough data points
        logging.debug(f"Not enough data points for {h['hostname']}. Skip.")

  conn.close()


async def conn_process(cfg, sqlitefile):
  failed = True
  cfgcon = cfg.get('connectivity', {})
  if cfgcon:
    t = time.time()
    conn = sqlite3.connect(sqlitefile)

    for h in cfgcon.get('hostnames', []):
      score = [r for _,r in conn.execute('SELECT timestamp, result FROM tests WHERE host = ? AND timestamp >= (? - ?) ORDER BY timestamp DESC', (h, int(t), int(cfgcon.get('exec_delay', 1800))))]
      if len(score) >= min_points: # test if the results are relevant
        if check_failseries(score): # line of errors longer than minimum
          logging.debug(f"Connectivity down for {h}.")
        else:
          logging.debug(f"Connectivity works for {h}. Will not exec.")
          failed = False
      else:
        logging.debug(f"Not enough data points ({len(score)}) for conn test on {h}. Will not exec.")
        failed = False

    # we go past this point if all checks failed and we have enough values
    if failed and 'exec' in cfgcon:
      logging.warning(f"Connectivity check failed, going to exec the command.")
      await doexec(cfgcon['exec'])
    else:
      logging.warning(f"Connectivity check OK, will not exec.")

    conn.close()


async def cleanup(sqlitefile):
  t = time.time()
  conn = sqlite3.connect(sqlitefile)

  conn.execute('DELETE FROM tests WHERE timestamp < (? + ?)', (int(retention_time), int(t)))

  conn.close()


async def add_results(rlst, sqlitefile='data.sql'):
  def norm_res(r):
    if r: # True -> 1
      return 1
    else:
      return 0

  t = time.time()

  conn = sqlite3.connect(sqlitefile)
  conn.execute('''CREATE TABLE IF NOT EXISTS tests
             (timestamp int, host text, result int)''')

  conn.executemany("INSERT INTO tests VALUES (?,?,?)", [(int(t), h, norm_res(r)) for h,r in rlst])

  conn.commit()
  conn.close()


async def asyncmain(cfg, datafilename):
  host_tasks = [asyncio.create_task(test(h['hostname'])) for h in cfg.get('hosts', [])]
  conn_tasks = [asyncio.create_task(test(h)) for h in cfg.get('connectivity',{}).get('hostnames', [])]

  host_res = await asyncio.gather(*host_tasks)
  conn_res = await asyncio.gather(*conn_tasks)

  t = time.time()

#  for hdef, hres in zip(cfg.get('hosts', []), host_res):
#    print(f'{str(hdef)} : {str(hres)}')

#  for cres in conn_res:
#    print(f'{str(cres)}')

  await add_results(host_res, datafilename)
  await add_results(conn_res, datafilename)
  
  await hosts_process(cfg, datafilename)
  await conn_process(cfg, datafilename)

  await cleanup(datafilename)


def printlogs(datafilename):
  conn = sqlite3.connect(datafilename)
  print("Last checks:")
  for h,ts,r in conn.execute("SELECT host,max(timestamp),result FROM tests GROUP BY host"):
    print(f"{h} at {ts} ({lima(ts)}) was {'OK' if r else 'fail'}")

  print("Last notifies:")
  for h,ts,ls in conn.execute("SELECT host,max(timestamp),laststate FROM notifies GROUP BY host"):
    print(f"{h} at {ts} ({lima(ts)}) was {'reset to OK' if ls else 'failed'}")


  conn.close()

 

@click.command()
@click.option('-d', '--debug', 'dbg', is_flag=True)
@click.option('-p', '--printstat', 'printstat', is_flag=True)
@click.argument('config', type=click.File('r'), default="cfg.yml")
@click.argument('data', type=click.Path(), default="data.sql")
def main(dbg, printstat, config, data):
  if dbg:
    logging.basicConfig(level=logging.DEBUG)

  if printstat:
    printlogs(data)
    return

  cfg = yaml.load(config, Loader=yaml.Loader)

  asyncio.run(asyncmain(cfg, data))


if __name__ == '__main__':
  main()
