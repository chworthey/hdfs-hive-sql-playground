"""
Copyright 2021 Patrick S. Worthey
Orchestrates a hadoop + Hive + SQL cluster of docker nodes
"""
import argparse
import collections
import distutils.dir_util
import json
import os
import re
import shutil
import subprocess
import sys
import time

# PyPI installed modules...
import requests

# The root directory of the playground repository
ROOT_DIR = os.path.dirname(os.path.realpath(__file__))

# The hadoop distribution path on the docker nodes
HADOOP_HOME = '/himage/hadoop-3.3.0'

# The hive distribution path on the docker nodes
HIVE_HOME = '/himage/apache-hive-3.1.2-bin'

# The sqoop distribution path on the docker nodes
SQOOP_HOME = '/himage/sqoop-1.4.7.bin__hadoop-2.6.0'

# The path of the docker-compose.yml file
COMPOSE_FILE = os.path.join(ROOT_DIR, 'docker-compose.yml')

# The non-secured sql password used on the sql node
SQL_TEST_PASSWORD = 'myStrong(*)Password'

# The number of data nodes in the cluster (this variable only affects health checks)
NUM_DATA_NODES = 1

# The number of node manager nodes in the cluster (this variable only affects health checks)
NUM_NODE_MANAGERS = 1

# The minimum amount of disk space each node requires to operate (applicable in health checks)
MIN_DISK_SPACE = 8589934592 # 1GB

# Exposed localhost ports for each of the nodes
PORT_UI_NN1    = 3000
PORT_UI_DN1    = 3001
PORT_UI_RMAN   = 3002
PORT_UI_NM1    = 3003
PORT_UI_MRHIST = 3004
PORT_UI_HS     = 3005
PORT_SQL_SQL   = 3006

# Descriptions of what each port does
PORT_DOC = [
  (PORT_UI_NN1,    'http', 'Web UI for the primary name node'),
  (PORT_UI_DN1,    'http', 'Web UI for data node 1'),
  (PORT_UI_RMAN,   'http', 'Web UI for YARN resource manager'),
  (PORT_UI_NM1,    'http', 'Web UI for node manager 1'),
  (PORT_UI_MRHIST, 'http', 'Web UI map reduce history server'),
  (PORT_UI_HS,     'http', 'Web UI for hive server'),
  (PORT_SQL_SQL,   'sql (tcp/ip)', 'SQL server connection port')
]

# A health checklist item description
NodeHealthBeanCheck = collections.namedtuple('NodeHealthBeanCheck', \
  'bean_name prop_name check_func')

# The status of a single node in the cluster
NodeHealthReport = collections.namedtuple('NodeHealthReport', \
  'is_healthy message')

# A summary of the status on each of the nodes in the cluster
HealthReportSummary = collections.namedtuple('HealthReportSummary', \
  'cluster_healthy nn1 dn1 rman nm1 mrhist hs client sql')

class Config:
  """
  Represents the configuration for any playground tasks
  """
  def __init__(self, project_name=None, source_dir=None, data_dir=None, volumes_dir=None):
    self.project_name = project_name
    self.source_dir = source_dir
    self.data_dir = data_dir
    self.volumes_dir = volumes_dir

  @property
  def project_name(self):
    """
    The project name used for the Docker-Compose project name
    """
    return self._project_name

  @project_name.setter
  def project_name(self, value):
    self._project_name = value

  @property
  def source_dir(self):
    """
    The local directory containing files to be uploaded to the client node /src directory upon 
    setup.
    """
    return self._source_dir

  @source_dir.setter
  def source_dir(self, value):
    if value:
      self._source_dir = os.path.abspath(value)
    else:
      self._source_dir = None

  @property
  def data_dir(self):
    """
    The local directory containing files to be ingested into HDFS upon setup.
    """
    return self._data_dir

  @data_dir.setter
  def data_dir(self, value):
    if value:
      self._data_dir = os.path.abspath(value)
    else:
      self._data_dir = None

  @property
  def volumes_dir(self):
    """
    The local directory (which may not yet exist) where docker will persist files between runs.
    """
    return self._volumes_dir

  @volumes_dir.setter
  def volumes_dir(self, value):
    if value:
      self._volumes_dir = os.path.abspath(value)
    else:
      self._volumes_dir = None

  def save(self, filename):
    """
    Saves the configuration to a file.
    """
    with open(filename, 'w') as _fp:
      json.dump({ \
        'project_name': self._project_name, \
        'source_dir': self._source_dir, \
        'data_dir': self._data_dir, \
        'volumes_dir': self._volumes_dir \
      }, _fp, indent=2)

  @staticmethod
  def load(filename):
    """
    Loads the configuration from a file.
    """
    with open(filename, 'r') as _fp:
      _c = Config()
      _j = json.load(_fp)
      _c.project_name = _j['project_name']
      _c.source_dir = _j['source_dir']
      _c.data_dir = _j['data_dir']
      _c.volumes_dir = _j['volumes_dir']
      return _c

def exec_docker(config, node_name, command, workdir=None, \
  interactive=False, detached=False, check=True):
  """
  Executes a command on a node through docker.
  """
  _args = ['docker', 'exec']
  if workdir:
    _args.append('-w')
    _args.append(workdir)
  if interactive:
    _args.append('-i')
    _args.append('-t')
  if detached:
    _args.append('-d')
  _args.append('%s_%s_1' % (config.project_name, node_name))
  split_spaces = True
  for _c in command.split('"'):
    if split_spaces:
      _splt = _c.split(' ')
      for _s in _splt:
        if _s:
          _args.append(_s)
    else:
      _args.append(_c)
    split_spaces = not split_spaces
  output = subprocess.run(_args, check=check, shell=True)
  return output.returncode


def build_img(config):
  """
  Builds or rebuilds the dockerfile images.
  """
  set_environment(config)
  os.system('docker-compose -p %s -f "%s" build' % (config.project_name, COMPOSE_FILE))

def format_hdfs(config):
  """
  Formats hdfs in the cluster.
  """
  exec_docker(config, 'nn1', '%s/bin/hdfs namenode -format -force clust' % (HADOOP_HOME))

def ingest_data(config):
  """
  Ingests data from the configured data volume into hdfs.
  """
  exec_docker(config, 'nn1', '%s/bin/hadoop fs -put /data /data' % (HADOOP_HOME))

def copy_source(config):
  """
  Copies from the configured local source directory to the source volume. 
  Use to update the client node's /src folder on a running cluster when new code is written.
  """
  if not os.path.exists(config.source_dir):
    print('Source directory does not exist. Please check configuration and try again.')
    return
  dir_name = os.path.join(config.volumes_dir, 'client')
  if not os.path.exists(dir_name):
    os.makedirs(dir_name)
  distutils.dir_util.copy_tree(config.source_dir, dir_name)
  print('Source files copied to volume.')

def setup_hive(config):
  """
  Makes required hdfs directories for hive to run and initializes the schema metastore.
  """
  fs_cmd = '%s/bin/hadoop fs ' % (HADOOP_HOME)
  exec_docker(config, 'nn1', fs_cmd + '-mkdir /tmp', check=False)
  exec_docker(config, 'nn1', fs_cmd + '-mkdir -p /user/hive/warehouse', check=False)
  exec_docker(config, 'nn1', fs_cmd + '-chmod g+w /tmp')
  exec_docker(config, 'nn1', fs_cmd + '-chmod g+w /user/hive/warehouse')
  exec_docker(config, 'hs', '%s/bin/schematool -dbType derby -initSchema' % \
    (HIVE_HOME), workdir='/metastore')

def cluster_up(config):
  """
  Boots the cluster up but does not run any of the daemons.
  """
  set_environment(config)
  os.system('docker-compose -p %s -f "%s" up -d' % (config.project_name, COMPOSE_FILE))

def start_hadoop_daemons(config):
  """
  Runs all daemons in the hadoop distribution on their respective nodes.
  """
  exec_docker(config, 'nn1', '%s/bin/hdfs --daemon start namenode' % (HADOOP_HOME))
  exec_docker(config, 'dn1', '%s/bin/hdfs --daemon start datanode' % (HADOOP_HOME))
  exec_docker(config, 'rman', '%s/bin/yarn --daemon start resourcemanager' % (HADOOP_HOME))
  exec_docker(config, 'nm1', '%s/bin/yarn --daemon start nodemanager' % (HADOOP_HOME))
  exec_docker(config, 'mrhist', '%s/bin/mapred --daemon start historyserver' % (HADOOP_HOME))

def start_hive_server(config):
  """
  Starts the hive server daemon.
  """
  exec_docker(config, 'hs', '%s/bin/hiveserver2' % (HIVE_HOME), \
    detached=True, workdir='/metastore')

def cluster_down(config):
  """
  Spins the cluster down.
  """
  set_environment(config)
  os.system('docker-compose -p %s -f "%s" down' % (config.project_name, COMPOSE_FILE))

def metric_request(port):
  """
  Sends an http request to a node's jmx endpoint. Returns the parsed json, or None on error.
  """
  try:
    _r = requests.get('http://localhost:%d/jmx' % (port))
  except:
    return None
  if _r.status_code != 200:
    return None
  try:
    return _r.json()
  except ValueError:
    return None

def find_bean_by_name(jsn, nme):
  """
  Extracts a bean of the given name from jmx metrics json object.
  """
  if 'beans' not in jsn:
    return None
  else:
    return next((b for b in jsn['beans'] if b['name'] == nme), None)

def extract_bean_prop(jsn, bean_name, propname):
  """
  Extracts a property of a bean of the given name from jmx metrics json object.
  """
  bean = find_bean_by_name(jsn, bean_name)
  if bean and propname in bean:
    return bean[propname]
  else:
    return None

def gen_node_report_from_checks(jsn, checks):
  """
  Creates a node health report using the jmx metrics json and a list of type NodeHealthBeanCheck
  """
  healthy = True
  messages = []
  for _c in checks:
    prop = extract_bean_prop(jsn, _c.bean_name, _c.prop_name)
    if prop is not None:
      report = _c.check_func(prop)
      prefix = '\u2705 '
      if not report.is_healthy:
        healthy = False
        prefix = '\u274C '

      messages.append(prefix + report.message)
    else:
      healthy = False
      messages.append('\u274C Missing required bean property. Bean name: "%s", property: "%s"' % \
        (_c.bean_name, _c.prop_name))
  message = '\n'.join(messages)
  return NodeHealthReport(is_healthy=healthy, message=message)

def _check_func_disk_space(prop_val):
  """
  A check function for comparing the prop_val to the expected disk space amount.
  """
  return NodeHealthReport(is_healthy=True, message='Sufficient disk space.') \
    if prop_val >= MIN_DISK_SPACE else NodeHealthReport(is_healthy=False, message='Insufficient' \
      ' disk space. Minimum required disk space is %d. Remaining bytes: %d' % \
      (MIN_DISK_SPACE, prop_val))

def json_checker_namenode(jsn):
  """
  Checks the jmx metrics json for the namenode and returns a node health report
  """
  checks = [
    NodeHealthBeanCheck( \
      bean_name='Hadoop:service=NameNode,name=StartupProgress', \
      prop_name='PercentComplete', \
      check_func=lambda i: NodeHealthReport(is_healthy=True, message='Startup completed.') \
        if i == 1.0 else NodeHealthReport(is_healthy=False, message='Startup not complete.' \
          ' Progress: %%%f.' % (i * 100)) \
    ),
    NodeHealthBeanCheck( \
      bean_name='Hadoop:service=NameNode,name=FSNamesystem', \
      prop_name='tag.HAState', \
      check_func=lambda i: NodeHealthReport(is_healthy=True, message='Namenode active.') \
        if i == 'active' else NodeHealthReport(is_healthy=False, message='Namenode inactive.' \
          ' State: "%s"' % (i)) \
    ),
    NodeHealthBeanCheck( \
      bean_name='Hadoop:service=NameNode,name=FSNamesystem', \
      prop_name='MissingBlocks', \
      check_func=lambda i: NodeHealthReport(is_healthy=True, message='No missing blocks.') \
        if i == 0 else NodeHealthReport(is_healthy=False, message='One or more missing blocks.' \
          ' Data is missing. Blocks missing: %d.' % (i)) \
    ),
    NodeHealthBeanCheck( \
      bean_name='Hadoop:service=NameNode,name=FSNamesystem', \
      prop_name='CapacityRemaining', \
      check_func=_check_func_disk_space
    ),
    NodeHealthBeanCheck( \
      bean_name='Hadoop:service=NameNode,name=FSNamesystemState', \
      prop_name='NumLiveDataNodes', \
      check_func=lambda i: NodeHealthReport(is_healthy=True, message='All data nodes' \
        ' are connected.') \
        if i == 1 else NodeHealthReport(is_healthy=False, message='Some data nodes are not' \
          ' connected. Number of connected data nodes: %d/%d' % (i, NUM_DATA_NODES)) \
    ),
    NodeHealthBeanCheck( \
      bean_name='Hadoop:service=NameNode,name=FSNamesystemState', \
      prop_name='NumStaleDataNodes', \
      check_func=lambda i: NodeHealthReport(is_healthy=True, message='No stale data nodes.') \
        if i == 0 else NodeHealthReport(is_healthy=False, message='Some data nodes have not ' \
          'sent a heartbeat in some time. Number of stale data nodes: %d' % (i)) \
    )
  ]
  return gen_node_report_from_checks(jsn, checks)

def json_checker_datanode(jsn):
  """
  Checks the jmx metrics json for the datanode and returns a node health report
  """
  checks = [
    NodeHealthBeanCheck( \
      bean_name='Hadoop:service=DataNode,name=FSDatasetState', \
      prop_name='Remaining', \
      check_func=_check_func_disk_space \
    ),
    NodeHealthBeanCheck( \
      bean_name='Hadoop:service=DataNode,name=FSDatasetState', \
      prop_name='NumFailedVolumes', \
      check_func=lambda i: NodeHealthReport(is_healthy=True, message='No failed volumes.') \
        if i == 0 else NodeHealthReport(is_healthy=False, message='One or more volumes have' \
          ' failed. Number of failed volumes: %d' % (i)) \
    )
  ]
  return gen_node_report_from_checks(jsn, checks)

def json_checker_resourcemanager(jsn):
  """
  Checks the jmx metrics json for the resource manager node and returns a node health report
  """
  checks = [
    NodeHealthBeanCheck( \
      bean_name='Hadoop:service=ResourceManager,name=ClusterMetrics', \
      prop_name='NumActiveNMs', \
      check_func=lambda i: NodeHealthReport(is_healthy=True, message='All node managers' \
        ' connected.') \
        if i == 1 else NodeHealthReport(is_healthy=False, message='One or more node' \
          ' managers not connected. Number of connected node managers: %d/%d' % \
          (i, NUM_NODE_MANAGERS)) \
    ),
    NodeHealthBeanCheck( \
      bean_name='Hadoop:service=ResourceManager,name=ClusterMetrics', \
      prop_name='NumUnhealthyNMs', \
      check_func=lambda i: NodeHealthReport(is_healthy=True, message='All node managers' \
        ' are healthy.') \
        if i == 0 else NodeHealthReport(is_healthy=False, message='One or more node' \
        ' managers are unhealthy. Number of unhealthy node managers: %d' % (i)) \
    )
  ]
  return gen_node_report_from_checks(jsn, checks)

def json_checker_response_only(jsn):
  """
  Checks the jmx metrics json on any node that has jmx metrics but no other specific health checks
  """
  healthy = 'beans' in jsn and len(jsn['beans']) > 0
  if healthy:
    return NodeHealthReport(is_healthy=True, message='\u2705 Response has expected json.')
  else:
    return NodeHealthReport(is_healthy=False, message='\u274C Response does not' \
      ' have expected json.')


def gen_node_health_report(jsn, json_checker_func):
  """
  Generates a node health report using the json checker function
  """
  if jsn:
    return json_checker_func(jsn)
  else:
    message = '\u274C Could not fetch metrics from server. Most likely the node is down.'
    return NodeHealthReport(is_healthy=False, message=message)

def gen_docker_health_report(config, node_name):
  """
  Generates a health report simply based on if the given node is running or not.
  """
  return_code = exec_docker(config, node_name, 'bash -c exit 0', check=False)
  if return_code == 0:
    return NodeHealthReport(is_healthy=True, message='\u2705 Node running')
  else:
    return NodeHealthReport(is_healthy=False, message='\u274C Node not running')

def gen_health_summary(config):
  """
  Generates a health report summary on the running cluster.
  """
  _name_node1 = gen_node_health_report(metric_request(PORT_UI_NN1), json_checker_namenode)
  _data_node1 = gen_node_health_report(metric_request(PORT_UI_DN1), json_checker_datanode)
  _rman  = gen_node_health_report(metric_request(PORT_UI_RMAN), json_checker_resourcemanager)
  _nm1 = gen_node_health_report(metric_request(PORT_UI_NM1), json_checker_response_only)
  _mrhist = gen_node_health_report(metric_request(PORT_UI_MRHIST), json_checker_response_only)
  _hs = gen_node_health_report(metric_request(PORT_UI_HS), json_checker_response_only)
  _client = gen_docker_health_report(config, 'client')
  _sql = gen_docker_health_report(config, 'sql')
  _cluster_healthy = \
    _name_node1.is_healthy and \
    _data_node1.is_healthy and \
    _rman.is_healthy and \
    _nm1.is_healthy and \
    _mrhist.is_healthy and \
    _hs.is_healthy and \
    _client.is_healthy and \
    _sql.is_healthy

  summary = HealthReportSummary( \
    cluster_healthy=_cluster_healthy, \
    nn1=_name_node1, \
    dn1=_data_node1, \
    rman=_rman, \
    nm1=_nm1, \
    mrhist=_mrhist, \
    hs=_hs, \
    client=_client, \
    sql=_sql)
  return summary

def print_node_health(report):
  """
  Prints a node health report
  """
  if report is None:
    print('? Report not implemented.')
    print()
    return
  print('Overall Status:')
  print('\u2705 Healthy' if report.is_healthy else '\u274C Unhealthy')
  print('Checklist:')
  print(report.message)
  print()

def print_summary(summary):
  """
  Prints a summary health report
  """
  print('NAME NODE 1')
  print_node_health(summary.nn1)
  print('DATA NODE 1')
  print_node_health(summary.dn1)
  print('RESOURCE MANAGER')
  print_node_health(summary.rman)
  print('NODE MANAGER 1')
  print_node_health(summary.nm1)
  print('MAP REDUCE HISTORY SERVER')
  print_node_health(summary.mrhist)
  print('HIVE SERVER')
  print_node_health(summary.hs)
  print('CLIENT NODE')
  print_node_health(summary.client)
  print('SQL SERVER')
  print_node_health(summary.sql)
  print('OVERALL CLUSTER HEALTH')
  if summary.cluster_healthy:
    print('\u2705 Healthy')
  else:
    print('\u274C Unhealthy')

def print_health(config):
  """
  Prints the health of the cluster
  """
  print('Checking cluster health.')
  print()
  summary = gen_health_summary(config)
  print_summary(summary)

def wait_for_healthy_nodes_print(config, timeout):
  """
  Blocks until all nodes are healthy or until timeout, and prints the results.
  """
  _start = time.time()
  summary = wait_for_healthy_nodes(config, timeout=timeout)
  print('Wait completed in %fs. Summary:' % (time.time() - _start))
  print()
  print_summary(summary)

def get_summary_preview_str(summary):
  """
  Gets a oneliner string displaying the summarized cluster health
  """
  _s = [
    ('nn1', summary.nn1.is_healthy),
    ('dn1', summary.dn1.is_healthy),
    ('rman', summary.rman.is_healthy),
    ('nm1', summary.nm1.is_healthy),
    ('mrhist', summary.mrhist.is_healthy),
    ('hs', summary.hs.is_healthy),
    ('client', summary.client.is_healthy),
    ('sql', summary.sql.is_healthy)
  ]
  _s2 = map(lambda a : '%s %s' % \
    (('\u2705' if a[1] else '\u274C'), a[0]), _s)
  return ', '.join(_s2)

def wait_for_healthy_nodes(config, timeout=200, interval=5):
  """
  Blocks until all nodes are healthy or until timeout
  """
  _summary = None
  for _t in range(int(timeout / interval)):
    _summary = gen_health_summary(config)
    if _summary.cluster_healthy:
      return _summary
    else:
      print('...Waiting... ' + get_summary_preview_str(_summary))
      time.sleep(interval)
  return _summary

def setup(config):
  """
  One-time setup for the cluster.
  """
  print('Destroying volumes.')
  destroy_volumes(config)

  print('Spinning cluster up.')
  cluster_up(config)

  print('Formatting HDFS.')
  format_hdfs(config)

  print('Starting Hadoop Daemons.')
  start_hadoop_daemons(config)

  print('Setting up Hive server.')
  setup_hive(config)

  print('Ingesting configured data volume into HDFS (this could take some time).')
  ingest_data(config)

  print('Copying configured source folder to the client node volume.')
  copy_source(config)

  print('Spinning cluster down.')
  cluster_down(config)

def print_port_doc():
  """
  Prints documentation on the exposed ports.
  """
  print('Exposed ports on localhost:')
  for _p in PORT_DOC:
    print('Port: %s, Type: %s, Description: %s' % \
      (_p[0], _p[1], _p[2]))

def start(config, wait=True):
  """
  Boots up the cluster and starts all of the daemons on the cluster.
  """
  print('Spinning cluster up.')
  cluster_up(config)

  print('Starting Hadoop Daemons.')
  start_hadoop_daemons(config)

  print('Starting Hive Server.')
  start_hive_server(config)

  if wait:
    print('Starting wait routine.')
    wait_for_healthy_nodes_print(config, 200)

  print_port_doc()

def stop(config):
  """
  Spins down the cluster.
  """
  print('Spinning cluster down.')
  cluster_down(config)

def destroy_volumes(config):
  """
  Removes the persistant file storage of the cluster.
  """
  print('Spinning cluster down.')
  cluster_down(config)

  if not os.path.exists(config.volumes_dir):
    print('Volumes directory does not exist. Cannot delete.')
  else:
    print('Deleting files.')
    shutil.rmtree(config.volumes_dir)

def print_hadoop_node_logs(config, node_name):
  """
  Prints the logs of the given hadoop node.
  """
  exec_docker(config, node_name, 'cat %s/logs/*.log')

def beeline_cli(config):
  """
  Launches an interactive cli on the client node with beeline cli.
  """
  exec_docker(config, 'client', '%s/bin/beeline -u jdbc:hive2://hs:10000' % (HIVE_HOME), \
    workdir='/src', interactive=True)

def bash_cli(config, nodename):
  """
  Launches an interactive bash shell on the given node.
  """
  exec_docker(config, nodename, 'bash', interactive=True)

def sqlcmd_cli(config, local):
  """
  Launches an interactive sql cli on the client node or local host if specified. 
  """
  if local:
    os.system('sqlcmd -S tcp:localhost,%d -U sa -P %s' % (PORT_SQL_SQL, SQL_TEST_PASSWORD))
  else:
    exec_docker(config, 'client', '/opt/mssql-tools/bin/sqlcmd -S sql -U sa -P %s' % \
      (SQL_TEST_PASSWORD), workdir='/src', interactive=True)

def sql_exec_query(config, query, database_name='master'):
  """
  Executes an sql query from the client node.
  """
  exec_docker(config, 'client', '/opt/mssql-tools/bin/sqlcmd -S sql' \
    ' -U sa -d %s -P %s -q "%s"' % \
    (database_name, SQL_TEST_PASSWORD, query), workdir='/src')

def sql_exec_file(config, filename):
  """
  Executes an sql file from the source directory on the client node.
  """
  exec_docker(config, 'client', '/opt/mssql-tools/bin/sqlcmd -S sql -U sa -P %s -i "%s"' % \
      (SQL_TEST_PASSWORD, filename), workdir='/src')

def sqoop_export(config, export_dir, sql_table, database_name='master', delimiter=','):
  """
  Exports HDFS text delimited files to the sql node. 
  """
  exec_docker(config, 'client', '%s/bin/sqoop export --connect' \
    ' "jdbc:sqlserver://sql;databaseName=%s"' \
    ' --username "sa" --password "%s" --export-dir "%s" --table "%s"' \
    ' --input-fields-terminated-by "%s"' % \
    (SQOOP_HOME, database_name, SQL_TEST_PASSWORD, export_dir, sql_table, delimiter), \
    workdir='/src')

def launch_ssms_win_local(executable_path):
  """
  Launches Sql Server Management Studio locally.
  """
  if os.name == 'nt':
    if os.path.exists(executable_path):
      print('Note: Connection will only succeed if "Remember Password" has been checked in ' \
        'the SSMS login previously.')
      print('Use test password: %s' % (SQL_TEST_PASSWORD))
      os.system('"%s" -S tcp:localhost,%d -U sa' % (executable_path, PORT_SQL_SQL))
    else:
      print('The executable path for ssms does not exist. Please provide the correct one with' \
        ' arg "-f".' \
      )
  else:
    print('This command is not implemented for non-Windows platforms.')

def exec_hive_file(config, src_file):
  """
  Executes a hive script file from the source directory on the client node.
  """
  exec_docker(config, 'client', '%s/bin/beeline -u jdbc:hive2://hs:10000 -f %s' % \
    (HIVE_HOME, src_file), workdir='/src')

def exec_hive_query(config, query):
  """
  Executes a hive query from the client node.
  """
  exec_docker(config, 'client', '%s/bin/beeline -u jdbc:hive2://hs:10000 -e "%s"' % \
    (HIVE_HOME, query), workdir='/src')

def input_with_validator(prompt, failure_msg, validator_func):
  """
  Prompts for interactive user input using a validator function.
  """
  while True:
    val = input(prompt)
    if validator_func(val):
      return val
    else:
      print(failure_msg)

def validate_project_name(val):
  """
  Input validator function for project name configuration.
  """
  pattern = re.compile(r'\W')
  return not pattern.search(val) and val.isalnum()

def validate_directory(val):
  """
  Input validator function for a directory name.
  """
  return os.path.exists(val)

def validate_parent_directory(val):
  """
  Input validator function for a directory name where only the parent directory needs to exist.
  """
  return os.path.exists(os.path.dirname(val))

def validate_yn(val):
  """
  Input validator function for yes/no prompts.
  """
  _l = val.lower()
  return _l == 'y' or _l == 'n'

def set_environment(config):
  """
  Sets the environment variables for consumption by docker-compose.
  """
  os.environ['project_name'] = config.project_name
  os.environ['source_dir'] = config.source_dir
  os.environ['data_dir'] = config.data_dir
  os.environ['volumes_dir'] = config.volumes_dir
  os.environ['sql_test_password'] = SQL_TEST_PASSWORD

def configure(args):
  """
  Returns config using a file, arguments, or interactive input.
  """
  _f = args.config_file
  config = None
  if get_config_file_needed(args):
    if not os.path.exists(_f):
      _o = input_with_validator('Config file "%s" does not exist. Would you like to create one' \
        ' interactively? (y/n): ' % _f, 'Please input "y" or "n".', validate_yn)
      if _o.lower() == 'y':
        config = configure_interactively()
        config.save(_f)
        print('Config saved.')
      else:
        print('Program needs configuration. Exiting.')
        sys.exit(1)
        return
    else:
      config = Config.load(_f)
      print('Config read.')

    if args.project_name:
      config.project_name = args.project_name
    if args.source_dir:
      config.source_dir = args.source_dir
    if args.data_dir:
      config.data_dir = args.data_dir
    if args.volumes_dir:
      config.volumes_dir = args.volumes_dir
  else:
    config = Config(args.project_name, args.source_dir, args.data_dir, args.volumes_dir)

  return config

def configure_interactively():
  """
  Creates a config from interactive input.
  """
  proj_name = input_with_validator( \
    'Please input your project name: ', \
    'No spaces or non-alphanumeric characters allowed.', \
    validate_project_name \
  )
  src_dir = input_with_validator( \
    'Please input your playground src directory: ', \
    'Please use a valid directory name that exists.', \
    validate_directory \
  )
  data_dir = input_with_validator( \
    'Please input your data directory: ', \
    'Please use a valid directory name that exists.', \
    validate_directory \
  )
  vol_dir = input_with_validator( \
    'Please input your volumes directory: ', \
    'Please use a valid parent directory name that exists.', \
    validate_parent_directory \
  )
  config = Config(proj_name, src_dir, data_dir, vol_dir)
  return config

def build_img_cmd(config, args):
  """
  Command line function. See build_img() for documentation.
  """
  build_img(config)

def format_hdfs_cmd(config, args):
  """
  Command line function. See format_hdfs() for documentation.
  """
  format_hdfs(config)

def ingest_data_cmd(config, args):
  """
  Command line function. See ingest_data() for documentation.
  """
  ingest_data(config)

def copy_source_cmd(config, args):
  """
  Command line function. See copy_source() for documentation.
  """
  copy_source(config)

def setup_hive_cmd(config, args):
  """
  Command line function. See setup_hive() for documentation.
  """
  setup_hive(config)

def cluster_up_cmd(config, args):
  """
  Command line function. See cluster_up() for documentation.
  """
  cluster_up(config)

def start_hadoop_daemons_cmd(config, args):
  """
  Command line function. See start_hadoop_daemons() for documentation.
  """
  start_hadoop_daemons(config)

def start_hive_server_cmd(config, args):
  """
  Command line function. See start_hive_server() for documentation.
  """
  start_hive_server(config)

def cluster_down_cmd(config, args):
  """
  Command line function. See cluster_down() for documentation.
  """
  cluster_down(config)

def setup_cmd(config, args):
  """
  Command line function. See setup() for documentation.
  """
  if args.skip_confirm:
    setup(config)
    return

  result = input_with_validator('Are you sure you want to delete directory "%s" and all of its' \
    ' files? y/n: ' % (config.volumes_dir), \
    'Please use "y" or "n".', \
    validate_yn \
  ).lower()
  if result == 'y':
    setup(config)
  else:
    print('Cancelling.')

def start_cmd(config, args):
  """
  Command line function. See start() for documentation.
  """
  start(config, wait=not args.no_wait)

def stop_cmd(config, args):
  """
  Command line function. See stop() for documentation.
  """
  stop(config)

def destroy_volumes_cmd(config, args):
  """
  Command line function. See destroy_volumes() for documentation.
  """
  if args.skip_confirm:
    destroy_volumes(config)
    return

  result = input_with_validator('Are you sure you want to delete directory "%s" and all of its' \
    ' files? y/n: ' % (config.volumes_dir), \
    'Please use "y" or "n".', \
    validate_yn \
  ).lower()
  if result == 'y':
    destroy_volumes(config)
  else:
    print('Cancelling.')

def print_hadoop_node_logs_cmd(config, args):
  """
  Command line function. See print_hadoop_node_logs() for documentation.
  """
  print_hadoop_node_logs(config, args.node)

def beeline_cli_cmd(config, args):
  """
  Command line function. See beeline_cli() for documentation.
  """
  beeline_cli(config)

def bash_cli_cmd(config, args):
  """
  Command line function. See bash_cli() for documentation.
  """
  bash_cli(config, args.node)

def sqlcmd_cli_cmd(config, args):
  """
  Command line function. See sqlcmd_cli() for documentation.
  """
  sqlcmd_cli(config, args.local)

def sql_exec_query_cmd(config, args):
  """
  Command line function. See sql_exec_query() for documentation.
  """
  sql_exec_query(config, args.query, args.database)

def sql_exec_file_cmd(config, args):
  """
  Command line function. See sql_exec_file() for documentation.
  """
  sql_exec_file(config, args.filename)

def sqoop_export_cmd(config, args):
  """
  Command line function. See sqoop_export() for documentation.
  """
  sqoop_export(config, args.export_dir, args.sql_table, args.database_name, args.delimiter)

def local_sql_info_cmd(config, args):
  """
  Command line function. Prints out non-secured sql server connection info.
  """
  print('SERVER NAME: tcp:localhost,%d' % (PORT_SQL_SQL))
  print('AUTHENTICATION: SQL Server AUthentication')
  print('LOGIN: sa')
  print('PASSWORD: %s' % (SQL_TEST_PASSWORD))

def launch_ssms_win_local_cmd(config, args):
  """
  Command line function. See launch_ssms_win_local() for documentation.
  """
  launch_ssms_win_local(args.executable_path)

def exec_hive_file_cmd(config, args):
  """
  Command line function. See exec_hive_file() for documentation.
  """
  exec_hive_file(config, args.src_path)

def exec_hive_query_cmd(config, args):
  """
  Command line function. See exec_hive_query() for documentation.
  """
  exec_hive_query(config, args.query)

def print_health_cmd(config, args):
  """
  Command line function. See print_health() for documentation.
  """
  print_health(config)

def wait_for_healthy_nodes_cmd(config, args):
  """
  Command line function. See wait_for_healthy_nodes_print() for documentation.
  """
  wait_for_healthy_nodes_print(config, args.timeout)

def get_config_file_needed(args):
  """
  Determines whether or not we need to fetch additional config variables from a file.
  """
  return not (args.project_name and args.source_dir and args.data_dir and args.volumes_dir)

def main():
  """
  Main entry point for the program
  """
  parser = argparse.ArgumentParser(prog='playground', description='HDFS, Hive, and SQL Playground')
  parser.set_defaults(func=None)

  # config-file
  parser.add_argument('--config-file', '-c', default='config.json', help='The filename' \
    ' of the configuration file.')

  # config-overrides
  config_group = parser.add_argument_group('config-overrides', description='Overrides' \
    ' the configuration variables.')
  config_group.add_argument('--project-name', '-p')
  config_group.add_argument('--source-dir', '-s')
  config_group.add_argument('--data-dir', '-d')
  config_group.add_argument('--volumes-dir', '-v')
  config_group.set_defaults(project_name=None, source_dir=None, data_dir=None, volumes_dir=None)

  subparsers = parser.add_subparsers()

  # build-img
  subparsers.add_parser('build-img', help='Builds or rebuilds the required Docker images. Do this' \
    ' when you change the Dockerfile or anything in ./bin/.').set_defaults(func=build_img_cmd)

  # format-hdfs
  subparsers.add_parser('format-hdfs', help='Formats the entire distributed file system of the' \
    ' running cluster.').set_defaults(func=format_hdfs_cmd)

  # ingest-data
  subparsers.add_parser('ingest-data', help='Copies the mounted data volume to HDFS at /data on' \
    ' the running cluster.').set_defaults(func=ingest_data_cmd)

  # copy-source
  subparsers.add_parser('copy-source', help='Copies the configured source folder to the mounted' \
    ' client node volume.').set_defaults(func=copy_source_cmd)

  # setup-hive
  subparsers.add_parser('setup-hive', help='Creates the Hive schema metastore and makes' \
    ' necessary directories in HDFS. Cluster should be up and hadoop daemons should already' \
    ' be running.').set_defaults(func=setup_hive_cmd)

  # cluster-up
  subparsers.add_parser('cluster-up', help='Boots up all the nodes on the cluster but does not' \
    ' start any of their services.').set_defaults(func=cluster_up_cmd)

  # start-hadoop
  subparsers.add_parser('start-hadoop', help='Starts the name node and data node services for' \
    ' HDFS on a running cluster.').set_defaults(func=start_hadoop_daemons_cmd)

  # start-hive
  subparsers.add_parser('start-hive', help='Starts the hive server in the running cluster.') \
    .set_defaults(func=start_hive_server_cmd)

  # cluster-down
  subparsers.add_parser('cluster-down', help='Shuts down all of the nodes.') \
    .set_defaults(func=cluster_down_cmd)

  # setup
  setup_p = subparsers.add_parser('setup', help='Sets up the cluster for the first time.')
  setup_p.add_argument('--skip-confirm', '-y', action='store_true', help='Skips any confirmation' \
    ' messages')
  setup_p.set_defaults(func=setup_cmd, skip_confirm=False)

  # start
  start_p = subparsers.add_parser('start', help='Spins up the cluster and starts the daemons on ' \
    'each node.')
  start_p.add_argument('--no-wait', '-w', action='store_true', help='Exits immediately after ' \
    'the cluster daemons have been told to start rather than blocking until the nodes are healthy.')
  start_p.set_defaults(func=start_cmd, no_wait=False)

  # stop
  subparsers.add_parser('stop', help='Stops all of the services and shuts down all of the nodes.') \
    .set_defaults(func=stop_cmd)

  # destroy-vol
  destroy_vol_p = subparsers.add_parser('destroy-vol', help='Removes all persisted cluster files.')
  destroy_vol_p.add_argument('--skip-confirm', '-y', action='store_true')
  destroy_vol_p.set_defaults(func=destroy_volumes_cmd, skip_confirm=False)

  # print-hadoop-logs
  print_hadoop_node_logs_p = subparsers.add_parser('print-hadoop-logs', help='Prints the log file' \
    ' of the specified hadoop node.')
  print_hadoop_node_logs_p.add_argument('--node', '-n', help='The node to check the logs for.')
  print_hadoop_node_logs_p.set_defaults(func=print_hadoop_node_logs_cmd)

  # beeline-cli
  subparsers.add_parser('beeline-cli', help='Launches a cli using beeline on the client node.') \
    .set_defaults(func=beeline_cli_cmd)

  # bash-cli
  bash_cli_p = subparsers.add_parser('bash-cli', help='Launches bash cli on a single node in the' \
    ' cluster.')
  bash_cli_p.add_argument('--node', '-n', help='The Docker service name of the node. Refer to the' \
    ' docker-compose.yml. Examples: "client", "nn1", "dn1", etc.')
  bash_cli_p.set_defaults(func=bash_cli_cmd, node='client')

  # sql-cli
  sql_cli_p = subparsers.add_parser('sql-cli', help='Launches sqlcmd on the client' \
    ' node or locally.')
  sql_cli_p.add_argument('--local', '-l', action='store_true', help='If specified, sqlcmd is' \
    ' launched on the host machine instead of the client node. Note: this requires sqlcmd to' \
      ' be on the environment PATH variable.')
  sql_cli_p.set_defaults(func=sqlcmd_cli_cmd, local=False)

  # sql-exec-query
  sql_exec_query_p = subparsers.add_parser('sql-exec-query', help='Executes an SQL query.')
  sql_exec_query_p.add_argument('--query', '-q', help='The sql query.')
  sql_exec_query_p.add_argument('--database', '-d', help='The database to use.')
  sql_exec_query_p.set_defaults(func=sql_exec_query_cmd, database='master')

  # sql-exec-file
  sql_exec_file_p = subparsers.add_parser('sql-exec-file', help='Executes an SQL file on the ' \
    'client node.')
  sql_exec_file_p.add_argument('--filename', '-f', help='The relative filename in the source dir.')
  sql_exec_file_p.set_defaults(func=sql_exec_file_cmd)

  # sqoop-export
  sqoop_export_p = subparsers.add_parser('sqoop-export', help='Exports CSV files loaded in HDFS' \
    ' to the sql server node.')
    #args.export_dir, args.sql_table, args.database_name, args.delimiter
  sqoop_export_p.add_argument('--export-dir', '-e', help='The directory in HDFS which contains' \
    ' the CSV files.')
  sqoop_export_p.add_argument('--sql-table', '-t', help='The name of the sql table to export to.' \
    ' Note: this table should already exist with the correct schema.')
  sqoop_export_p.add_argument('--database-name', '-b', help='The name of the database to' \
    ' export to.')
  sqoop_export_p.add_argument('--delimiter', '-d', help='The character used to for delimiting' \
    ' the values in the HDFS files.')
  sqoop_export_p.set_defaults(func=sqoop_export_cmd, database_name='master', delimiter=',')

  # local-sql-info
  subparsers.add_parser('local-sql-info', help='Shows the connection information for connecting' \
    ' to the sql server from the parent host.').set_defaults(func=local_sql_info_cmd)

  # launch-ssms
  launch_ssms_p = subparsers.add_parser('launch-ssms', help='Note: Only works on Windows and' \
    ' requires installation of SQL Server Management Server. This command launches SQL Server' \
    ' Management Server using the local connection information.')
  launch_ssms_p.add_argument('--executable-path', '-f')
  launch_ssms_p.set_defaults(func=launch_ssms_win_local_cmd, executable_path= \
    'C:\\Program Files (x86)\\Microsoft SQL Server Management Studio 18\\Common7\\IDE\\Ssms.exe')

  # exec-hive-file
  exec_hive_file_p = subparsers.add_parser('exec-hive-file', help='Executes a hive script from' \
    ' the src folder.')
  exec_hive_file_p.add_argument('--src-path', '-f', help='The relative path to the file on the ' \
    'linux node')
  exec_hive_file_p.set_defaults(func=exec_hive_file_cmd)

  # exec-hive-query
  exec_hive_query_p = subparsers.add_parser('exec-hive-query', help='Executes a single' \
    ' hive query.')
  exec_hive_query_p.add_argument('--query', '-e', help='The hive query string to execute.')
  exec_hive_query_p.set_defaults(func=exec_hive_query_cmd)

  # print-health
  subparsers.add_parser('print-health', help='Prints the cluster health information.') \
    .set_defaults(func=print_health_cmd)

  # wait-for-healthy-nodes
  wait_p = subparsers.add_parser('wait-for-healthy-nodes', help='Waits until the cluster is ' \
    'healthy or until timeout.')
  wait_p.add_argument('--timeout', '-t', help='The time in seconds until command timeout.')
  wait_p.set_defaults(func=wait_for_healthy_nodes_cmd, timeout=200)

  args = parser.parse_args()
  if not args.func:
    print('No subcommand selected. Use -h to get help.')
    parser.print_usage()
    return

  config = configure(args)
  args.func(config, args)
  print('Program end.')

if __name__ == '__main__':
  main()
