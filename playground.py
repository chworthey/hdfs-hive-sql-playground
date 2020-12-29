"""
Doc str
"""
import argparse
import collections
import distutils.dir_util
import json
import os
import re
import shutil
import subprocess
import time
from tkinter import Tk

# PyPI installed modules...
import requests

ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
HADOOP_HOME = '/himage/hadoop-3.3.0'
HIVE_HOME = '/himage/apache-hive-3.1.2-bin'
COMPOSE_FILE = os.path.join(ROOT_DIR, 'docker-compose.yml')
SQL_TEST_PASSWORD = 'myStrong(!)Password'
NUM_DATA_NODES = 1
NUM_NODE_MANAGERS = 1
MIN_DISK_SPACE = 8589934592 # 1GB

PORT_UI_NN1    = 3000
PORT_UI_DN1    = 3001
PORT_UI_RMAN   = 3002
PORT_UI_NM1    = 3003
PORT_UI_MRHIST = 3004
PORT_UI_HS     = 3005
PORT_SQL_SQL   = 3006

PORT_DOC = [
  (PORT_UI_NN1,    'http', 'Web UI for the primary name node'),
  (PORT_UI_DN1,    'http', 'Web UI for data node 1'),
  (PORT_UI_RMAN,   'http', 'Web UI for YARN resource manager'),
  (PORT_UI_NM1,    'http', 'Web UI for node manager 1'),
  (PORT_UI_MRHIST, 'http', 'Web UI map reduce history server'),
  (PORT_UI_HS,     'http', 'Web UI for hive server'),
  (PORT_SQL_SQL,   'sql (tcp/ip)', 'SQL server connection port')
]

NodeHealthBeanCheck = collections.namedtuple('NodeHealthBeanCheck', \
  'bean_name prop_name check_func')
NodeHealthReport = collections.namedtuple('NodeHealthReport', \
  'is_healthy message')
HealthReportSummary = collections.namedtuple('HealthReportSummary', \
  'cluster_healthy nn1 dn1 rman nm1 mrhist hs client sql sqp')

class Config:
  """
  Doc str
  """
  def __init__(self, project_name=None, source_dir=None, data_dir=None, volumes_dir=None):
    self._project_name = project_name
    self._source_dir = source_dir
    self._data_dir = data_dir
    self._volumes_dir = volumes_dir

  @property
  def project_name(self):
    """
    Doc str
    """
    return self._project_name

  @project_name.setter
  def project_name(self, value):
    self._project_name = value

  @property
  def source_dir(self):
    """
    Doc str
    """
    return self._source_dir

  @source_dir.setter
  def source_dir(self, value):
    self._source_dir = value

  @property
  def data_dir(self):
    """
    Doc str
    """
    return self._data_dir

  @data_dir.setter
  def data_dir(self, value):
    self._data_dir = value

  @property
  def volumes_dir(self):
    """
    Doc str
    """
    return self._volumes_dir

  @volumes_dir.setter
  def volumes_dir(self, value):
    self._volumes_dir = value

  def save(self, filename):
    """
    Doc str
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
    Doc str
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
  doc str
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
  _args += command.split(' ')
  output = subprocess.run(_args, check=check, shell=True)
  return output.returncode


def build_img(config):
  """
  doc str
  """
  set_environment(config)
  os.system('docker-compose -p %s -f "%s" build' % (config.project_name, COMPOSE_FILE))

def format_hdfs(config):
  """
  doc str
  """
  exec_docker(config, 'nn1', '%s/bin/hdfs namenode -format -force clust' % (HADOOP_HOME))

def ingest_data(config):
  """
  doc str
  """
  exec_docker(config, 'nn1', '%s/bin/hadoop fs -put /data /data' % (HADOOP_HOME))

def copy_source(config):
  """
  doc str
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
  doc str
  """
  fs_cmd = '%s/bin/hadoop fs ' % (HADOOP_HOME)
  exec_docker(config, 'nn1', fs_cmd + '-mkdir /tmp', check=False)
  exec_docker(config, 'nn1', fs_cmd + '-mkdir -p /user/hive/warehouse', check=False)
  exec_docker(config, 'nn1', fs_cmd + '-chmod g+w /tmp')
  exec_docker(config, 'nn1', fs_cmd + '-chmod g+w /user/hive/warehouse')
  exec_docker(config, 'hs', '%s/bin/schematool -dbType derby -initSchema' % \
    (HIVE_HOME), workdir='/metastore')

def setup_sqoop(config):
  """
  doc str
  """
  print('blah')

def cluster_up(config):
  """
  doc str
  """
  set_environment(config)
  os.system('docker-compose -p %s -f "%s" up -d' % (config.project_name, COMPOSE_FILE))

def start_hadoop_daemons(config):
  """
  doc str
  """
  exec_docker(config, 'nn1', '%s/bin/hdfs --daemon start namenode' % (HADOOP_HOME))
  exec_docker(config, 'dn1', '%s/bin/hdfs --daemon start datanode' % (HADOOP_HOME))
  exec_docker(config, 'rman', '%s/bin/yarn --daemon start resourcemanager' % (HADOOP_HOME))
  exec_docker(config, 'nm1', '%s/bin/yarn --daemon start nodemanager' % (HADOOP_HOME))
  exec_docker(config, 'mrhist', '%s/bin/mapred --daemon start historyserver' % (HADOOP_HOME))

def start_hive_server(config):
  """
  doc str
  """
  exec_docker(config, 'hs', '%s/bin/hiveserver2' % (HIVE_HOME), \
    detached=True, workdir='/metastore')

def cluster_down(config):
  """
  doc str
  """
  set_environment(config)
  os.system('docker-compose -p %s -f "%s" down' % (config.project_name, COMPOSE_FILE))

def metric_request(port):
  """
  doc str
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

def find_bean_by_type(jsn, typ):
  """
  doc str
  """
  if 'beans' not in jsn:
    return None
  else:
    return next((b for b in jsn['beans'] if b['name'] == typ), None)

def extract_bean_prop(jsn, bean_name, propname):
  """
  doc str
  """
  bean = find_bean_by_type(jsn, bean_name)
  if bean and propname in bean:
    return bean[propname]
  else:
    return None

def gen_node_report_from_checks(jsn, checks):
  """
  doc str
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
  return NodeHealthReport(is_healthy=True, message='Sufficient disk space.') \
    if prop_val >= MIN_DISK_SPACE else NodeHealthReport(is_healthy=False, message='Insufficient' \
      ' disk space. Minimum required disk space is %d. Remaining bytes: %d' % \
      (MIN_DISK_SPACE, prop_val))

def json_checker_namenode(jsn):
  """
  doc str
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
  doc str
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
  doc str
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
  doc str
  """
  healthy = 'beans' in jsn and len(jsn['beans']) > 0
  if healthy:
    return NodeHealthReport(is_healthy=True, message='\u2705 Response has expected json.')
  else:
    return NodeHealthReport(is_healthy=False, message='\u274C Response does not' \
      ' have expected json.')


def gen_node_health_report(jsn, json_checker_func):
  """
  doc str
  """
  if jsn:
    return json_checker_func(jsn)
  else:
    message = '\u274C Could not fetch metrics from server. Most likely the node is down.'
    return NodeHealthReport(is_healthy=False, message=message)

def gen_docker_health_report(config, node_name):
  """
  doc str
  """
  return_code = exec_docker(config, node_name, 'bash -c exit 0', check=False)
  if return_code == 0:
    return NodeHealthReport(is_healthy=True, message='\u2705 Node running')
  else:
    return NodeHealthReport(is_healthy=False, message='\u274C Node not running')

def gen_health_summary(config):
  """
  doc str
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
    sql=_sql, \
    sqp=None)
  return summary

def print_node_health(report):
  """
  doc str
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
  doc str
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
  print('SQOOP SERVER')
  print_node_health(summary.sqp)
  print('OVERALL CLUSTER HEALTH')
  if summary.cluster_healthy:
    print('\u2705 Healthy')
  else:
    print('\u274C Unhealthy')

def print_health(config):
  """
  doc str
  """
  print('Checking cluster health.')
  print()
  summary = gen_health_summary(config)
  print_summary(summary)

def wait_for_healthy_nodes_print(config, timeout):
  """
  doc str
  """
  _start = time.time()
  summary = wait_for_healthy_nodes(config, timeout=timeout)
  print('Wait completed in %fs. Summary:' % (time.time() - _start))
  print()
  print_summary(summary)

def get_summary_preview_str(summary):
  """
  doc str
  """
  _s = [
    ('nn1', summary.nn1.is_healthy),
    ('dn1', summary.dn1.is_healthy),
    ('rman', summary.rman.is_healthy),
    ('nm1', summary.nm1.is_healthy),
    ('mrhist', summary.mrhist.is_healthy),
    ('hs', summary.hs.is_healthy),
    ('client', summary.client.is_healthy),
    ('sql', summary.sql.is_healthy),
    ('sqp', True)
  ]
  _s2 = map(lambda a : '%s %s' % \
    (('\u2705' if a[1] else '\u274C'), a[0]), _s)
  return ', '.join(_s2)

def wait_for_healthy_nodes(config, timeout=200, interval=5):
  """
  doc str
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
  doc str
  """
  print('Spinning cluster up.')
  cluster_up(config)

  print('Formatting HDFS.')
  format_hdfs(config)

  print('Starting Hadoop Daemons.')
  start_hadoop_daemons(config)

  print('Setting up Hive server.')
  setup_hive(config)

  print('Setting up Sqoop server.')
  setup_sqoop(config)

  print('Ingesting configured data volume into HDFS (this could take some time).')
  ingest_data(config)

  print('Copying configured source folder to the client node volume.')
  copy_source(config)

  print('Spinning cluster down.')
  cluster_down(config)

def print_port_doc():
  """
  doc str
  """
  print('Exposed ports on localhost:')
  for _p in PORT_DOC:
    print('Port: %s, Type: %s, Description: %s' % \
      (_p[0], _p[1], _p[2]))

def start(config, wait=True):
  """
  doc str
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
  doc str
  """
  print('Spinning cluster down.')
  cluster_down(config)

def destroy_volumes(config):
  """
  doc str
  """
  print('Spinning cluster down.')
  cluster_down(config)

  if not os.path.exists(config.volumes_dir):
    print('Volumes directory does not exist. Cannot delete.')
  else:
    print('Deleting files.')
    shutil.rmtree(config.volumes_dir)

def beeline_cli(config):
  """
  doc str
  """
  exec_docker(config, 'client', '%s/bin/beeline -u jdbc:hive2://hs:10000' % (HIVE_HOME), \
    workdir='/src', interactive=True)

def bash_cli(config, nodename):
  """
  doc str
  """
  exec_docker(config, nodename, 'bash', interactive=True)

def sqlcmd_cli(config, local):
  """
  doc str
  """
  if local:
    os.system('sqlcmd -S tcp:localhost,3007 -U sa -P %s' % (SQL_TEST_PASSWORD))
  else:
    exec_docker(config, 'client', 'sqlcmd -S sql -U sa -P %s' % (SQL_TEST_PASSWORD), \
      workdir='/src', interactive=True)

def launch_ssms_win_local(executable_path):
  """
  doc str
  """
  if os.name == 'nt':
    if os.path.exists(executable_path):
      print('Copying the test SQL server password to the clipboard since we do not care' \
        ' about security concerns. Paste it when prompted.' \
      )
      _r = Tk()
      _r.withdraw()
      _r.clipboard_clear()
      _r.clipboard_append(SQL_TEST_PASSWORD)
      _r.update()
      _r.destroy()
      os.system('%s -U sa -S tcp:localhost,3007' % (executable_path))
    else:
      print('The executable path for ssms does not exist. Please provide the correct one with' \
        ' arg "-f".' \
      )
  else:
    print('This command is not implemented for non-Windows platforms.')

def exec_hive_file(config, src_file):
  """
  doc str
  """
  exec_docker(config, 'client', '$HIVE_HOME/bin/beeline -u jdbc:hive2://hs:10000 -f %s' % \
    (src_file))

def exec_sql_file(config, src_file):
  """
  doc str
  """
  exec_docker(config, 'client', 'sqlcmd -S sql -U sa -P $SQL_PWD -f %s' % (src_file))

def input_with_validator(prompt, failure_msg, validator_func):
  """
  doc str
  """
  while True:
    val = input(prompt)
    if validator_func(val):
      return val
    else:
      print(failure_msg)

def validate_project_name(val):
  """
  doc str
  """
  pattern = re.compile(r'\W')
  return not pattern.search(val) and val.isalnum()

def validate_directory(val):
  """
  doc str
  """
  return os.path.exists(val)

def validate_parent_directory(val):
  """
  doc str
  """
  return os.path.exists(os.path.dirname(val))

def validate_yn(val):
  """
  doc str
  """
  _l = val.lower()
  return _l == 'y' or _l == 'n'

def set_environment(config):
  """
  doc str
  """
  os.environ['project_name'] = config.project_name
  os.environ['source_dir'] = config.source_dir
  os.environ['data_dir'] = config.data_dir
  os.environ['volumes_dir'] = config.volumes_dir
  os.environ['sql_test_password'] = SQL_TEST_PASSWORD

def configure(args):
  """
  doc str
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
  doc str
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
  doc str
  """
  build_img(config)

def format_hdfs_cmd(config, args):
  """
  doc str
  """
  format_hdfs(config)

def ingest_data_cmd(config, args):
  """
  doc str
  """
  ingest_data(config)

def copy_source_cmd(config, args):
  """
  doc str
  """
  copy_source(config)

def setup_hive_cmd(config, args):
  """
  doc str
  """
  setup_hive(config)

def setup_sqoop_cmd(config, args):
  """
  doc str
  """
  setup_sqoop(config)

def cluster_up_cmd(config, args):
  """
  doc str
  """
  cluster_up(config)

def start_hadoop_daemons_cmd(config, args):
  """
  doc str
  """
  start_hadoop_daemons(config)

def start_hive_server_cmd(config, args):
  """
  doc str
  """
  start_hive_server(config)

def cluster_down_cmd(config, args):
  """
  doc str
  """
  cluster_down(config)

def setup_cmd(config, args):
  """
  doc str
  """
  setup(config)

def start_cmd(config, args):
  """
  doc str
  """
  start(config, wait=not args.no_wait)

def stop_cmd(config, args):
  """
  doc str
  """
  stop(config)

def destroy_volumes_cmd(config, args):
  """
  doc str
  """
  if not os.path.exists(config.volumes_dir):
    print('Volumes directory does not exist. Cannot delete.')
    return

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

def beeline_cli_cmd(config, args):
  """
  doc str
  """
  beeline_cli(config)

def bash_cli_cmd(config, args):
  """
  doc str
  """
  bash_cli(config, args.node)

def sqlcmd_cli_cmd(config, args):
  """
  doc str
  """
  sqlcmd_cli(config, args.local)

def local_sql_info_cmd(config, args):
  """
  doc str
  """
  print('SERVER NAME: tcp:localhost,3005')
  print('AUTHENTICATION: SQL Server AUthentication')
  print('LOGIN: sa')
  print('PASSWORD: %s' % (SQL_TEST_PASSWORD))

def launch_ssms_win_local_cmd(config, args):
  """
  doc str
  """
  launch_ssms_win_local(args.executable_path)

def exec_hive_file_cmd(config, args):
  """
  doc str
  """
  exec_hive_file(config, args.src_path)

def exec_sql_file_cmd(config, args):
  """
  doc str
  """
  exec_sql_file(config, args.src_path)

def print_health_cmd(config, args):
  """
  doc str
  """
  print_health(config)

def wait_for_healthy_nodes_cmd(config, args):
  """
  doc str
  """
  wait_for_healthy_nodes_print(config, args.timeout)

def get_config_file_needed(args):
  """
  doc str
  """
  return not (args.project_name and args.source_dir and args.data_dir and args.volumes_dir)

def main():
  """
  doc str
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

  # setup-sqoop
  subparsers.add_parser('setup-sqoop', help='TODO: help').set_defaults(func=setup_sqoop_cmd)

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
  subparsers.add_parser('setup', help='TODO: help').set_defaults(func=setup_cmd)

  # start
  start_p = subparsers.add_parser('start', help='TODO: help')
  start_p.add_argument('--no-wait', '-w', action='store_true')
  start_p.set_defaults(func=start_cmd, no_wait=False)

  # stop
  subparsers.add_parser('stop', help='Stops all of the services and shuts down all of the nodes.') \
    .set_defaults(func=stop_cmd)

  # destroy-vol
  destroy_vol_p = subparsers.add_parser('destroy-vol', help='TODO: help')
  destroy_vol_p.add_argument('--skip-confirm', '-y', action='store_true')
  destroy_vol_p.set_defaults(func=destroy_volumes_cmd, skip_confirm=False)

  # beeline-cli
  subparsers.add_parser('beeline-cli', help='TODO: help').set_defaults(func=beeline_cli_cmd)

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

  # local-sql-info
  subparsers.add_parser('local-sql-info', help='Shows the connection information for connecting' \
    ' to the sql server from the parent host.').set_defaults(func=local_sql_info_cmd)

  # launch-ssms
  launch_ssms_p = subparsers.add_parser('launch-ssms', help='Note: Only works on Windows and' \
    ' requires installation of SQL Server Management Server. This command launches SQL Server' \
    ' Management Server using the local connection information.')
  launch_ssms_p.add_argument('--executable-path', '-f')
  launch_ssms_p.set_defaults(func=sqlcmd_cli_cmd, executable_path='C:\\Program Files (x86)\\' \
    'Microsoft SQL Server Management Studio 18\\Common7\\IDE\\Ssms.exe')

  # exec-hive-file
  exec_hive_file_p = subparsers.add_parser('exec-hive-file', help='Executes a hive script from' \
    ' the src folder.')
  exec_hive_file_p.add_argument('--src-path', '-f', help='The relative path to the file on the ' \
    'linux node')
  exec_hive_file_p.set_defaults(func=exec_hive_file_cmd)

  # exec-sql-file
  exec_sql_file_p = subparsers.add_parser('exec-sql-file', help='Executes a sql script from' \
    ' the src folder.')
  exec_sql_file_p.add_argument('--src-path', '-f', help='The relative path to the file on the ' \
    'linux node')
  exec_sql_file_p.set_defaults(func=exec_sql_file_cmd)

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
