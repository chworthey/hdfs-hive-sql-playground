"""
Doc str
"""
import argparse
import os
import json
import re
import shutil
from tkinter import Tk

ROOT_DIR = os.path.dirname(os.path.realpath(__file__))
HADOOP_HOME = '/himage/hadoop-3.3.0'
HIVE_HOME = '/himage/apache-hive-3.1.2-bin'
COMPOSE_FILE = os.path.join(ROOT_DIR, 'docker-compose.yml')
VOLUMES_DIR = os.path.join(ROOT_DIR, 'volumes')
SQL_TEST_PASSWORD = 'myStrong(!)Password'

def build_img(config):
  """
  doc str
  """
  os.system('docker-compose -p %s -f "%s" build' % (config['project_name'], COMPOSE_FILE))

def format_hdfs(config):
  """
  doc str
  """
  os.system('docker exec %s_nn1_1 %s/bin/hdfs namenode -format clust' % \
    (config['project_name'], HADOOP_HOME) \
  )

def ingest_data(config):
  """
  doc str
  """
  os.system('docker exec %s_nn1_1 %s/bin/hadoop fs -put /data /data' % \
    (config['project_name'], HADOOP_HOME) \
  )

def setup_hive(config):
  """
  doc str
  """
  fs_cmd = 'docker exec %s_nn1_1 %s/bin/hadoop fs ' % (config['project_name'], HADOOP_HOME)
  os.system(fs_cmd + '-mkdir /tmp')
  os.system(fs_cmd + '-mkdir -p /user/hive/warehouse')
  os.system(fs_cmd + '-chmod g+w /tmp')
  os.system(fs_cmd + '-chmod g+w /user/hive/warehouse')
  os.system('docker exec -w /metastore %s_hs_1 %s/bin/schematool -dbType derby -initSchema' % \
    (config['project_name'], HIVE_HOME) \
  )

def setup_sqoop(config):
  """
  doc str
  """
  print('blah')

def cluster_up(config):
  """
  doc str
  """
  os.system('docker-compose -p %s -f "%s" up -d' % (config['project_name'], COMPOSE_FILE))

def start_hadoop_daemons(config):
  """
  doc str
  """
  _n = config['project_name']
  os.system('docker exec %s_nn1_1 %s/bin/hdfs --daemon start namenode' % \
    (_n, HADOOP_HOME) \
  )
  os.system('docker exec %s_dn1_1 %s/bin/hdfs --daemon start datanode' % \
    (_n, HADOOP_HOME) \
  )
  os.system('docker exec %s_rman_1 %s/bin/yarn --daemon start resourcemanager' % \
    (_n, HADOOP_HOME) \
  )
  os.system('docker exec %s_nm1_1 %s/bin/yarn --daemon start nodemanager' % \
    (_n, HADOOP_HOME) \
  )
  os.system('docker exec %s_mrhist_1 %s/bin/mapred --daemon start historyserver' % \
    (_n, HADOOP_HOME) \
  )

def start_hive_server(config):
  """
  doc str
  """
  os.system('docker exec -d -w /metastore %s_hs_1 %s/bin/hiveserver2' % \
    (config['project_name'], HIVE_HOME) \
  )

def cluster_down(config):
  """
  doc str
  """
  os.system('docker-compose -p %s -f "%s" down' % (config['project_name'], COMPOSE_FILE))

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

  print('Spinning cluster down.')
  cluster_down(config)

def start(config):
  """
  doc str
  """
  print('Spinning cluster up.')
  cluster_up(config)

  print('Starting Hadoop Daemons.')
  start_hadoop_daemons(config)

  print('Starting Hive Server.')
  start_hive_server(config)

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

  print('Deleting files.')
  shutil.rmtree(VOLUMES_DIR)

def beeline_cli(config):
  """
  doc str
  """
  os.system('docker exec -w /src -it %s_client_1 %s/bin/beeline -u jdbc:hive2://hs:10000' % \
     (config['project_name'], HIVE_HOME) \
  )

def bash_cli(config, nodename):
  """
  doc str
  """
  os.system('docker exec -it %s_%s_1 bash' %(config['project_name'], nodename))

def sqlcmd_cli(config, local):
  """
  doc str
  """
  if local:
    os.system('sqlcmd -S tcp:localhost,3005 -U sa -P %s' % (SQL_TEST_PASSWORD))
  else:
    os.system('docker exec -w /src -it %s_client_1 sqlcmd -S sql -U sa -P %s' % \
      (config['project_name'], SQL_TEST_PASSWORD) \
    )

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
      os.system('%s -U sa -S tcp:localhost,3005' % (executable_path))
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
  os.system('docker exec %s_client_1 $HIVE_HOME/bin/beeline -u jdbc:hive2://hs:10000 -f %s' % \
    (config['project_name'], src_file) \
  )

def exec_sql_file(config, src_file):
  """
  doc str
  """
  os.system('docker exec %s_client_1 sqlcmd -S sql -U sa -P $SQL_PWD -f %s' % \
    (config['project_name'], src_file) \
  )

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

def validate_yn(val):
  """
  doc str
  """
  _l = val.lower()
  return _l == 'y' or _l == 'n'

def configure():
  """
  doc str
  """
  config_path = os.path.join(ROOT_DIR, 'config.json')
  config = {}
  if not os.path.isfile(config_path):
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
    config['project_name'] = proj_name
    config['source_dir'] = src_dir
    config['data_dir'] = data_dir
    with open(config_path, 'w') as _fp:
      json.dump(config, _fp, indent=2)
    os.environ['project_name'] = proj_name
    os.environ['source_dir'] = src_dir
    os.environ['data_dir'] = data_dir
    print('Config written.')
  else:
    with open(config_path, 'r') as _fp:
      config = json.load(_fp)
    os.environ['project_name'] = config['project_name']
    os.environ['source_dir'] = config['source_dir']
    os.environ['data_dir'] = config['data_dir']
    print('Config read.')

  os.environ['sql_test_password'] = SQL_TEST_PASSWORD
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
  start(config)

def stop_cmd(config, args):
  """
  doc str
  """
  stop(config)

def destroy_volumes_cmd(config, args):
  """
  doc str
  """
  if args.skip_confirm:
    destroy_volumes(config)
    return

  result = input_with_validator('Are you sure you want to delete directory "%s" and all of its' \
    ' files? y/n: ' % (VOLUMES_DIR), \
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

def main():
  """
  doc str
  """
  parser = argparse.ArgumentParser(prog='playground', description='HDFS, Hive, and SQL Playground')
  parser.set_defaults(func=None)

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
  subparsers.add_parser('start', help='TODO: help').set_defaults(func=start_cmd)

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
  bash_cli_p.set_defaults(func=sqlcmd_cli_cmd, node='client')

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

  args = parser.parse_args()
  if not args.func:
    print('No args supplied. Use -h to get help.')
    parser.print_usage()
    return

  config = configure()
  args.func(config, args)
  print('Program end.')

if __name__ == '__main__':
  main()
