import argparse
import os
import json
import re
import shutil

root_dir = os.path.dirname(os.path.realpath(__file__))
hadoop_home = '/himage/hadoop-3.3.0'
hive_home = '/himage/apache-hive-3.1.2-bin'
compose_file = os.path.join(root_dir, 'docker-compose.yml')
volumes_dir = os.path.join(root_dir, 'volumes')

def build_img():
  os.system('docker-compose -f "%s" build' % (compose_file))

def format_hdfs(config):
  os.system('docker exec %s_nn1_1 %s/bin/hdfs namenode -format clust' % 
    (config['project_name'], hadoop_home)
  )

def ingest_data(config):
  os.system('docker exec %s_nn1_1 %s/bin/hadoop fs -put /data /data' %
    (config['project_name'], hadoop_home)
  )

def setup_hive(config):
  fs_cmd = 'docker exec %s_nn1_1 %s/bin/hadoop fs ' % (config['project_name'], hadoop_home)
  os.system(fs_cmd + '-mkdir /tmp')
  os.system(fs_cmd + '-mkdir -p /user/hive/warehouse')
  os.system(fs_cmd + '-chmod g+w /tmp')
  os.system(fs_cmd + '-chmod g+w /user/hive/warehouse')
  os.system('docker exec -w /metastore %s_hs_1 %s/bin/schematool -dbType derby -initSchema' % (config['project_name'], hive_home))

def setup_sqoop(config):
  print('blah')

def cluster_up():
  os.system('docker-compose -f "%s" up -d' % (compose_file))

def start_hadoop_daemons(config):
  n = config['project_name']
  os.system('docker exec %s_nn1_1 %s/bin/hdfs --daemon start namenode' % (n, hadoop_home))
  os.system('docker exec %s_dn1_1 %s/bin/hdfs --daemon start datanode' % (n, hadoop_home))
  os.system('docker exec %s_rman_1 %s/bin/yarn --daemon start resourcemanager' % (n, hadoop_home))
  os.system('docker exec %s_nm1_1 %s/bin/yarn --daemon start nodemanager' % (n, hadoop_home))
  os.system('docker exec %s_mrhist_1 %s/bin/mapred --daemon start historyserver' % (n, hadoop_home))

def start_hive_server(config):
  os.system('docker exec -d -w /metastore %s_hs_1 %s/bin/hiveserver2' % (config['project_name'], hive_home))

def cluster_down():
  os.system('docker-compose -f "%s" down' % (compose_file))

def setup(config):
  print('Spinning cluster up.')
  cluster_up()

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
  cluster_down()

def start(config):
  print('Spinning cluster up.')
  cluster_up()

  print('Starting Hadoop Daemons.')
  start_hadoop_daemons(config)

  print('Starting Hive Server.')
  start_hive_server(config)

def stop():
  print('Spinning cluster down.')
  cluster_down()

def destroy_volumes():
  print('Spinning cluster down.')
  cluster_down()
  print('Deleting files.')
  shutil.rmtree(volumes_dir)

def input_with_validator(prompt, failure_msg, validator_func):
  while True:
    val = input(prompt)
    if validator_func(val):
      return val
    else:
      print(failure_msg)

def validate_project_name(val):
  pattern = re.compile(r'\W')
  return not pattern.search(val) and val.isalnum()

def validate_directory(val):
  return os.path.exists(val)

def validate_yn(val):
  l = val.lower()
  return l == 'y' or l == 'n'

def configure():
  config_path = os.path.join(root_dir, 'config.json')
  config = {}
  if not os.path.isfile(config_path):
    proj_name = input_with_validator(
      'Please input your project name: ',
      'No spaces or non-alphanumeric characters allowed.',
      validate_project_name
    )
    src_dir = input_with_validator(
      'Please input your playground src directory: ',
      'Please use a valid directory name that exists.',
      validate_directory
    )
    data_dir = input_with_validator(
      'Please input your data directory: ',
      'Please use a valid directory name that exists.',
      validate_directory
    )
    config['project_name'] = proj_name
    config['source_dir'] = src_dir
    config['data_dir'] = data_dir
    with open(config_path, 'w') as fp:
      json.dump(config, fp, indent=2)
    os.environ['project_name'] = proj_name
    os.environ['source_dir'] = src_dir
    os.environ['data_dir'] = data_dir
    print('Config written.')
  else:
    with open(config_path, 'r') as fp:
      config = json.load(fp)
    os.environ['project_name'] = config['project_name']
    os.environ['source_dir'] = config['source_dir']
    os.environ['data_dir'] = config['data_dir']
    print('Config read.')
  
  return config

def build_img_cmd(config, args):
  build_img()

def format_hdfs_cmd(config, args):
  format_hdfs(config)

def ingest_data_cmd(config, args):
  ingest_data(config)

def setup_hive_cmd(config, args):
  setup_hive(config)

def setup_sqoop_cmd(config, args):
  setup_sqoop(config)

def cluster_up_cmd(config, args):
  cluster_up()

def start_hadoop_daemons_cmd(config, args):
  start_hadoop_daemons(config)

def start_hive_server_cmd(config, args):
  start_hive_server(config)

def cluster_down_cmd(config, args):
  cluster_down()

def setup_cmd(config, args):
  setup(config)

def start_cmd(config, args):
  start(config)

def stop_cmd(config, args):
  stop()

def destroy_volumes_cmd(config, args):
  if args.skip_confirm:
    destroy_volumes()
    return

  result = input_with_validator('Are you sure you want to delete directory "%s" and all of its files? y/n: ' % (volumes_dir),
    'Please use "y" or "n".',
    validate_yn
  ).lower()
  if result == 'y':
    destroy_volumes()
  else:
    print('Cancelling.')

def main():
  
  parser = argparse.ArgumentParser(prog='playground', description='HDFS, Hive, and SQL Playground')
  parser.set_defaults(func=None)

  subparsers = parser.add_subparsers()

  subparsers.add_parser('build-img', help='TODO: help').set_defaults(func=build_img_cmd)
  subparsers.add_parser('format-hdfs', help='TODO: help').set_defaults(func=format_hdfs_cmd)
  subparsers.add_parser('ingest-data', help='TODO: help').set_defaults(func=ingest_data_cmd)
  subparsers.add_parser('setup-hive', help='TODO: help').set_defaults(func=setup_hive_cmd)
  subparsers.add_parser('setup-sqoop', help='TODO: help').set_defaults(func=setup_sqoop_cmd)
  subparsers.add_parser('cluster-up', help='TODO: help').set_defaults(func=cluster_up_cmd)
  subparsers.add_parser('start-hadoop', help='TODO: help').set_defaults(func=start_hadoop_daemons_cmd)
  subparsers.add_parser('start-hive', help='TODO: help').set_defaults(func=start_hive_server_cmd)
  subparsers.add_parser('cluster-down', help='TODO: help').set_defaults(func=cluster_down_cmd)
  subparsers.add_parser('setup', help='TODO: help').set_defaults(func=setup_cmd)
  subparsers.add_parser('start', help='TODO: help').set_defaults(func=start_cmd)
  subparsers.add_parser('stop', help='TODO: help').set_defaults(func=stop_cmd)
  destroy_vol_p = subparsers.add_parser('destroy-vol', help='TODO: help')
  destroy_vol_p.add_argument('--skip-confirm', '-y', action='store_true')
  destroy_vol_p.set_defaults(func=destroy_volumes_cmd)

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