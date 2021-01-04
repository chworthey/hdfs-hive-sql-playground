"""
Runs all of the Hive and SQL operations in series, python-style.
"""
import os
import sys
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
import playground

def print_task_doc(task_name):
  """
  Prints the task name noticeably so we can see the corresponding console output
  """
  print('########################################################')
  print('runall.py -- Starting task: %s' % (task_name))
  print('########################################################')

def main():
  """
  Main program entry point.
  """
  config = playground.Config( \
    project_name='example', \
    source_dir=os.path.join(SCRIPT_DIR, 'src'), \
    data_dir=os.path.join(SCRIPT_DIR, 'data'), \
    volumes_dir=os.path.join(SCRIPT_DIR, 'volumes') \
  )

  # start fresh each time
  print_task_doc('setup')
  playground.setup(config)

  print_task_doc('start')
  playground.start(config, wait=True)

  print_task_doc('hive_script1')
  playground.exec_hive_file(config, 'hive/create_m33_raw_ext_tbl.hql')

  print_task_doc('hive_script2')
  playground.exec_hive_file(config, 'hive/create_m33_schem_view.hql')

  print_task_doc('hive_query_check1')
  playground.exec_hive_query(config, 'SELECT * FROM m33_schem LIMIT 100')

  print_task_doc('hive_script3')
  playground.exec_hive_file(config, 'hive/create_insert_m33_tbl.hql')

  print_task_doc('hive_query_check2')
  playground.exec_hive_query(config, 'SELECT * FROM m33 LIMIT 100')

  print_task_doc('sql_script1')
  playground.sql_exec_file(config, 'sql/create_astro_database.sql')

  print_task_doc('sql_script2')
  playground.sql_exec_file(config, 'sql/create_m33_tbl.sql')

  print_task_doc('sqoop_export1')
  playground.sqoop_export(config, '/user/hive/warehouse/m33', 'm33', database_name='astroDB')

  print_task_doc('sql_query_check1')
  playground.sql_exec_query(config, 'SELECT TOP 100 * FROM m33', database_name='astroDB')

  print_task_doc('stop')
  playground.stop(config)

if __name__ == '__main__':
  main()
