"""
Copyright 2021 Patrick S. Worthey
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
  # We can set the config variables here instead of relying on config.json
  config = playground.Config( \
    project_name='example', \
    source_dir=os.path.join(SCRIPT_DIR, 'src'), \
    data_dir=os.path.join(SCRIPT_DIR, 'data'), \
    volumes_dir=os.path.join(SCRIPT_DIR, 'volumes') \
  )

  # Only needs to be run once ever (unless you destroy the volumes)
  print_task_doc('setup')
  playground.setup(config)

  # Boots up the cluster with all daemons running
  print_task_doc('start')
  playground.start(config, wait=True)

  # Creates an external hive table pointing to the astro data
  print_task_doc('hive_script1')
  playground.exec_hive_file(config, 'hive/create_m33_raw_ext_tbl.hql')

  # Creates a schematized view on the external hive table (extracts columns from text)
  print_task_doc('hive_script2')
  playground.exec_hive_file(config, 'hive/create_m33_schem_view.hql')

  # Runs a query to check the output of the view
  print_task_doc('hive_query_check1')
  playground.exec_hive_query(config, 'SELECT * FROM m33_schem LIMIT 100')

  # Creates a new Hive table stored as CSV, and inserts the view
  print_task_doc('hive_script3')
  playground.exec_hive_file(config, 'hive/create_insert_m33_tbl.hql')

  # Runs a query to check the output of the hive table
  print_task_doc('hive_query_check2')
  playground.exec_hive_query(config, 'SELECT * FROM m33 LIMIT 100')

  # Creates a new SQL database on the SQL Server node
  print_task_doc('sql_script1')
  playground.sql_exec_file(config, 'sql/create_astro_database.sql')

  # Creates an empty landing table for future export
  print_task_doc('sql_script2')
  playground.sql_exec_file(config, 'sql/create_m33_tbl.sql')

  # Exports data from CSV to the SQL table using Sqoop
  print_task_doc('sqoop_export1')
  playground.sqoop_export(config, '/user/hive/warehouse/m33', 'm33', database_name='astroDB')

  # Runs an SQL query to check the table
  print_task_doc('sql_query_check1')
  playground.sql_exec_query(config, 'SELECT TOP 100 * FROM m33', database_name='astroDB')

  # Spins down the cluster
  print_task_doc('stop')
  playground.stop(config)

if __name__ == '__main__':
  main()
