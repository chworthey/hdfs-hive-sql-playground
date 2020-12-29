"""
Runs all of the Hive and SQL operations in series, python-style.
"""
import os
import sys
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(SCRIPT_DIR))
import playground

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
  playground.destroy_volumes(config)
  playground.setup(config)
  playground.start(config)
  playground.exec_hive_file(config, 'hive/create_m33_raw.hql')
  playground.exec_hive_file(config, 'hive/create_m33_schem_view.hql')

if __name__ == '__main__':
  main()
