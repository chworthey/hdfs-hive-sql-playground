<#
    .SYNOPSIS
        Copyright 2021 Patrick S. Worthey
        Runs all of the Hive and SQL operations in series, PowerShell-style.
#>
$PROJECT_DIR = Resolve-Path (Join-Path $PSScriptRoot ../)
$MAIN_DIR = $PSScriptRoot

$PY_PATH = Join-Path $PROJECT_DIR playground.py
$SRC_DIR = Join-Path $MAIN_DIR src
$DATA_DIR = Join-Path $MAIN_DIR data
$VOL_DIR = Join-Path $MAIN_DIR volumes

# We can set the config variables here instead of relying on config.json
$CONFIG_ARGS = "-p `"example`" -s `"$SRC_DIR`" -d `"$DATA_DIR`" -v `"$VOL_DIR`""

$tasks = @(
  # Only needs to be run once ever (unless you destroy the volumes)
  "python $PY_PATH $CONFIG_ARGS setup -y",

  # Boots up the cluster with all daemons running
  "python $PY_PATH $CONFIG_ARGS start",

  # Creates an external hive table pointing to the astro data
  "python $PY_PATH $CONFIG_ARGS exec-hive-file -f `"hive/create_m33_raw_ext_tbl.hql`"",

  # Creates a schematized view on the external hive table (extracts columns from text)
  "python $PY_PATH $CONFIG_ARGS exec-hive-file -f `"hive/create_m33_schem_view.hql`"",

  # Runs a query to check the output of the view
  "python $PY_PATH $CONFIG_ARGS exec-hive-query -e `"SELECT * FROM m33_schem LIMIT 100`"",

  # Creates a new Hive table stored as CSV, and inserts the view
  "python $PY_PATH $CONFIG_ARGS exec-hive-file -f `"hive/create_insert_m33_tbl.hql`"",

  # Runs a query to check the contents of the hive table
  "python $PY_PATH $CONFIG_ARGS exec-hive-query -e `"SELECT * FROM m33 LIMIT 100`"",

  # Creates a new SQL database on the SQL Server node
  "python $PY_PATH $CONFIG_ARGS sql-exec-file -f `"sql/create_astro_database.sql`"",

  # Creates an empty landing table for future export
  "python $PY_PATH $CONFIG_ARGS sql-exec-file -f `"sql/create_m33_tbl.sql`"",

  # Exports data from CSV to the SQL table using Sqoop
  "python $PY_PATH $CONFIG_ARGS sqoop-export --export-dir `"/user/hive/warehouse/m33`" --sql-table `"m33`" --database-name `"astroDB`"",

  # Runs an SQL query to check the table
  "python $PY_PATH $CONFIG_ARGS sql-exec-query -q `"SELECT TOP 100 * FROM m33`" -d `"astroDB`"",

  # Spins down the cluster
  "python $PY_PATH $CONFIG_ARGS stop"
)

foreach ($task in $tasks) {
  try {
    echo "########################################################"
    echo "runall.ps1 -- Starting task: $task"
    echo "########################################################"
    Invoke-Expression $task
  }
  catch {
    exit 1
  }
}