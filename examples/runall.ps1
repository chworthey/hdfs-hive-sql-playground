$PROJECT_DIR = Resolve-Path (Join-Path $PSScriptRoot ../)
$MAIN_DIR = $PSScriptRoot

$PY_PATH = Join-Path $PROJECT_DIR playground.py
$SRC_DIR = Join-Path $MAIN_DIR src
$DATA_DIR = Join-Path $MAIN_DIR data
$VOL_DIR = Join-Path $MAIN_DIR volumes

$CONFIG_ARGS = "-p `"example`" -s `"$SRC_DIR`" -d `"$DATA_DIR`" -v `"$VOL_DIR`""

$tasks = @(
  "python $PY_PATH $CONFIG_ARGS setup -y",
  "python $PY_PATH $CONFIG_ARGS start",
  "python $PY_PATH $CONFIG_ARGS exec-hive-file -f `"hive/create_m33_raw_ext_tbl.hql`"",
  "python $PY_PATH $CONFIG_ARGS exec-hive-file -f `"hive/create_m33_schem_view.hql`"",
  "python $PY_PATH $CONFIG_ARGS exec-hive-query -e `"SELECT * FROM m33_schem LIMIT 100`"",
  "python $PY_PATH $CONFIG_ARGS exec-hive-file -f `"hive/create_insert_m33_tbl.hql`"",
  "python $PY_PATH $CONFIG_ARGS exec-hive-query -e `"SELECT * FROM m33 LIMIT 100`"",
  "python $PY_PATH $CONFIG_ARGS sql-exec-file -f `"sql/create_astro_database.sql`"",
  "python $PY_PATH $CONFIG_ARGS sql-exec-file -f `"sql/create_m33_tbl.sql`"",
  "python $PY_PATH $CONFIG_ARGS sqoop-export --export-dir `"/user/hive/warehouse/m33`" --sql-table `"m33`" --database-name `"astroDB`"",
  "python $PY_PATH $CONFIG_ARGS sql-exec-query -q `"SELECT TOP 100 * FROM m33`" -d `"astroDB`"",
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