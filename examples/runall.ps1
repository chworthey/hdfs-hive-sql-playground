$PROJECT_DIR = Resolve-Path (Join-Path $PSScriptRoot ../)
$MAIN_DIR = $PSScriptRoot

$PY_PATH = Join-Path $PROJECT_DIR playground.py
$SRC_DIR = Join-Path $MAIN_DIR src
$DATA_DIR = Join-Path $MAIN_DIR data
$VOL_DIR = Join-Path $MAIN_DIR volumes

$CONFIG_ARGS = "-p `"example`" -s `"$SRC_DIR`" -d `"$DATA_DIR`" -v `"$VOL_DIR`""

$tasks = @(
  "python $PY_PATH $CONFIG_ARGS destroy-vol -y",
  "python $PY_PATH $CONFIG_ARGS setup",
  "python $PY_PATH $CONFIG_ARGS start",
  "python $PY_PATH $CONFIG_ARGS exec-hive-file -f `"hive/create_m33_raw.hql`"",
  "python $PY_PATH $CONFIG_ARGS exec-hive-file -f `"hive/create_m33_schem_view.hql`"",
  "python $PY_PATH $CONFIG_ARGS exec-hive-query -e `"SELECT * FROM m33_schem LIMIT 100`"",
  "python $PY_PATH $CONFIG_ARGS stop"
)

foreach ($task in $tasks) {
  try {
    Invoke-Expression $task
  }
  catch {
    exit 1
  }
}