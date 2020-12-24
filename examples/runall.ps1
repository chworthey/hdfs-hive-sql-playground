$project_dir = "$PSScriptRoot../"
Push-Location $project_dir

python playground.py exec-hive-file -f "hive/create_m33_raw.hql"
python playground.py exec-hive-file -f "hive/create_m33_schem_view.hql"
