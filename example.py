from eddie_helper.make_scripts import run_python_script, run_stage_script

sorter_name = "kilosort4"
mouse_days = {25: [20,22]}

for mouse, days in mouse_days.items():
    for day in days:

        stageout_dict = {
            f'/exports/eddie/scratch/chalcrow/harry/data/M{mouse}/D{day}/results/': '/exports/cmvm/datastore/sbms/groups/CDBS_SIDB_storage/NolanLab/ActiveProjects/Chris/Cohort12/derivatives/M{mouse}/D{day}/'
        }

        run_python_script(f"sort.py {mouse} {day} {sorter_name}", username="chalcrow", cores=8, job_name=f"{mouse}D{day}_sort")
        run_stage_script(stageout_dict, hold_jid=f"{mouse}D{day}_sort")