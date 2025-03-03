#!/usr/bin/env python3
'''
Script to create the Bacula Backup files under "Job" and "Fileset"
Takes input: "-server" - the server the files are on
"-path" - the path to the files (e.g. /mnt/data/seblab-data)
Optional: "-snapoff" (defaults to On) - use the FS Snapshot plugin
"-schedule" - pick a pre-defined Schedule for the backup. If not set the job won't auto-run.
"-bpath" - If you have Bacula installed somewhere weird.
Assumes the client (i.e. the server we are backing up from) has already been added to Bacula!
'''
import argparse
import platform
import bacula_functions as bf

def main():
    '''
    Parses the variables passed in, calls the appropriate other parts!
    '''
    parser = argparse.ArgumentParser(description="Bacula Fileset & Job Creation Script.")
    parser.add_argument("-server", help="The file-server the dataset is on (String)", required=True)
    parser.add_argument("-path", help="The path to the files we're backing up (String)",
        required=True)
    parser.add_argument("-setname", help="ZFS pool name", required=True)
    parser.add_argument("-schedule", help="The schedule to be used, if not set it won't run",
        choices=["First", "Second", "Third"])
    parser.add_argument("-snapoff",
        help="Do not use ZFS Snapshots for this backup set (Boolean switch)",
        action="store_true")
    parser.add_argument("-bpath",
        help="Path to the Bacula Config Folder - defaults to /opt/bacula/etc/conf.d/Director/",
        default="/opt/bacula/etc/conf.d/Director/")
    args = parser.parse_args()
    ### SCRATCH POOL & LIBRARY CHANGER ###
    scratch_pool = "Scratch"
    tape_changer = "QuantumLib1"
    ######################################
    #Check that Bacula is happy before doing anything at all:
    try:
        bf.check_bacula("Start of script, no action taken")
    except bf.BConsoleError as e:
        print("Error checking Bacula Config before starting!")
        print(e)
        raise e
    #We assume now that Bacula is already happy, and we can go ahead with creating things
    #Set the Bacula Configuration Folder path
    # platform.node() gets the current host's name
    if args.bpath.endswith("/"):
        conf_path = args.bpath + platform.node().split(".")[0] + "-dir/"
    else:
        conf_path = args.bpath + "/" + platform.node().split(".")[0] + "-dir/"
    if args.snapoff:
        #Snapoff is set!
        snapshot = False
    else:
        snapshot = True
    server = args.server.split(".")[0]
    #Set Bacula Job Class things:
    bacula_job = bf.BaculaJob(server, args.setname, "zbkp_" + args.setname + "_fs",
                           "zbkp_" + args.setname + "_job", args.path, args.schedule, snapshot,
                           tape_changer, scratch_pool)
    bf.create_pool(bacula_job, conf_path)
    try:
        bf.check_bacula("Created Pool files")
    except bf.BConsoleError as e:
        print("Error with Bacula Config, created pool files")
        print(e)
        raise
    bf.create_fileset(bacula_job, conf_path)
    try:
        bf.check_bacula("Created Fileset Files")
    except bf.BConsoleError as e:
        print("Error with Fileset Files")
        print(e)
        raise
    bf.create_job(bacula_job, conf_path)
    try:
        bf.check_bacula("Created Fileset and Job files")
    except bf.BConsoleError as e:
        print("Error with Job Files")
        print(e)
        raise
    try:
        reloaded = bf.reload_bacula()
        restarted = bf.bacula_restart
    except bf.subprocess.CalledProcessError:
        print("!!!!!!!!!!!!!!!!!!!!")
        print("!!! WARNING !!!")
        print("Something went wrong when running 'systemctl restart bacula-dir'")
        print("You MUST check this manually!")
        print("!!!!!!!!!!!!!!!!!!!!")
    if not restarted:
        print("!!!Alert!!!")
        print("Jobs are running, so the Director has not been restarted")
        if reloaded:
            print("This is probably fine, but you should run 'systemctl restart bacula-dir' manually",
                "once the current jobs are finished.")
        else:
            #Reloaded is False, so it's not so fine...
            print("!!! In addition, Bacula refused to Reload, so new items have not been loaded !!!")
            print("!!! You will need to restart the Bacula Director manually for them to appear !!!")
            print("!!! Use the command 'systemctl restart bacula-dir' once running jobs are completed !!!")
    raise SystemExit
if __name__ == '__main__':
    main()
