# Python Scripts
This is a collection of some Python Scripts I have written.

## Note on Bacula Scripts
Unless noted otherwise, they are all expected to run on the Director Server, and for email-sending a local postfix mail-forward is expected.

### bacula_functions.py
This is a libary-file of various functions to interact with Bacula using Python. Should be imported to the required Python scripts, and then the functions, dataclasses, error-classes can be referenced.

### bacula_create.py
Python script (uses the bacula_functions.py file for various functions) to make creating new jobs in Bacula simpler (creates Job and Fileset files), and callable from other scripts.

Should be run on the Director server.

Takes input: "-server" - the server the files are on

"-path" - the path to the files (e.g. /mnt/data/some-dataset)

Optional: "-snapoff" (defaults to On) - use the FS Snapshot plugin

"-schedule" - pick a pre-defined Schedule for the backup. If not set the job won't auto-run.

"-bpath" - If you have Bacula installed somewhere weird.

Assumes the client (i.e. the server we are backing up from) has already been added to Bacula!

### bacula_job_check.py
Script to be run via Cron Job. SSH's onto servers, gets a list of all mounted ZFS datasets, then checks for Bacula Jobs for them. If any jobs are missing it sends an email listing them to an address.

## bacula_audit.py
Script to be run via Cron job. Will work through all ZFS datasets on listed servers, pick a small-ish file and SHA1-sum it, then attempt to restore the same file from a backup and compare the SHA1-sum. In the event of them not matching, sends an email.

Expects all ZFS datasets to have a ".zfs/<date>-monthly" snapshot to check against.
