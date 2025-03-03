#!/usr/bin/python3
'''
Script to Restore a single random file
Checksums it against an existing file to ensure all is correct
Sends email if there is a problem
Assumes there is a ".zfs/<date>-monthly" snapshot
'''
import subprocess
from datetime import datetime
import csv
import hashlib
import os
import random
import re
import bacula_functions as bf

def main():
    '''
    Main section of script, calls various other functions!
    '''
    #Local Variables:
    email_address = "<NOTIFICATION EMAIL>"
    local_restore_path = "/tmp/restore/"
    log_file = "/var/log/bacula/logs/audit" + "-" + datetime.today().strftime('%Y-%m-%d') + ".log"
    servers = [ '<SERVER1>', '<SERVER2>', '<SERVER3>' ]
    audit_file_path = "/var/log/zfs-audit-list/"
    zfs_datasets = []
    auditing_list = []
    #Main Script:
    write_log(log_file, "Starting Audit Job")
    #TODO: Likely change this to use SSH Key-based login instead
    username = input("Enter SSH username:")
    try:
        for server in servers:
            zfs_datasets.extend(ssh_zfs(server, username))
    except ValueError as e:
        write_log(log_file, e)
        bf.error_email(email_address, f"Error with ZFS output from {server}")
        raise f"Error with ZFS output from {server}"
    except subprocess.CalledProcessError as e:
        write_log(log_file, e)
        bf.error_email(email_address, f"Problem SSH'ing to {server}\n{e}")
        raise e
    if os.path.exists(audit_file_path):
        auditing_list = audit_file_read(audit_file_path)
        for zfs_item in zfs_datasets:
            #Loop through the list and add any missing items:
            if not any(d['path'] == zfs_item[0] for d in auditing_list):
                write_log(log_file, f"Dataset: {zfs_item[0]} not in Audit List, adding it")
                auditing_list.append({"path" : zfs_item[0], "server" : zfs_item[1], "checked" : "0"})
                audit_file_write(audit_file_path, auditing_list)
    else:
        write_log(log_file, f"NOTE: Audit file {audit_file_path} does not exist, creating it")
        #Audit List File doesn't exist, so create the file, and do the list
        for item in zfs_datasets:
            auditing_list.append({"path" : item[0], "server" : item[1], "checked" : "0"})
        audit_file_write(audit_file_path, auditing_list)
    auditing_list = sorted(auditing_list, key = lambda no_check: no_check['checked'])
    if int(auditing_list[0]['checked']) == int(auditing_list[1]['checked']):
        #smallest 2 jobs' numbers are the same, so we should find all of them and pick a random one
        subset_list = []
        subset_list.append(auditing_list[0])
        for audit_item in auditing_list:
            if audit_item['checked'] == subset_list[-1]['checked']:
                subset_list.append(audit_item)
        dataset = random.choice(subset_list)
    else:
        dataset = auditing_list[0]
    try:
        monthly_path = get_latest_monthly(dataset, username)
        write_log(log_file, f"Server: {dataset["server"]} Dataset: {dataset["path"]} Snapshot: {monthly_path} chosen for Audit")
        audit_file = get_files(server, monthly_path, username)
        remote_checksum = checksum_file(server, audit_file, username)
        #print(f"File: {audit_file} / checksum: {remote_checksum}")
        write_log(log_file, f"File {audit_file} Checksum: {remote_checksum}")
    except subprocess.CalledProcessError as exc:
        write_log(log_file, f"Error SSH'ing to {server}")
        bf.error_email(email_address, f"Error SSH'ing to {server}")
        raise ConnectionError(f"Error connecting to {server}") from exc
    except IOError as e:
        bf.error_email(email_address, f"IOError! \n{e}")
        raise e
    file_tuple = os.path.split(audit_file)
    #RegEx magic to remove the whole ".zfs/snapshot/zback-name" from the string, so Bacula has a proper path for the file
    backups_file_path = re.sub(r'\/\.zfs\/snapshot\/\zback:\d{4}-\d{2}-\d{2}-\d{4}:monthly', '', audit_file)
    local_file_path = local_restore_path + file_tuple[1]
    #Update the Audit List, as a Restore can take a long time
    #we don't want a second restore of the same dataset due to waiting.
    updated_audit_list = []
    for list_item in auditing_list:
        if list_item['path'] == dataset['path']:
            updated_audit_list.append({'path': f"{list_item['path']}", 'server': f"{list_item['server']}", 'checked': int(list_item['checked']) +1})
        else:
            updated_audit_list.append(list_item)
    audit_file_write(audit_file_path, updated_audit_list)
    try:
        #Make sure the restore-folder already exists:
        os.makedirs(local_restore_path, exist_ok=True)
        restore_client = dataset['server'].split(".")[0] + "-fd"
        #Call the Restore Function, store the JobID returned:
        restore_status, restore_jobid = bf.bacula_restore(restore_client, file_tuple[0], backups_file_path, local_restore_path)
        if restore_status == "Restore OK":
            write_log(log_file, f"Restored file {local_restore_path}, Job: {restore_jobid}")
        else:
            #What do we do when it didn't restore OK?
            raise RuntimeError (f"Restore Error! Job: {restore_jobid} \n Status: {restore_status}")
    except (subprocess.CalledProcessError, bf.BConsoleError, RuntimeError) as e:
        #BConsoleError is one we manually raise, handle it the same though!
        write_log(log_file, "!!!ERROR!!!")
        write_log(log_file, "Bacula Restore failed")
        write_log(log_file, e)
        bf.error_email(email_address, f"Error with Bacula Restore for {dataset['server']} - {dataset['path']}\n {e}")
        raise e # we need to quit out here...
    try:
        local_checksum = checksum_file("local", local_file_path, "none")
    except FileNotFoundError as e:
        print(f"File {local_file_path} not found")
        write_log(log_file, f"Local restore file: {local_file_path} not found!")
        bf.error_email(email_address, f"Can't find file {local_file_path} after restore!\n{e}")
        raise (f"File {local_file_path} not found") from e
    write_log(log_file, f"Restored file checksum: {local_checksum}")
    #Compare the checksums:
    if local_checksum != remote_checksum:
        #We've got a problem!
        write_log(log_file, "ERROR! Checksums do not match!")
        write_log(log_file, f"Remote Checksum: {remote_checksum} <> Local Checksum: {local_checksum}")
        bf.send_email(email_address, "Checksum failed", f"Failed check for: {dataset}")
    else:
        #They do match!
        write_log(log_file, "Checksums match!")
    #Cleanup file:
    try:
        os.remove(local_file_path)
    except OSError:
        write_log(log_file, f"ERROR! Can't remove {local_file_path}")
        bf.error_email(email_address, f"Error deleting {local_file_path}")
    #If we get here, everything worked!
    write_log(log_file, "Audit completed successfully")

def audit_file_write(audit_file_path:str, audit_list:list):
    '''
    Function to write Audit File as CSV in format
    mountpoint, server, checked
    '''
    with open(audit_file_path, 'w', encoding='utf-8') as csv_file:
        writer = csv.writer(csv_file, dialect="excel")
        for row in audit_list:
            writer.writerow([row[0], row[1], row[2]])

def audit_file_read(audit_file_path) -> list:
    '''
    Function to read the Audit File (CSV)
    Returns a list of Dictionary items, sorted by the "checked" field
    '''
    audit_list = []
    if os.path.exists(audit_file_path):
        try:
            with open(audit_file_path, 'r', encoding='utf-8') as csv_file:
                reader = csv.DictReader(csv_file, fieldnames=("path", "server", "checked"), dialect='excel')
                for row in reader:
                    audit_list.append(row)
        except IOError as exc:
            raise(f"Error reading Audit file {audit_file_path}") from exc
    else:
        raise FileNotFoundError
    return sorted(audit_list, key = lambda no_check: no_check['checked'])

def get_latest_monthly(dataset, username) -> str:
    '''
    Hacky function to get the "latest" monthly snapshot folder
    "dataset" should be a Dict with Path, Server, Checked
    '''
    cmd = f"ls -td -- {dataset["path"]}/.zfs/snapshot/*monthly* | head -n 1"
    try:
        ssh_out = subprocess.run(['ssh', f'{username}@{dataset["server"]}', cmd], capture_output=True, text=True, check=True).stdout
    except subprocess.CalledProcessError:
        print(f"Error with SSH to {dataset["server"]}")
        raise
    return ssh_out.strip()

def get_files(server, path, username) -> str:
    '''
    Function to get a random file list to test restore
    '''
    ssh_cmd = f"find {path} -type f -size -50M -mtime +35 | shuf -n1"
    try:
        audit_file = subprocess.run(['ssh', f'{username}@{server}', ssh_cmd], capture_output=True,
                                    text=True, check=True).stdout
    except subprocess.CalledProcessError:
        print(f"Error with SSH to {server}")
        raise
    return audit_file.strip()

def checksum_file(server, audit_file, username) -> str:
    '''
    Function to run sha1sum on file on either local or remote server
    '''
    if server != "local":
        ssh_checksum_cmd = f"sha1sum {audit_file} | " + r"sed -e 's/^\(.\{40\}\).*/\1/'"
        audit_checksum = subprocess.run(['ssh', f'{username}@{server}', ssh_checksum_cmd],
                                        capture_output=True, text=True, check=True).stdout
        return audit_checksum
    else:
        #Taken from: https://www.geeksforgeeks.org/python-program-to-find-hash-of-file/
        hash_func = hashlib.new("sha1")
        with open(audit_file, 'rb') as file:
            # Read the file in chunks of 8192 bytes
            while chunk := file.read(8192):
                hash_func.update(chunk)
        return hash_func.hexdigest()

def ssh_zfs(server, username) -> list[tuple]:
    '''
    Function to SSH on to servers, get ZFS output, return list of ZFS output
    Returns List of Tuples: "mountpoint", "server"
    '''
    zfs_mountpoints = []
    zfs_command = "zfs list -Ho mountpoint"
    #We only need the ZFS Mountpoint really...
    try:
        ssh_output = (subprocess.run(['ssh', f'{username}@{server}', zfs_command],
                                     capture_output=True, text=True, check=True).stdout).split("\n")
    except subprocess.CalledProcessError:
        print(f"Error with SSH to {server}")
        raise
    if len(ssh_output) > 1: #Will return 1 item even if it's just the Pool
        for zline in ssh_output:
            if zline != "none": #If the mountpoint isn't "none"
                zfs_mountpoints.append(zline)
        return list(tuple(filter(None,zfs_mountpoints)),server)
    else:
        #ZFS output was empty - this is bad!
        raise ValueError(f"ZFS list from {server} was empty!")

def write_log(file:str, content:str):
    '''
    Function to write to a logfile
    Will create it if required, or append
    '''
    line = "[" + datetime.now().strftime("%H:%M:%S") + "] " + content
    try:
        with open(file, "a", encoding="utf-8") as log_file: #Append will create if required
            log_file.write(line)
    except IOError as exc:
        print(f"Warning, error writing to {file}")
        raise IOError(f"Error writing to {file}") from exc

if __name__ == '__main__':
    main()
