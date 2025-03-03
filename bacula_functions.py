'''Library of Bacula Functions for Python'''
import subprocess
from dataclasses import dataclass
import os
import shutil
from datetime import datetime
import smtplib
import platform
from email.message import EmailMessage
import re

class BConsoleError(Exception):
    '''Bacula Console Error - don't do anything, just another Exception'''

@dataclass
class BaculaInfo():
    '''
    Dataclass for Bacula Information from config files
    '''
    bacula_client: str
    bacula_fileset: str
    bacula_schedule: str
    bacula_file_path: str
    bacula_client_server: str

@dataclass
class BaculaJob():
    '''
    Dataclass for Bacula Job Information
    '''
    server: str #File Server
    set_name: str #ZFS Set name
    bacula_fs_name: str #Name of the Bacula Fileset
    job_name: str #Name of the Bacula Job
    path: str #Path to the files to backup
    sched: str #Schedule to be used
    snapshot: bool #If Snapshots are to be used
    autochanger: str #Tape Autochanger to be used
    scratch: str #Scratch Pool

def error_email(error_message:str, email_address:(str | list)):
    '''
    Small function to call other email function
    Simply takes either a list or string for email address,
    and an error message in as Body of message
    Has "ERROR WITH AUDIT YYYY-MM-DD HH:MM" in subject
    '''
    subject = f"ERROR WITH AUDIT {datetime.today().strftime('%Y-%m-%d %H:%M')}"
    send_email(email_address, subject, error_message)

def send_email(email_address:(str | list), subject:str, body:str):
    '''
    Function to send email
    Takes input of email address (string or list), subject, body, if it should Raise an error
    Takes input of email address (string or list), subject, body, if it should Raise an error
    if multiple email addresses are required then they should be a long string with a comma between them
    '''
    if isinstance(email_address, list):
        for email in email_address:
            if email_address == "":
                #first entry, so no comma required:
                to_address = email
            else:
                to_address = to_address + "," + email
    elif isinstance(email_address, str):
        to_address = email_address
    else:
        #not a string or a list - we can't handle that!
        raise TypeError("Email address not string or list!")
    if isinstance(email_address, list):
        for email in email_address:
            if email_address == "":
                #first entry, so no comma required:
                to_address = email
            else:
                to_address = to_address + "," + email
    elif isinstance(email_address, str):
        to_address = email_address
    else:
        #not a string or a list - we can't handle that!
        raise TypeError("Email address not string or list!")
    msg = EmailMessage()
    msg['From'] = 'audit'
    msg['To'] = to_address
    msg['To'] = to_address
    msg['Subject'] = subject
    msg.set_content(body)
    with smtplib.SMTP('localhost') as s:
        s.send_message(msg)

def set_perms(files:str, group:str, user:str, perms:int="640"):
    '''
    Sets file permissions by default to rw,r,nothing (640)
    Optional parameter for setting different perm octal
    Octal must be valid (3-numbers, 0-7)
    Sets File & Group Owner to those passed in
    '''
    perm_oct = int("0o" + perms)
    if isinstance(files, list):
        for file in files:
            os.chmod(file, perm_oct)
            shutil.chown(file, user, group)
    else:
        os.chmod(files, perm_oct)
        shutil.chown(files, user, group)

def create_pool(bacula_job: BaculaJob, conf_path):
    '''
    Creates per-job pools for Full & Diff
    Diff Volume Retention is 6 months, Job Retention is 5 months
    Full Volume Retention is 8 months, Job Retention is 7 months
    This means Jobs are removed from the Catalogue after the Job Retention
    The Tapes are Recycled after the Volume Retention period
    '''
    pool_dict = {
        "full" : {
            "path" : conf_path + "Pool/" + bacula_job.set_name + "_full_pool.cfg",
            "JobRetention" : "18144000",
            "VolumeRetention" : "20736000"
        },
        "diff" : {
            "path" : conf_path + "Pool/" + bacula_job.set_name + "_diff_pool.cfg",
            "JobRetention" : "12960000",
            "VolumeRetention" : "15552000"
        }
    }
    for diff_full, values in pool_dict.items():
        try:
            with open(values['path'], "w", encoding="utf-8") as pool_file:
                pool_file.write("Pool {\n")
                pool_file.write(f'  Name = "{bacula_job.set_name}_pool_{diff_full}"\n')
                pool_file.write(f'  Description = "{bacula_job.set_name} Tape {diff_full} Pool"\n')
                pool_file.write('  Catalog = "BaculaCatalog"')
                pool_file.write('  CleaningPrefix = "CLN"\n')
                pool_file.write(f'  JobRetention = {values["JobRetention"]}\n')
                pool_file.write('  PoolType = "Backup"\n')
                pool_file.write(f'  RecyclePool = "{bacula_job.scratch}"\n')
                pool_file.write(f'  ScratchPool = "{bacula_job.scratch}"\n')
                pool_file.write(f'  Storage = "{bacula_job.autochanger}"\n')
                pool_file.write(f'  VolumeRetention = {values["VolumeRetention"]}\n')
                pool_file.write('}\n')
            set_perms(values["path"], "bacula", "bacula")
        except IOError as exc:
            raise(f"Error writing {values['path']}") from exc

def check_create_def_job_def(bacula_job: BaculaJob, conf_path):
    '''
    Function to check if the "Default Job Definition" exists
    Create it if it doesn't.
    '''
    jd_path = conf_path + "JobDefs/Default_Tape_JD.cfg"
    if os.path.exists(jd_path):
        #If it already exists we assume it's OK
        return
    else:
        #Create the file:
        try:
            with open(jd_path, 'w', encoding="utf-8") as jdfile:
                jdfile.write("JobDefs {\n")
                jdfile.write('  Name = "Default_Tape_JD"\n')
                jdfile.write('  Description = "Default Tape Job Def"\n')
                jdfile.write('  Type = "Backup"\n')
                jdfile.write("  AllowDuplicateJobs = no\n")
                jdfile.write("  AllowMixedPriority = yes\n")
                jdfile.write("  CancelLowerLevelDuplicates = yes\n")
                jdfile.write("  CancelQueuedDuplicates = no\n")
                jdfile.write('  Messages = "Default"\n')
                jdfile.write(f"  Storage = \"{bacula_job.autochanger}\"\n")
                jdfile.write('  WriteBootstrap = "/opt/bacula/bsr/%c_%n.bsr"')
                jdfile.write("}\n")
            set_perms(jd_path, "bacula", "bacula")
        except IOError as exc:
            raise(f"Error writing {jd_path}") from exc

def create_fileset(bacula_job:BaculaJob, conf_path):
    '''
    Creates the Fileset for Bacula, takes the "BaculaJob" dataclass & bacula Config Path as input.
    '''
    fs_file_name = conf_path + "Fileset/" + bacula_job.bacula_fs_name + ".cfg"
    try:
        with open(fs_file_name, 'w', encoding="utf-8") as fsfile:
            fsfile.write("Fileset {\n")
            fsfile.write(f'  Name = "{bacula_job.bacula_fs_name}"\n')
            fsfile.write(f'  Description = "{bacula_job.server} - {bacula_job.set_name} Backup Fileset"\n')
            if bacula_job.snapshot:
                fsfile.write('  EnableSnapshot = yes\n')
            fsfile.write('  EnableVss = no\n')
            fsfile.write('  Include {\n')
            fsfile.write('   Options {\n')
            fsfile.write('    AclSupport = yes\n')
            fsfile.write('    Signature = Sha256\n')
            fsfile.write('    XattrSupport = yes\n')
            fsfile.write('   }\n')
            fsfile.write(f'   File = "{bacula_job.path}"\n')
            fsfile.write('  }\n')
            fsfile.write('}\n')
        set_perms(fs_file_name, "bacula", "bacula")
    except IOError as exc:
        raise(f"Error writing {fs_file_name}") from exc

def create_job(bacula_job: BaculaJob, conf_path):
    '''
    Creates the Job File for Bacula, takes the "BaculaJob" dataclass & bacula Config Path as input.
    Schedule must already exist!
    Will overwrite any other job-file with the same name.
    '''
    jobname = conf_path + "Job/" + bacula_job.job_name + ".cfg"
    try:
        with open(jobname, 'w', encoding="utf-8") as jobfile:
            jobfile.write('Job {\n')
            jobfile.write(f'  Name = {bacula_job.job_name}\n')
            jobfile.write(f'  Description = "{bacula_job.server} - {bacula_job.set_name} Backup Job"\n')
            jobfile.write(f'  Client = "{bacula_job.server}-fd"\n')
            jobfile.write(f'  DifferentialBackupPool = "{bacula_job.set_name}_pool_diff"\n')
            jobfile.write(f'  Fileset = "{bacula_job.bacula_fs_name}"\n')
            jobfile.write(f'  FullBackupPool = "{bacula_job.set_name}_pool_full"\n')
            jobfile.write(f'  Schedule = "{bacula_job.sched}"\n')
            jobfile.write('  JobDefs = "Default_Tape_JD"\n')
            jobfile.write(f'  Pool = "{bacula_job.set_name}_pool_full"\n')
            jobfile.write('}\n')
        set_perms(jobname, "bacula", "bacula")
    except IOError as exc:
        raise(f"Error writing {jobname}") from exc

def check_bacula(call_location):
    '''
    Just a wrapper for calling the Bacula "check files" function.
    Should catch any errors from this and kill the script
    Takes input of "call_location" to provide context for where this failed
    so we can cleanup if required!
    "/opt/bacula/bin/bacula-dir -u bacula -g bacula -t" is the command
    '''
    try:
        result = subprocess.run(["/opt/bacula/bin/bacula-dir", "-u", "bacula", "-g", "bacula", "-t"],
        check=True, universal_newlines=True)
    except subprocess.CalledProcessError as e:
        raise BConsoleError(f'Check Bacula failed at: {call_location}') from e
    if result.returncode != 0:
        raise BConsoleError(result.stdout)

def reload_bacula() -> bool:
    '''
    Just a wrapper to reload Bacula to read the newly created files.
    "echo reload | /opt/bacula/bin/bconsole" is the command
    Returns "True" if no error was logged, or False / raise an error if it was.
    '''
    reload_command = 'echo "reload" | /opt/bacula/bin/bconsole'
    try:
        result = subprocess.run(reload_command, check=True, shell=True, stdout=subprocess.PIPE)
        if "Please correct" in str(result):
            raise BConsoleError("Bad config, please check")
    except (subprocess.CalledProcessError, BConsoleError) as exception:
        raise BConsoleError from exception
    if result.returncode != 0:
        #In case the Try doesn't work...
        raise BConsoleError(result.stdout)
    if "Request ignored" in str(result.stdout):
        return False
    else:
        return True

def search_file(filename, search_string) -> str:
    '''
    Function to search a given file for a string
    Returns the value from the item searched for (assumes format of '<item> = "<value>"')
    Will return the <value> from between the ""
    '''
    try:
        with open(filename, 'r', encoding='utf-8') as searching_file:
            for line in enumerate(searching_file):
                if search_string in line[1]:
                    return line[1].split("\"")[1]
        return None #If we don't find anything, return None
    except IOError as e:
        raise e

def get_bacula_info(job_file_list, fileset_file_list, client_file_list) -> list[BaculaInfo]:
    '''
    Function to search Bacula Files for Client, Job info
    Returns a list of BaculaInfo type items with info
    '''
    client_file_info = {}
    info_list = []
    jobs_info = []
    fileset_info = {}
    ## Adding in additional Search items:
    #Get info from Job Files
    for job_file in job_file_list:
        jf_client = search_file(job_file, "Client")
        jf_fileset = search_file(job_file, "Fileset")
        jf_schedule = search_file(job_file, "Schedule")
        jobs_info.append({"Client": f"{jf_client}", "Fileset": f"{jf_fileset}", "Schedule": f"{jf_schedule}"})
    #Get info from Fileset Files:
    for fileset_file in fileset_file_list:
        fs_name = search_file(fileset_file, "Name")
        fs_path = search_file(fileset_file, "File ")
        fileset_info[fs_name] = [fs_path]
    #Get info from Client Files
    for client_file in client_file_list:
        cf_name = search_file(client_file, "Name")
        cf_address =  search_file(client_file, "Address")
        client_file_info[cf_name] = [cf_address]
    #Go through the list to stitch things together:
    for job_entry in jobs_info:
        try:
            address = client_file_info[job_entry["Client"]][0]
        except KeyError as exc:
            raise KeyError(f"{job_entry["Client"]} - Client File doesn't exist") from exc
        try:
            path = fileset_info[job_entry["Fileset"]][0]
        except KeyError as exc:
            raise KeyError(f"{job_entry["Fileset"]} - Fileset file doesn't exist") from exc
        info_list.append(BaculaInfo(job_entry["Client"], job_entry["Fileset"],
                                    job_entry["Schedule"], path, address))
    return info_list

def bacula_restore(src_serv:str, file:str, source_folder:str,
                restore_folder:str, res_client="localhost") -> tuple[str, str]:
    #https://www.bacula.org/15.0.x-manuals/en/console/Bacula_Enterprise_Console.html#784
    # May be able to use "wait" along with JobID?
    '''
    Function to create Bacula Restore job
    Takes "Source Servername", "File to restore", "Source File Path", "Restore folder path",
    "Restore client (if not specified then defaults to Director)"
    Returns a Tuple of "str: Restore Status", "str: JobID=<ID>"
    Returns a Tuple of "str: Restore Status", "str: JobID=<ID>"
    should wait for job, if the job fails then should notify by email
    '''
    #set "restore_status" now, so if it's not in the messages we handle it
    restore_status = "ERROR"
    #set "restore_status" now, so if it's not in the messages we handle it
    restore_status = "ERROR"
    if res_client == "localhost":
        res_client = platform.node().split(".")[0] + "-fd"
    else:
        if not res_client.endswith("-fd"):
            res_client = res_client + "-fd"
    if not source_folder.endswith("/"):
        source_folder = source_folder + "/"
    if not restore_folder.endswith("/"):
        restore_folder = restore_folder + "/"
    bc_bin = "/opt/bacula/bin/bconsole"
    bacula_params = (f"restore client={src_serv}-fd restoreclient={res_client} file={file} "
        "strip_prefix={source_folder} add_prefix={restore_folder} current done wait yes")
    #Run "Messages" to clear it first...
    subprocess.run(f"echo -e .messages | {bc_bin}", shell=True,stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, universal_newlines=True, check=True)
    bacula_params = (f"restore client={src_serv}-fd restoreclient={res_client} file={file} "
        "strip_prefix={source_folder} add_prefix={restore_folder} current done wait yes")
    bcmd = f"echo -e {bacula_params} | {bc_bin}"
    try:
        result = subprocess.run(bcmd, shell=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, universal_newlines=True, check=True)
        #Try doesn't necessarily return errors from bconsole, but returncode should.
        result = subprocess.run(bcmd, shell=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, universal_newlines=True, check=True)
        #Try doesn't necessarily return errors from bconsole, but returncode should.
        if result.returncode != 0:
            raise BConsoleError(result.stdout)
        restore_jobid = re.search(r"JobId=\d\d", result.stdout)[0]
        messages = subprocess.run(f"echo -e .messages | {bc_bin}", shell=True,stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, universal_newlines=True, check=True)
    except (subprocess.CalledProcessError, BConsoleError) as exception:
        raise BConsoleError from exception
    messages_list = messages.stdout.strip().split("\n")
    for item in messages_list:
        #Look for the "Termination" line and return that as the Restore_Status
        if "Termination:" in item:
            restore_status = item.split(":")[1].lstrip()
    return restore_status, restore_jobid

def bacula_restart() -> bool:
    '''
    Function to check if any jobs are running
    If no jobs are running try to restart the Director and return True if successful.
    If jobs are running then returns False and doesn't try.
    '''
    bc_bin = "/opt/bacula/bin/bconsole"
    running_jobs = []
    try:
        result = subprocess.run(f"echo -e .status dir running | {bc_bin}", shell=True,stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, universal_newlines=True, check=True)
        if result.returncode != 0:
            raise BConsoleError(result.stdout)
    except (subprocess.CalledProcessError, BConsoleError) as e:
        raise BConsoleError("Error running bconsole") from e
    messages = result.stdout.splitlines()
    for line in messages:
        if "is running" in line:
            running_job = line.split()
            running_jobs.append({"JobID": running_job[0], "Type": running_job[1],
                                "Level": running_job[2], "Files": running_job[3],
                                "Bytes": running_job[4] + running_job[5], "Name": running_job[6]})
    if len(running_jobs) > 0:
        #List has something, so we have running jobs
        return False
    else:
        #List is empty, so nothing is running - restart the Director
        result = subprocess.run("systemctl restart bacula-dir", shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, universal_newlines=True, check=True)
        if result.returncode != 0:
            raise subprocess.CalledProcessError
        #Systemctl doesn't necessarily fail when the restart does. Check if it's now running.
        status = subprocess.run("systemctl status bacula-dir", shell=True, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, universal_newlines=True, check=True)
        for line in status.stdout.splitlines():
            if "Active:" in line:
                list_line = line.split()
                if ((list_line[1] != "active") and (list_line[2] != "(running)")):
                    #Systemctl doesn't show Active & Running - something went wrong!
                    raise subprocess.CalledProcessError
        #If we get here then it should've restarted!
        return True
