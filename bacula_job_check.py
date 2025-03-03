#!/usr/bin/python3
'''
Script to check for Bacula Jobs for datasets
'''
import glob
import platform
import subprocess
from dataclasses import dataclass
import bacula_functions as bf

@dataclass
class ZFSOutput():
    '''
    Class for formatting ZFS output
    '''
    size: str
    size_b: int
    mount: str
    dataset: str
    server: str

def size_convert(size):
    '''
    Stupid function to convert to (Binary) Bytes
    Bases conversion on the last-character of the string
    Makes me die a little inside.
    '''
    last_char = size[-1]
    match last_char:
        case "K":
            size_b = float(size[:-1])*1000
        case "M":
            size_b = float(size[:-1])*1000*1000
        case "G":
            size_b = float(size[:-1])*1000*1000*1000
        case "T":
            size_b = float(size[:-1])*1000*1000*1000*1000
    return(int(size_b))

def ssh_zfs(servers, username):
    '''
    Function to SSH on to servers, get ZFS output, return list of ZFS output
    '''
    zfs_output = []
    zfs_command = "zfs list -Ho used,mountpoint,name | awk '{split($3, arr, \"/\")} {if(NR>1)print $1, $2, arr[2]}'"
    #AWK magic - formats the output so there is a single space between items, splits the "LIDO1/Dataset" to return just "Dataset"
    for server in servers:
        try:
            ssh_output = (subprocess.run(['ssh', f'{username}@{server}', zfs_command], capture_output=True, text=True, check=True).stdout).split("\n")
        except subprocess.CalledProcessError as e:
            print(f"Error with SSH to {server}")
            raise e
        for zline in ssh_output:
            zitem = zline.split(" ")
            #Zitem = [0]Used(string with T/G/M/K), [1]Mountpoint, [2]Dataset-name
            if (len(zitem) >1): #Skip over any empty ones
                size = size_convert(zitem[0])
                ztemp = ZFSOutput(size, zitem[0], zitem[1], zitem[2], server)
                zfs_output.append(ztemp)
    return zfs_output

def main():
    '''
    Main script, calls functions from bacula_functions
    Will check for Jobs for Datasets, email if there are sets with no job
    '''
    #Variables:
    bacula_info_list = [] #List for combined Bacula info
    bacula_path = "/opt/bacula/etc/conf.d/Director/" + platform.node() + "-dir/"
    server_list = [ '<SERVER 3>',
                   '<SERVER 2>', 
                   '<More Servers...>' 
                   ]
    email_address = "<NOTIFICATION EMAIL>"
    #Get File listings:
    job_file_list = glob.glob(f'{bacula_path}Job/*.cfg', recursive=False)
    fileset_file_list = glob.glob(f'{bacula_path}Fileset/*.cfg', recursive=False)
    client_file_list = glob.glob(f'{bacula_path}Client/*.cfg', recursive=False)
    bacula_info_list = bf.get_bacula_info(job_file_list, fileset_file_list, client_file_list)
    #Must be changed to use SSH key!
    username = input("Enter SSH username:")
    ssh_zfs_list = ssh_zfs(server_list, username)
    #Having now gotten the Bacula info and ZFS info, check if jobs exist for each dataset...
    for zfs in ssh_zfs_list.copy():
        for bacula_item in bacula_info_list:
            if zfs.server == bacula_item.bacula_client_server and zfs.mount == bacula_item.bacula_file_path:
                #We found a match of ZFS Server + MountPoint & Bacula Server + File Path
                ssh_zfs_list.remove(zfs)
    if len(ssh_zfs_list) > 0:
        #We still have items left, so we need to flag this!
        for zfs_left in ssh_zfs_list:
            body = f"No Bacula job found for filesystem {zfs_left.dataset} on {zfs_left.server}. Please check this is correct"
            subject = f"Check tape backups for {zfs_left.dataset} on {zfs_left.server}"
            cmd = (f'echo {body} | mailx -s {subject}') + email_address
            subprocess.run(cmd, shell=True, check=True)

if __name__ == '__main__':
    main()
