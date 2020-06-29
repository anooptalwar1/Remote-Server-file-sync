import os
from os import listdir
from os.path import isfile, join
import pysftp
import stat
import time
import datetime
import constants
import subprocess

# importing variables from constants
hostname = constants.hostname
username = constants.username
password = constants.password
localpath = constants.localpath
remotepath = constants.remotepath


def syncopy():
    """
    Sync to all the contents of remote directory.
    """
    cnopts = pysftp.CnOpts()
    cnopts.hostkeys = None
    with pysftp.Connection(hostname, username=username, password=password, cnopts=cnopts) as sftp:
        sftp.cwd(remotepath)
        for f in sftp.listdir_attr():
            if not stat.S_ISDIR(f.st_mode):
                remote_dir_file = os.path.join(remotepath, f.filename)
                local_dir_file = os.path.join(localpath, f.filename)
                #check if the file already exists
                print("Checking %s..." % f.filename)
                if ((not os.path.isfile(local_dir_file)) or
                    (f.st_mtime > os.path.getmtime(local_dir_file))):
                    print("Downloading %s..." % f.filename)
                    sftp.get(f.filename, local_dir_file)


if __name__ == "__main__":
    syncopy()
    # execution of file conversion script
    process = subprocess.Popen(['/data/xpstats/run/case/scripts/CronJob', '-primaryOnly CRON_SG_CONVERT_FIX_RAW_MESSAGE 30', 'sg_covertFixMsgExtract'],
                           stdout=subprocess.PIPE,
                           universal_newlines=True)
    while True:
        output = process.stdout.readline()
        print(output.strip())
        # Polling of conversion logs
        return_code = process.poll()
        if return_code is not None:
            print('RETURN CODE', return_code)
            # Process has finished, read rest of the output
            for output in process.stdout.readlines():
                print(output.strip())
            break
