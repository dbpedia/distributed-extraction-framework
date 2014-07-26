#!/usr/bin/env python

###
# This script sets up a Spark cluster on Google Compute Engine
# sigmoidanalytics.com
###

from __future__ import with_statement

import logging
import os
import pipes
import random
import shutil
import subprocess
import sys
import tempfile
import time
import commands
import urllib2
from optparse import OptionParser
from sys import stderr
import shlex
import getpass
import threading
import traceback

###
# Make sure gcutil is installed and authenticated 
# Usage: spark_gce.py <project> <no-slaves> <slave-type> <master-type> <identity-file> <zone> <cluster-name> <spark-mem> [<local-log-dir>]'
# Usage: spark_gce.py <project> <cluster-name> [<identity-file> <local-log-dir>] destroy'
###

identity_file = ""
slave_no = 0
slave_type = ""
master_type = ""
zone = ""
cluster_name = ""
username = ""
project = ""
spark_mem = ""
nmon_log_dir = ""
worker_cores = 0
worker_instances = 0
force_reformat = True


def read_args():

    global identity_file
    global slave_no
    global slave_type
    global master_type
    global zone
    global cluster_name
    global username
    global project
    global spark_mem
    global nmon_log_dir
    global worker_cores
    global worker_instances

    argc = len(sys.argv)
    if argc >= 10 and argc <= 12:
        project = sys.argv[1]
        slave_no = int(sys.argv[2])
        slave_type = sys.argv[3]
        master_type = sys.argv[4]
        identity_file = sys.argv[5]
        zone = sys.argv[6]
        cluster_name = sys.argv[7]
        spark_mem = sys.argv[8]
        worker_instances = int(sys.argv[9])
        worker_cores = int(sys.argv[10]) if argc >= 11 else 1 # default number of cores per worker = 0
        if argc == 12: nmon_log_dir = sys.argv[11]
        username = getpass.getuser()

    elif argc >= 4 and sys.argv[argc - 1].lower() == "destroy":

        print 'Destroying cluster ' + sys.argv[2]

        project = sys.argv[1]
        cluster_name = sys.argv[2]
        if argc == 6:
            identity_file = sys.argv[3]
            nmon_log_dir = sys.argv[4]
            username = getpass.getuser()
        
        if nmon_log_dir != "":
            print 'Downloading all nmon logs to ' + nmon_log_dir
            (master_nodes, slave_nodes) = get_cluster_ips()
            aggregate_nmon(master_nodes,slave_nodes)
        
        try:

            command = 'gcutil --project=' + project + ' listinstances --columns=name,external-ip --format=csv'
            output = subprocess.check_output(command, shell=True)
            output = output.split("\n")

            master_nodes=[]
            slave_nodes=[]

            for line in output:
                if len(line) >= 5:
                    host_name = line.split(",")[0]
                    host_ip = line.split(",")[1]
                    if host_name == cluster_name + '-master':
                        command = 'gcutil deleteinstance ' + host_name + ' --project=' + project
                        command = shlex.split(command)
                        subprocess.call(command)
                    elif cluster_name + '-slave' in host_name:
                        command = 'gcutil deleteinstance ' + host_name + ' --project=' + project
                        command = shlex.split(command)
                        subprocess.call(command)

        except:
            print "Failed to Delete instances"
            sys.exit(1)

        sys.exit(0)

    else:
        print 'Usage: spark_gce.py <project> <no-slaves> <slave-type> <master-type> <identity-file> <zone> <cluster-name> <spark-mem> <worker_instances> [<worker_cores>] [<local-log-dir>]'
        print 'Usage: spark_gce.py <project> <cluster-name> [<identity-file> <local-log-dir>] destroy'
        sys.exit(0)



def setup_network():

    print '[ Setting up Network & Firewall Entries ]'

    try:
        command = 'gcutil addnetwork ' + cluster_name + '-network --project=' + project
        command = shlex.split(command)
        subprocess.call(command)

        '''
        command = 'gcutil addfirewall --allowed :22 --project '+ project + ' ssh --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)

        command = 'gcutil addfirewall --allowed :8080 --project '+ project + ' spark-webui --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :8081 --project '+ project + ' spark-webui2 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :19999 --project '+ project + ' rpc1 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :50030 --project '+ project + ' rpc2 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :50070 --project '+ project + ' rpc3 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :60070 --project '+ project + ' rpc4 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :4040 --project '+ project + ' app-ui --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :4041 --project '+ project + ' app-ui2 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :4042 --project '+ project + ' app-ui3 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :4043 --project '+ project + ' app-ui4 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :4044 --project '+ project + ' app-ui5 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :4045 --project '+ project + ' app-ui6 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :10000 --project '+ project + ' shark-server --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :50060 --project '+ project + ' rpc5 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :50075 --project '+ project + ' rpc6 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :60060 --project '+ project + ' rpc7 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed :60075 --project '+ project + ' rpc8 --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)

        '''

        #Uncomment the above and comment the below section if you don't want to open all ports for public.
        command = 'gcutil addfirewall --allowed tcp:1-65535 --project '+ project + ' all-tcp-open --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)
        command = 'gcutil addfirewall --allowed udp:1-65535 --project '+ project + ' all-udp-open --network ' + cluster_name +'-network'
        command = shlex.split(command)
        subprocess.call(command)


    except OSError:
        print "Failed to setup Network & Firewall. Exiting.."
        sys.exit(1)


def launch_master():

    print '[ Launching Master ]'
    command = 'gcutil --service_version="v1" --project="' + project + '" addinstance "' + cluster_name + '-master" --zone="' + zone + '" --machine_type="' + master_type + '" --network="' + cluster_name + '-network" --external_ip_address="ephemeral" --service_account_scopes="https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.full_control" --image="https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20140619" --persistent_boot_disk="true" --auto_delete_boot_disk="true"'

    command = shlex.split(command)
    subprocess.call(command)


def launch_slaves():

    print '[ Launching Slaves ]'

    for s_id in range(1,slave_no+1):
        command = 'gcutil --service_version="v1" --project="' + project + '" addinstance "' + cluster_name + '-slave' + str(s_id) + '" --zone="' + zone + '" --machine_type="' + slave_type + '" --network="' + cluster_name + '-network" --external_ip_address="ephemeral" --service_account_scopes="https://www.googleapis.com/auth/userinfo.email,https://www.googleapis.com/auth/compute,https://www.googleapis.com/auth/devstorage.full_control" --image="https://www.googleapis.com/compute/v1/projects/centos-cloud/global/images/centos-6-v20140619" --persistent_boot_disk="true" --auto_delete_boot_disk="true"'
        command = shlex.split(command)
        subprocess.call(command)

def launch_cluster():

    print '[ Creating the Cluster ]'

    setup_network()

    launch_master()

    launch_slaves()


def check_gcutils():

    myexec = "gcutil"
    print '[ Verifying gcutil ]'
    try:
        subprocess.call([myexec, 'whoami'])

    except OSError:
        print "%s executable not found. \n# Make sure gcutil is installed and authenticated\nPlease follow https://developers.google.com/compute/docs/gcutil/" % myexec
        sys.exit(1)

def get_cluster_ips():

    command = 'gcutil --project=' + project + ' listinstances --columns=name,external-ip --format=csv'
    output = subprocess.check_output(command, shell=True)
    output = output.split("\n")

    master_nodes=[]
    slave_nodes=[]

    for line in output:
        if len(line) >= 5:
            host_name = line.split(",")[0]
            host_ip = line.split(",")[1]
            if host_name == cluster_name + '-master':
                master_nodes.append(host_ip)
            elif cluster_name + '-slave' in host_name:
                slave_nodes.append(host_ip)

    # Return all the instances
    return (master_nodes, slave_nodes)

def enable_sudo(master,command):
    '''
    ssh_command(master,"echo \"import os\" > setuid.py ")
    ssh_command(master,"echo \"import sys\" >> setuid.py")
    ssh_command(master,"echo \"import commands\" >> setuid.py")
    ssh_command(master,"echo \"command=sys.argv[1]\" >> setuid.py")
    ssh_command(master,"echo \"os.setuid(os.geteuid())\" >> setuid.py")
    ssh_command(master,"echo \"print commands.getstatusoutput(\"command\")\" >> setuid.py")
    '''
    os.system("ssh -i " + identity_file + " -t -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + master + " '" + command + "'")

def ssh_thread(host,command):

    enable_sudo(host,command)

def install_java(master_nodes,slave_nodes):

    print '[ Installing Java and Development Tools ]'
    master = master_nodes[0]

    master_thread = threading.Thread(target=ssh_thread, args=(master,"sudo yum install -y java-1.7.0-openjdk;sudo yum install -y java-1.7.0-openjdk-devel;sudo yum groupinstall \'Development Tools\' -y"))
    master_thread.start()

    #ssh_thread(master,"sudo yum install -y java-1.7.0-openjdk")
    for slave in slave_nodes:

        slave_thread = threading.Thread(target=ssh_thread, args=(slave,"sudo yum install -y java-1.7.0-openjdk;sudo yum install -y java-1.7.0-openjdk-devel;sudo yum groupinstall \'Development Tools\' -y"))
        slave_thread.start()

        #ssh_thread(slave,"sudo yum install -y java-1.7.0-openjdk")

    slave_thread.join()
    master_thread.join()


def ssh_command(host,command):

    #print "ssh -i " + identity_file + " -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + host + " '" + command + "'"
    commands.getstatusoutput("ssh -i " + identity_file + " -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + host + " '" + command + "'" )
    
def scp_command(host,remote_path,local_path):
    #print "scp -i " + identity_file + " -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + host + ":" + remote_path + " " + local_path
    commands.getstatusoutput("scp -i " + identity_file + " -o 'UserKnownHostsFile=/dev/null' -o 'CheckHostIP=no' -o 'StrictHostKeyChecking no' "+ username + "@" + host + ":" + remote_path + " " + local_path)


def deploy_keys(master_nodes,slave_nodes):

    print '[ Generating SSH Keys on Master ]'
    key_file = os.path.basename(identity_file)
    master = master_nodes[0]
    ssh_command(master,"ssh-keygen -q -t rsa -N \"\" -f ~/.ssh/id_rsa")
    ssh_command(master,"cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys")
    os.system("scp -i " + identity_file + " -oUserKnownHostsFile=/dev/null -oCheckHostIP=no -oStrictHostKeyChecking=no -o 'StrictHostKeyChecking no' "+ identity_file + " " + username + "@" + master + ":")
    ssh_command(master,"chmod 600 " + key_file)
    ssh_command(master,"tar czf .ssh.tgz .ssh")

    ssh_command(master,"ssh-keyscan -H $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) >> ~/.ssh/known_hosts")
    ssh_command(master,"ssh-keyscan -H $(cat /etc/hosts | grep $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) | cut -d\" \" -f2) >> ~/.ssh/known_hosts")

    print '[ Transfering SSH keys to slaves ]'
    for slave in slave_nodes:
        print commands.getstatusoutput("ssh -i " + identity_file + " -oUserKnownHostsFile=/dev/null -oCheckHostIP=no -oStrictHostKeyChecking=no " + username + "@" + master + " 'scp -i " + key_file + " -oStrictHostKeyChecking=no .ssh.tgz " + username +"@" + slave  + ":'")
        ssh_command(slave,"tar xzf .ssh.tgz")
        ssh_command(master,"ssh-keyscan -H " + slave + " >> ~/.ssh/known_hosts")
        ssh_command(slave,"ssh-keyscan -H $(cat /etc/hosts | grep $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) | cut -d\" \" -f2) >> ~/.ssh/known_hosts")
        ssh_command(slave,"ssh-keyscan -H $(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1) >> ~/.ssh/known_hosts")

def attach_drive(master_nodes,slave_nodes):

    print '[ Adding new 100GB drive on Master ]'
    master = master_nodes[0]

    # get list of disks that already exist
    command = 'gcutil --project=' + project + ' listdisks --columns=name --format=csv'
    disks = subprocess.check_output(command, shell=True).split("\n")

    disk_name = cluster_name + '-m-disk'
    if disk_name not in disks: # add disk if it doesn't exist already
        command='gcutil --service_version="v1" --project="' + project + '" adddisk "' + disk_name + '" --size_gb="100" --zone="' + zone + '"'
        command = shlex.split(command)
        subprocess.call(command)

    command = 'gcutil --project='+ project +' attachdisk --zone=' + zone +' --disk=' + disk_name + ' ' + cluster_name + '-master'
    command = shlex.split(command)
    subprocess.call(command)
    master_thread = None
    if force_reformat is True or disk_name not in disks: # format disk if it is new
        master_thread = threading.Thread(target=ssh_thread, args=(master,"sudo mkfs.ext3 /dev/disk/by-id/google-"+ disk_name + " -F < /dev/null"))
        master_thread.start()

    print '[ Adding new 100GB drive on Slaves ]'

    i = 1
    slave_thread = None
    for slave in slave_nodes:

        master = slave
        disk_name = cluster_name + '-s' + str(i) + '-disk'
        if disk_name not in disks: # add disk if it doesn't exist already
            command='gcutil --service_version="v1" --project="' + project + '" adddisk "' + disk_name + '" --size_gb="100" --zone="' + zone + '"'
            command = shlex.split(command)
            subprocess.call(command)

        command = 'gcutil --project='+ project +' attachdisk --zone=' + zone +' --disk=' + disk_name + ' ' + cluster_name + '-slave' +  str(i)
        command = shlex.split(command)
        subprocess.call(command)

        if force_reformat is True or disk_name not in disks: # format disk if it is new
            slave_thread = threading.Thread(target=ssh_thread, args=(slave,"sudo mkfs.ext3 /dev/disk/by-id/google-" + disk_name + " -F < /dev/null"))
            slave_thread.start()
        i=i+1

    if slave_thread != None: slave_thread.join()
    if master_thread != None: master_thread.join()

    print '[ Mounting new Volume ]'
    enable_sudo(master_nodes[0],"sudo mount /dev/disk/by-id/google-"+ cluster_name + "-m-disk /mnt")
    enable_sudo(master_nodes[0],"sudo chown " + username + ":" + username + " /mnt")
    i=1
    for slave in slave_nodes:
        enable_sudo(slave,"sudo mount /dev/disk/by-id/google-"+ cluster_name + "-s" + str(i) +"-disk /mnt")
        enable_sudo(slave,"sudo chown " + username + ":" + username + " /mnt")
        i=i+1

    print '[ All volumns mounted, will be available at /mnt ]'

def setup_spark(master_nodes,slave_nodes):
    print '[ Downloading Binaries ]'

    master = master_nodes[0]

    ssh_command(master,"rm -fr engine")
    ssh_command(master,"mkdir engine")

    setup_maven(master_nodes)
    
    if nmon_log_dir != "": setup_nmon(master_nodes,slave_nodes)
    
    ssh_command(master,"cd engine;wget http://apache.mesi.com.ar/spark/spark-0.9.1/spark-0.9.1-bin-hadoop2.tgz")
    ssh_command(master,"cd engine;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/scala.tgz")
    ssh_command(master,"cd engine;tar zxf spark-0.9.1-bin-hadoop2.tgz;rm spark-0.9.1-bin-hadoop2.tgz")
    ssh_command(master,"cd engine;tar zxf scala.tgz;rm scala.tgz")

    print '[ Building Spark with Hadoop 2.2.0 ]'
    ssh_command(master,"cd engine/spark-0.9.1-bin-hadoop2;SCALA_HOME=\"/home/`whoami`/engine/scala\" MAVEN_OPTS=\"-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m\" mvn -Dhadoop.version=2.2.0 -Dprotobuf.version=2.5.0 -DskipTests clean package")

    print '[ Updating Spark Configurations ]'
    ssh_command(master,"cd engine;cd spark-0.9.1-bin-hadoop2/conf;cp spark-env.sh.template spark-env.sh")
    ssh_command(master,"cd engine;cd spark-0.9.1-bin-hadoop2/conf;echo 'export SCALA_HOME=\"/home/`whoami`/engine/scala\"' >> spark-env.sh")
    ssh_command(master,"cd engine;cd spark-0.9.1-bin-hadoop2/conf;echo 'export SCALA_HOME=\"/home/`whoami`/engine/scala\"' >> .bashrc")
    ssh_command(master,"cd engine;cd spark-0.9.1-bin-hadoop2/conf;echo 'export SPARK_MEM=%s' >> spark-env.sh" % spark_mem)
    ssh_command(master,"cd engine;cd spark-0.9.1-bin-hadoop2/conf;echo 'export SPARK_WORKER_CORES=%d' >> spark-env.sh" % worker_cores)
    ssh_command(master,"cd engine;cd spark-0.9.1-bin-hadoop2/conf;echo 'export SPARK_WORKER_INSTANCES=%d' >> spark-env.sh" % worker_instances)
    ssh_command(master,"cd engine;cd spark-0.9.1-bin-hadoop2/conf;echo \"SPARK_JAVA_OPTS+=\\\" -Dspark.local.dir=/mnt/spark \\\"\" >> spark-env.sh")
    ssh_command(master,"cd engine;cd spark-0.9.1-bin-hadoop2/conf;echo 'export SPARK_JAVA_OPTS' >> spark-env.sh")
    ssh_command(master,"cd engine;cd spark-0.9.1-bin-hadoop2/conf;echo 'export SPARK_MASTER_IP=PUT_MASTER_IP_HERE' >> spark-env.sh")
    ssh_command(master,"cd engine;cd spark-0.9.1-bin-hadoop2/conf;echo 'export MASTER=spark://PUT_MASTER_IP_HERE:7077' >> spark-env.sh")
    ssh_command(master,"cd engine;cd spark-0.9.1-bin-hadoop2/conf;echo 'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.65.x86_64' >> spark-env.sh")
    #ssh_command(master,"cd engine/spark-0.9.1-bin-hadoop2;MAVEN_OPTS=\"-Xmx2g -XX:MaxPermSize=512M -XX:ReservedCodeCacheSize=512m\" /home/`whoami`/engine/apache-maven-3.0.5/mvn -Dhadoop.version=2.2.0 -Dprotobuf.version=2.5.0 -DskipTests clean package")

    ssh_command(master,"cd engine/spark-0.9.1-bin-hadoop2/conf/;rm slaves")
    for slave in slave_nodes:
        ssh_command(master,"echo " + slave + " >> engine/spark-0.9.1-bin-hadoop2/conf/slaves")


    ssh_command(master,"sed -i \"s/PUT_MASTER_IP_HERE/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g\" engine/spark-0.9.1-bin-hadoop2/conf/spark-env.sh")

    ssh_command(master,"chmod +x engine/spark-0.9.1-bin-hadoop2/conf/spark-env.sh")

    print '[ Rsyncing Spark to all slaves ]'

    for slave in slave_nodes:
        ssh_command(master,"rsync -za /home/" + username + "/engine " + slave + ":")
        ssh_command(slave,"mkdir /mnt/spark")

    ssh_command(master,"mkdir /mnt/spark")
    print '[ Starting Spark Cluster ]'
    ssh_command(master,"engine/spark-0.9.1-bin-hadoop2/sbin/start-all.sh")

    setup_hadoop(master_nodes,slave_nodes)

    print "\n\nSpark Master Started, WebUI available at : http://" + master + ":8080"

def setup_nmon(master_nodes,slave_nodes):
    for host in slave_nodes + master_nodes:
        print "[ Setting up nmon on %s ]" % host
        ssh_command(host, "cd engine;mkdir nmon;cd nmon;wget -O nmon http://sourceforge.net/projects/nmon/files/nmon_x86_64_centos6;chmod a+x nmon;./nmon -s10 -c10000000 -f")

def aggregate_nmon(master_nodes,slave_nodes):
    for host in slave_nodes + master_nodes:
        log_file = os.path.join(nmon_log_dir, host + ".nmon")
        print "[ Saving nmon log from %s to %s ]" % (host, log_file)
        ssh_command(host, "killall nmon")
        scp_command(host, "/home/" + username + "/engine/nmon/*.nmon", log_file)

def set_limits(master_nodes,slave_nodes):
    for host in slave_nodes + master_nodes:
        ssh_command(host, "echo '*          soft    nofiles   60000' | sudo tee --append  /etc/security/limits.conf")
        ssh_command(host, "echo '*          hard    nofiles   60000' | sudo tee --append  /etc/security/limits.conf")

        ssh_command(host, "echo '*          soft    nproc     unlimited' | sudo tee --append  /etc/security/limits.conf")
        ssh_command(host, "echo '*          hard    nproc     unlimited' | sudo tee --append  /etc/security/limits.conf")

        ssh_command(host, "echo '*          soft    nproc     unlimited' | sudo tee /etc/security/limits.d/90-nproc.conf")
        ssh_command(host, "echo '*          hard    nproc     unlimited' | sudo tee --append  /etc/security/limits.d/90-nproc.conf")
        ssh_command(host, "echo 'root       soft    nproc     unlimited' | sudo tee --append  /etc/security/limits.d/90-nproc.conf")

def setup_maven(master_nodes):

    master = master_nodes[0]
    print '[ Downloading maven ]'

    ssh_command(master,"cd engine;wget http://mirror.cc.columbia.edu/pub/software/apache/maven/maven-3/3.0.5/binaries/apache-maven-3.0.5-bin.tar.gz")
    ssh_command(master,"cd engine;tar zxf apache-maven-3.0.5-bin.tar.gz")
    ssh_command(master,"cd engine;rm apache-maven-3.0.5-bin.tar.gz")
    ssh_command(master,"echo 'export MAVEN_HOME=/home/`whoami`/engine/apache-maven-3.0.5' >> .bashrc")
    ssh_command(master,"echo 'export PATH=\$PATH:\$MAVEN_HOME/bin' >> .bashrc")

def setup_hadoop(master_nodes,slave_nodes):

    master = master_nodes[0]
    print '[ Downloading hadoop ]'

    ssh_command(master,"cd engine;wget http://apache.mirrors.lucidnetworks.net/hadoop/common/hadoop-2.2.0/hadoop-2.2.0.tar.gz")
    ssh_command(master,"cd engine;tar zxf hadoop-2.2.0.tar.gz")
    ssh_command(master,"cd engine;rm hadoop-2.2.0.tar.gz")

    print '[ Configuring Hadoop ]'

    #Configure .bashrc
    ssh_command(master,"echo '#HADOOP_CONFS' >> .bashrc")
    ssh_command(master,"echo 'export JAVA_HOME=/usr/lib/jvm/java-1.7.0-openjdk-1.7.0.65.x86_64' >> .bashrc")
    ssh_command(master,"echo 'export HADOOP_INSTALL=/home/`whoami`/engine/hadoop-2.2.0' >> .bashrc")
    ssh_command(master,"echo 'export PATH=\$PATH:\$JAVA_HOME/bin:\$HADOOP_INSTALL/bin' >> .bashrc")#FIXME removed $PATH prefix because of it picking clients path
    ssh_command(master,"echo 'export PATH=\$PATH:\$HADOOP_INSTALL/sbin' >> .bashrc")
    ssh_command(master,"echo 'export HADOOP_COMMON_LIB_NATIVE_DIR=\$HADOOP_INSTALL/lib/native' >> .bashrc")
    ssh_command(master,"echo 'export HADOOP_OPTS=-Djava.library.path=\$HADOOP_INSTALL/lib' >> .bashrc")
    ssh_command(master,"echo 'export HADOOP_MAPRED_HOME=\$HADOOP_INSTALL' >> .bashrc")
    ssh_command(master,"echo 'export HADOOP_COMMON_HOME=\$HADOOP_INSTALL' >> .bashrc")
    ssh_command(master,"echo 'export HADOOP_HDFS_HOME=\$HADOOP_INSTALL' >> .bashrc")
    ssh_command(master,"echo 'export YARN_HOME=\$HADOOP_INSTALL' >> .bashrc")
    ssh_command(master,"echo 'export PATH=\$PATH:\$HADOOP_INSTALL/bin' >> .bashrc")

    #Remove *-site.xmls
    ssh_command(master,"cd engine/hadoop-2.2.0;rm etc/hadoop/core-site.xml")#TODO revisit
    ssh_command(master,"cd engine/hadoop-2.2.0;rm etc/hadoop/yarn-site.xml")
    ssh_command(master,"cd engine/hadoop-2.2.0;rm etc/hadoop/hdfs-site.xml")
    #Download Our Confs #TODO revisit
    ssh_command(master,"cd engine/hadoop-2.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/core-site.xml")
    ssh_command(master,"cd engine/hadoop-2.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/hdfs-site.xml")
    ssh_command(master,"cd engine/hadoop-2.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/mapred-site.xml")
    ssh_command(master,"cd engine/hadoop-2.2.0/etc/hadoop/;wget https://s3.amazonaws.com/sigmoidanalytics-builds/spark/0.9.1/gce/configs/yarn-site.xml")

    #Config Core-site
    ssh_command(master,"sed -i \"s/PUT-MASTER-IP/$(/sbin/ifconfig eth0 | grep \"inet addr:\" | cut -d: -f2 | cut -d\" \" -f1)/g\" engine/hadoop-2.2.0/etc/hadoop/core-site.xml")

    #Create data/node dirs
    ssh_command(master,"mkdir -p /mnt/hadoop/hdfs/namenode;mkdir -p /mnt/hadoop/hdfs/datanode")
    #Config slaves
    ssh_command(master,"cd engine/hadoop-2.2.0/etc/hadoop/;rm slaves")
    for slave in slave_nodes:
        ssh_command(master,"cd engine/hadoop-2.2.0/etc/hadoop/;echo " + slave + " >> slaves")
        #ssh_command(master,"cd engine/hadoop-2.2.0/etc/hadoop/;echo -e '\n'" + " >> slaves")

    print '[ Rsyncing with Slaves ]'
    #Rsync everything
    for slave in slave_nodes:
        print "Rsyncing to " + slave
        ssh_command(master,"rsync -za /home/" + username + "/engine " + slave + ":")
        ssh_command(slave,"mkdir -p /mnt/hadoop/hdfs/namenode;mkdir -p /mnt/hadoop/hdfs/datanode")
        ssh_command(master,"rsync -za /home/" + username + "/.bashrc " + slave + ":")

    print '[ Formating namenode ]'
    #Format namenode
    ssh_command(master,"engine/hadoop-2.2.0/bin/hdfs namenode -format")

    print '[ Starting DFS ]'
    #Start dfs
    ssh_command(master,"engine/hadoop-2.2.0/sbin/start-dfs.sh")

def real_main():


    print "[ Script Started ]"
    #Read the arguments
    read_args()
    #Make sure gcutil is accessible.
    check_gcutils()

    #Launch the cluster
    launch_cluster()

    #Wait some time for machines to bootup
    print '[ Waiting 120 Seconds for Machines to start up ]'
    time.sleep(120)

    #Get Master/Slave IP Addresses
    (master_nodes, slave_nodes) = get_cluster_ips()

    #Install Java and build-essential
    install_java(master_nodes,slave_nodes)

    #Generate SSH keys and deploy
    deploy_keys(master_nodes,slave_nodes)

    #Attach a new empty drive and format it
    attach_drive(master_nodes,slave_nodes)

    #Set up Spark/Shark/Hadoop
    setup_spark(master_nodes,slave_nodes)

    #Set ulimit values so that Spark jobs can work correctly
    set_limits(master_nodes,slave_nodes)



def main():
    try:
        real_main()
    except Exception as e:
        print >> stderr, "\nError:\n", e
        traceback.print_exc()

if __name__ == "__main__":

    main()
