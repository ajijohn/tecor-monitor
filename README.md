tecor-monitor
=============

Microclim - Backend Processor.

[![Build Status](https://travis-ci.org/trenchproject/tecor-monitor.svg)](https://travis-ci.org/trenchproject/tecor-monitor)

## Purpose

This module is the heart of the http://Microclim.org project, it monitors the requests, and processes
them. The module invokes the NCL script, and then uploads the generated files to S3, on completion, it 
sends an email to the requester with corresponding S3 links.
 
# Installation
```
git clone https://github.com/trenchproject/tecor-monitor.git tecor-monitor
```

# Install packages
```
pip3  install -r requirements.txt 
```
 
# Setup

Create a .env file with below

BUCKET={your bucketname}
AWSREGION=us-west-2
INPUTDIR=/ebminput
OUTPUTDIR=/ebmoutput

Setup credentials in 

~/.aws/credentials

Needs to have 

[default]
aws_access_key_id={your id}
aws_secret_access_key={access key}

# Usage
```
python3 -u monitor.py

nohup python3 monitor.py >logfile.txt 2>&1 </dev/null &
```

# Mounting filesystems

If you are mounting cold or SSD drives to your instance. Once you have mounted the drives

ls -al /dev/disk/by-uuid/
```
total 0
drwxr-xr-x 2 root root 100 Dec  1 17:19 .
drwxr-xr-x 4 root root  80 Dec  1 17:19 ..
lrwxrwxrwx 1 root root  11 Dec  1 17:19 87d0529b-216b-4930-9b54-45b0cdca9c06 -> ../../xvda1
lrwxrwxrwx 1 root root  10 Dec  1 17:19 96b6be69-eca2-44b1-9f15-d5e452ab3843 -> ../../xvdg
lrwxrwxrwx 1 root root  10 Dec  1 17:19 e2e676ef-d656-42d5-96f5-02b74f4f68b7 -> ../../xvdh
```

Add below lines to fstab


nano /etc/fstab
LABEL=cloudimg-rootfs   /        ext4   defaults,discard        0 0


--Add below lines

96b6be69-eca2-44b1-9f15-d5e452ab3843 /ebmoutput ext4    defaults,nofail        0       2
e2e676ef-d656-42d5-96f5-02b74f4f68b7 /ebminput  ext4    defaults,nofail        0       2

sudo file -s /dev/xvdf
sudo file -s /dev/xvdh
   


sudo mount /dev/xvdh /ebminput
sudo mount /dev/xvdg /ebmoutput
   
   
## DOI
[![DOI](https://zenodo.org/badge/68495458.svg)](https://zenodo.org/badge/latestdoi/68495458)

## License

Apache