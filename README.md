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

# Usage
```
python3 -u monitor.py

```

## License

Apache