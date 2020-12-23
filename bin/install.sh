#!/bin/bash

# Install MSSQL tools
curl https://packages.microsoft.com/config/rhel/8/prod.repo > /etc/yum.repos.d/msprod.repo
yum install -y mssql-tools unixODBC-devel