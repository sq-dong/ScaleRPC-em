#!/bin/bash
#
# SwitchML Project
# @file disable-icrc.sh
# @brief A small script to reenable ICRC checking when using RDMA. 
#
# Make sure to change the device name variable 'DEV' and 'REGS' 
# to match your setup.
#

DEV=$1
if [ -z "$DEV" ]; then
    echo "Usage: $0 <device_name>"
    exit 1
fi
REGS="0x5361c.12:1=1 0x5363c.12:1=1 0x53614.29:1=0 0x53634.29:1=1"
echo "WARNING: this script assumes you're using a ConnectX-5 NIC. If you're using something different, restore the register values and modify the script."

echo ibv_devinfo -d $DEV
ibv_devinfo -d $DEV

echo Status:
for i in $REGS
do
    CMD="sudo mstmcra $DEV ${i%=*}"
    printf "$CMD => "
    $CMD
done    

