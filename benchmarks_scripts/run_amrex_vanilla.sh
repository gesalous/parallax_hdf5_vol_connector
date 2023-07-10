#!/bin/bash

device="nvme0n1"
stat_file="/sys/block/${device}/stat"
sector_size_file="/sys/block/${device}/queue/hw_sector_size"

# Read the initial number of sectors written and read from the device
initial_sectors_written=$(awk '{print $7}' "$stat_file")
initial_sectors_read=$(awk '{print $3}' "$stat_file")

# Read the sector size
sector_size=$(cat "$sector_size_file")

# Execute programA with time
echo "=== Starting programA ==="
/usr/bin/time -f "Elapsed time: %E\nUser time: %U\nSystem time: %S\nCPU usage: %P\nMemory usage: %M" ./h5bench_amrex_sync "$1"

# Read the final number of sectors written and read from the device
final_sectors_written=$(awk '{print $7}' "$stat_file")
final_sectors_read=$(awk '{print $3}' "$stat_file")

# Calculate the bytes written and read
bytes_written=$(((final_sectors_written - initial_sectors_written) * sector_size))
echo "Bytes written to ${device}: ${bytes_written}"
bytes_written_mb=$(bc <<<"scale=2; $bytes_written / (1024 * 1024)")
echo "MB written to ${device}: ${bytes_written_mb}"

bytes_read=$(((final_sectors_read - initial_sectors_read) * sector_size))
echo "Bytes read from ${device}: ${bytes_read}"
bytes_read_mb=$(bc <<<"scale=2; $bytes_read / (1024 * 1024)")
echo "MB read from ${device}: ${bytes_read_mb}"
