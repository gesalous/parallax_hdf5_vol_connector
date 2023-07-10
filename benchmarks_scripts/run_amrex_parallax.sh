#!/bin/bash

device="nvme0n1"
stat_file="/sys/block/${device}/stat"
sector_size_file="/sys/block/${device}/queue/hw_sector_size"
perf_sampling_rate=99

# Read the initial number of sectors written and read from the device
initial_sectors_written=$(awk '{print $7}' "$stat_file")
initial_sectors_read=$(awk '{print $3}' "$stat_file")

# Read the sector size
sector_size=$(cat "$sector_size_file")

# Execute programA with time
echo "=== Starting programA ==="
../../parallax_hdf5_vol_connector/sith_build/_deps/parallax-build/lib/kv_format.parallax --device /tmp/nvme/par.dat --max_regions_num 128
if [[ $2 == "record" ]]; then
	HDF5_PLUGIN_PATH=/tmp/nvme/parallax_hdf5_vol_connector/sith_build/src/ HDF5_VOL_CONNECTOR=parallax_vol_connector perf record -F $perf_sampling_rate -g ./h5bench_amrex_sync "$1" &
	h5bench_amrex_sync_pid=$!
	wait $h5bench_amrex_sync_pid
else
	HDF5_PLUGIN_PATH=/tmp/nvme/parallax_hdf5_vol_connector/sith_build/src/ HDF5_VOL_CONNECTOR=parallax_vol_connector /usr/bin/time -f "Elapsed time: %E\nUser time: %U\nSystem time: %S\nCPU usage: %P\nMemory usage: %M" ./h5bench_amrex_sync "$1" | tee result_bench.txt
fi

# Read the final number of sectors written and read from the device
final_sectors_written=$(awk '{print $7}' "$stat_file")
final_sectors_read=$(awk '{print $3}' "$stat_file")

# Calculate the bytes written and read
bytes_written=$(((final_sectors_written - initial_sectors_written) * sector_size))
bytes_read=$(((final_sectors_read - initial_sectors_read) * sector_size))
echo "Bytes written to ${device}: ${bytes_written}"
bytes_written_mb=$(bc <<<"scale=2; $bytes_written / (1024 * 1024)")
echo "MB written to ${device}: ${bytes_written_mb}"
echo "Bytes read from ${device}: ${bytes_read}"
bytes_read_mb=$(bc <<<"scale=2; $bytes_read / (1024 * 1024)")
echo "MB read from ${device}: ${bytes_read_mb}"

output_file="result_bench.txt"
# Extracting values from the output file
dataset_bytes_written=$(grep "Dataset bytes written:" "$output_file" | awk '{print $4}')
dataset_bytes_read=$(grep "Dataset bytes read:" "$output_file" | awk '{print $4}')
group_bytes_read=$(grep "Group bytes read:" "$output_file" | awk '{print $4}')
group_bytes_written=$(grep "Group bytes written:" "$output_file" | awk '{print $4}')

# Calculating application bytes
application_bytes=$((dataset_bytes_written + dataset_bytes_read + group_bytes_read + group_bytes_written))
application_bytes_mb=$(bc <<<"scale=2; $application_bytes / (1024 * 1024)")

# Calculating the sum of bytes_written and bytes_read
sum_bytes=$((bytes_written + bytes_read))
sum_bytes_mb=$(bc <<<"scale=2; $sum_bytes / (1024 * 1024)")

# Calculating amplification
amplification=$(bc <<<"scale=2; $sum_bytes / $application_bytes")

echo "Application Bytes (B): $application_bytes"
echo "Application Bytes (MB): $application_bytes_mb"
echo "Total device traffic (B): $sum_bytes"
echo "Total device traffic (MB): $sum_bytes_mb"

echo "Amplification: $amplification"

if [[ $2 == "record" ]]; then
	echo "Producing Flamegraph"
	perf_data_file="perf.data"
	flamegraph_dir="/home1/private/gesalous/FlameGraph"
	# Generate the collapsed stack using perf script
	perf script -i $perf_data_file | $flamegraph_dir/stackcollapse-perf.pl >collapsed_stack.txt
	# Generate the flame graph from the collapsed stack
	$flamegraph_dir/flamegraph.pl collapsed_stack.txt >flamegraph.svg
fi
