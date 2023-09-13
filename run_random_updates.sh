#!/bin/bash

# Function to read I/O counter for a given device from /sys
function read_io_counter() {
	local device="$1"
	local io_counter="/sys/block/${device}/stat"
	local num_read_IOs=0
	num_read_IOs=$(awk '{print $1}' "$io_counter")
	local num_sectors_read=0
	num_sectors_read=$(awk '{print $3}' "$io_counter")

	local num_write_IOs=0
	num_write_IOs=$(awk '{print $5}' "$io_counter")
	local num_sectors_written=0
	num_sectors_written=$(awk '{print $7}' "$io_counter")

	echo "$num_sectors_read $num_sectors_written $num_read_IOs $num_write_IOs"
}

# Get the block size for the specified device in bytes
function get_block_size() {
	local device="$1"
	local block_size="/sys/block/${device}/queue/hw_sector_size"
	cat "$block_size"
}

# Function for Command A (replace with your actual command)
function command_a() {
	echo "Running Command A..."
	# ... (command A logic)
}

# Function for Command B (replace with your actual command)
function command_b() {
	echo "Running Command B..."
	# ... (command B logic)
}

# Default values
device=""

print_usage() {
	echo "Usage: script_name -d <device_name> -w <write|update> -c <hdf5|parallax|tiledb>"
}

# Parse command-line arguments
while getopts ":d:w:c:" opt; do
	case "$opt" in
	d)
		device=$OPTARG
		;;
	w)
		if [[ "$OPTARG" != "write" && "$OPTARG" != "update" ]]; then
			echo "Invalid argument for -w. Allowed: write, update" >&2
			print_usage
			exit 1
		fi
		write_option=$OPTARG
		;;
	c)
		if [[ "$OPTARG" != "hdf5" && "$OPTARG" != "parallax" && "$OPTARG" != "tiledb" ]]; then
			echo "Invalid argument for -c. Allowed: hdf5, parallax, tiledb" >&2
			print_usage
			exit 1
		fi
		config_option=$OPTARG
		;;
	\?)
		echo "Invalid option: -$OPTARG" >&2
		print_usage
		exit 1
		;;
	:)
		echo "Option -$OPTARG requires an argument." >&2
		print_usage
		exit 1
		;;
	esac
done

# Check if both -w and -c options are provided
if [[ -z "$write_option" || -z "$config_option" ]]; then
	echo "Both -w and -c options are mandatory." >&2
	print_usage
	exit 1
fi

# Read initial I/O counter for the specified device
mapfile -t initial_io_counter < <(read_io_counter "$device")

# Start the child script in the background
./calc_avg_queue_depth.sh "$device" >queue.log 2>&1 &
child_pid=$!

# Set environment variables if "parallax" flag is provided
echo "Config option = $config_option"
if [[ "$config_option" == "parallax" && "$write_option" == "write" ]]; then
	time env HDF5_PLUGIN_PATH=/home/gesalous/HDF5_staff/parallax_hdf5_vol_connector/build/src/ HDF5_VOL_CONNECTOR=parallax_vol_connector PARH5_VOLUME=/home/gesalous/parallax-test/par.dat PARH5_VOLUME_FORMAT=ON ./test/test_random_updates -w "$write_option"
elif [[ "$config_option" == "parallax" && "$write_option" == "update" ]]; then
	time env HDF5_PLUGIN_PATH=/home/gesalous/HDF5_staff/parallax_hdf5_vol_connector/build/src/ HDF5_VOL_CONNECTOR=parallax_vol_connector PARH5_VOLUME=/home/gesalous/parallax-test/par.dat PARH5_VOLUME_FORMAT=OFF ./test/test_random_updates -w "$write_option"
elif [[ "$config_option" == "tiledb" ]]; then
	time ./test/test_tiledb -w "$write_option"
else
	time ./test/test_random_updates -w "$write_option"
fi

kill "$child_pid"

# Read final I/O counter for the specified device
mapfile -t final_io_counter < <(read_io_counter "$device")

# Calculate total read and write I/O traffic in MB
block_size=$(get_block_size "$device")
total_sectors_read=$((final_io_counter[0] - initial_io_counter[0]))
total_sectors_written=$((final_io_counter[1] - initial_io_counter[1]))

total_read_IOs=$((final_io_counter[2] - initial_io_counter[2]))
total_write_IOs=$((final_io_counter[3] - initial_io_counter[3]))

total_read_KB=$((total_sectors_read * block_size / 1024))
total_write_KB=$((total_sectors_written * block_size / 1024))

total_read_mb=$((total_sectors_read * block_size / 1024 / 1024))
total_write_mb=$((total_sectors_written * block_size / 1024 / 1024))

echo "Total Read I/O Traffic: ${total_read_mb} MB"
echo "Total Write I/O Traffic: ${total_write_mb} MB"
echo "Total Read I/Os: ${total_read_IOs}"
echo "Total Write I/Os: ${total_write_IOs}"
echo "Average READ request size in KB: $((total_read_KB / total_read_IOs))"
echo "Average WRITE request size in KB: $((total_write_KB / total_write_IOs))"
awk '{ sum += $7 } END { if (NR > 0) print "Average Queue Size:", sum/NR }' queue.log
