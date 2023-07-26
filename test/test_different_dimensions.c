#include "hdf5.h"
#include <bits/getopt_core.h>
#include <getopt.h>
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#define N_ITERATIONS FILE_DIM1
#define MEM_ROWS_NUM 10
#define MEM_COLS_NUM 10
#define FILE_DIM0 200
#define FILE_DIM1 200
#define FILE_DIM2 200
#define FILE_DIM3 200

int main(int argc, char *argv[])
{
	int option = -1; // Default value for the number

	// Define the short and long options
	static struct option long_options[] = { { "number", required_argument, 0, 'n' }, { 0, 0, 0, 0 } };

	// Parse the command-line options
	int option_index = 0;
	while ((option = getopt_long(argc, argv, "n:", long_options, &option_index)) != -1) {
		switch (option) {
		case 'n':
			log_debug("Boom: %s", optarg);
			option = optarg ? atoi(optarg) : option; // Convert the option argument to an integer
			goto exit;
		case '?':
			// Invalid option or missing argument
			fprintf(stderr, "Usage: %s -n|--number <number>\n", argv[0]);
			return 1;

		default:
			break;
		}
	}
exit:
	// Check if the number is valid
	if (option == -1) {
		log_fatal("Error: Please provide a valid integer value using -n or --number option.");
		_exit(EXIT_FAILURE);
	}

	// Create a new HDF5 file
	hid_t file_id = H5Fcreate("example.h5", H5F_ACC_TRUNC, H5P_DEFAULT, H5P_DEFAULT);

	// Create a dataspace for the dataset
	hsize_t file_dims[4] = { FILE_DIM3, FILE_DIM2, FILE_DIM1, FILE_DIM0 };
	hid_t dataspace_id = H5Screate_simple(4, file_dims, NULL);

	// Create a 2D memory dataspace
	hsize_t mem_dims[2] = { MEM_ROWS_NUM, MEM_COLS_NUM };
	hid_t memspace_id = H5Screate_simple(2, mem_dims, NULL);
	int data[MEM_ROWS_NUM][MEM_COLS_NUM];

	// Fill the 2D memory space with the value 5
	for (int i = 0; i < MEM_ROWS_NUM; i++) {
		for (int j = 0; j < MEM_COLS_NUM; j++) {
			data[i][j] = 5;
		}
	}

	// Create the dataset
	hid_t dataset_id = H5Dcreate(file_id, "my_dataset", H5T_NATIVE_DOUBLE, dataspace_id, H5P_DEFAULT, H5P_DEFAULT,
				     H5P_DEFAULT);

	// Create a dataspace for the subarray
	dataspace_id = H5Dget_space(dataset_id);
	int idx_row = 0;
	int idx_column = 0;
	int num_rows = MEM_ROWS_NUM;
	int num_cols = MEM_COLS_NUM;
	int dim_i;
	int dim_j;
	int rows_bound, cols_bound;
	if (0 == option) { //Good case
		dim_i = FILE_DIM3;
		dim_j = FILE_DIM2;
		rows_bound = FILE_DIM1;
		cols_bound = FILE_DIM0;

	} else if (1 == option) { //Bad case
		rows_bound = FILE_DIM3;
		cols_bound = FILE_DIM2;
		dim_i = FILE_DIM0;
		dim_j = FILE_DIM1;
	} else if (2 == option) {
		dim_i = FILE_DIM3;
		rows_bound = FILE_DIM2;
		dim_j = FILE_DIM1;
		cols_bound = FILE_DIM0;
	} else if (3 == option) {
		rows_bound = FILE_DIM3;
		dim_i = FILE_DIM2;
		cols_bound = FILE_DIM1;
		dim_j = FILE_DIM0;
	} else {
		log_fatal("Wrong option");
		_exit(EXIT_FAILURE);
	}

	for (int i = 0; i < dim_i; i++) {
		for (int j = 0; j < dim_j; j++) {
			if (0 == option) { //Good case
				hsize_t start_op0[4] = { i, j, idx_row, idx_column }; // Starting point of the subarray
				// log_debug("start is at: [%d][%d][%d][%d]", i, j, idx_row, idx_column);
				hsize_t count_op0[4] = { 1, 1, MEM_ROWS_NUM, MEM_COLS_NUM }; // Size of the subarray
				H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, start_op0, NULL, count_op0, NULL);
			} else if (1 == option) {
				// log_debug("start is at: [%d][%d][%d][%d]", idx_row, idx_column,i,j);
				hsize_t start_op1[4] = { idx_row, idx_column, i, j }; // Starting point of the subarray
				hsize_t count_op1[4] = { MEM_ROWS_NUM, MEM_COLS_NUM, 1, 1 }; // Size of the subarray
				H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, start_op1, NULL, count_op1, NULL);
			} else if (2 == option) {
				// log_debug("start is at: [%d][%d][%d][%d]", i,idx_row, j, idx_column);
				hsize_t start_op2[4] = { i, idx_row, j, idx_column }; // Starting point of the subarray
				hsize_t count_op2[4] = { 1, MEM_ROWS_NUM, 1, MEM_COLS_NUM }; // Size of the subarray
				H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, start_op2, NULL, count_op2, NULL);
			} else if (3 == option) {
				// log_debug("start is at: [%d][%d][%d][%d]", idx_row, i, idx_column,j);
				hsize_t start_op3[4] = { idx_row, i, idx_column, j }; // Starting point of the subarray
				hsize_t count_op3[4] = { MEM_ROWS_NUM, 1, MEM_COLS_NUM, 1 }; // Size of the subarray
				H5Sselect_hyperslab(dataspace_id, H5S_SELECT_SET, start_op3, NULL, count_op3, NULL);
			}

			// Write the data to the dataset
			if (H5Dwrite(dataset_id, H5T_NATIVE_DOUBLE, memspace_id, dataspace_id, H5P_DEFAULT, data) < 0) {
				log_fatal("Write failed");
				_exit(EXIT_FAILURE);
			}
			// sleep(1);
			// idx_row += MEM_ROWS_NUM;
			// idx_column += MEM_COLS_NUM;
			// if (idx_row > rows_bound - MEM_ROWS_NUM) {
			// 	fprintf(stderr, "Resetting rows...\n");
			// 	idx_row = 0;
			// }
			// if (idx_column > cols_bound - MEM_COLS_NUM) {
			// 	fprintf(stderr, "Resetting columns...\n");
			// 	idx_column = 0;
			// }
		}
	}

	// Close the HDF5 objects
	H5Sclose(memspace_id);
	H5Dclose(dataset_id);
	H5Sclose(dataspace_id);
	H5Fclose(file_id);

	return 0;
}
