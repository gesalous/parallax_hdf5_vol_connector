#include "tiledb/api/c_api/api_external_common.h"
#include "tiledb/api/c_api/error/error_api_external.h"
#include <log.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <tiledb/tiledb.h>
#include <unistd.h>
#define TILEDB_SPARSE_ROWS 64
#define TILEDB_SPARSE_COLUMNS 64
#define TILEDB_UPDATE_PERCENTAGE 10
#define SUBCOLS 16
#define PRINT_ERROR(X)                                \
	do {                                          \
		tiledb_error_t *error;                \
		tiledb_ctx_get_last_error(X, &error); \
		const char *msg;                      \
		tiledb_error_message(error, &msg);    \
		log_fatal("%s", msg);                 \
		_exit(EXIT_FAILURE);                  \
	} while (0);

// Name of array.
const char *array_name = "tiledb_sparse_array";

static void create_array(void)
{
	// Create TileDB context
	tiledb_ctx_t *ctx;
	tiledb_ctx_alloc(NULL, &ctx);

	// The array will be 4x4 with dimensions "rows" and "cols", with domain [1,4].
	// int dim_domain[] = { 1, 4, 1, 4 };
	int dim_domain[] = { 0, TILEDB_SPARSE_ROWS - 1, 0, TILEDB_SPARSE_COLUMNS - 1 };
	int tile_extents[] = { 1, SUBCOLS };
	tiledb_dimension_t *d1;
	tiledb_dimension_alloc(ctx, "rows", TILEDB_INT32, &dim_domain[0], &tile_extents[0], &d1);
	tiledb_dimension_t *d2;
	tiledb_dimension_alloc(ctx, "cols", TILEDB_INT32, &dim_domain[2], &tile_extents[1], &d2);

	// Create domain
	tiledb_domain_t *domain;
	tiledb_domain_alloc(ctx, &domain);
	tiledb_domain_add_dimension(ctx, domain, d1);
	tiledb_domain_add_dimension(ctx, domain, d2);

	// Create a single attribute "a" so each (i,j) cell can store an integer
	tiledb_attribute_t *a;
	tiledb_attribute_alloc(ctx, "a", TILEDB_INT32, &a);

	// Create array schema
	tiledb_array_schema_t *array_schema;
	tiledb_array_schema_alloc(ctx, TILEDB_SPARSE, &array_schema);
	tiledb_array_schema_set_cell_order(ctx, array_schema, TILEDB_ROW_MAJOR);
	tiledb_array_schema_set_tile_order(ctx, array_schema, TILEDB_ROW_MAJOR);
	tiledb_array_schema_set_domain(ctx, array_schema, domain);
	tiledb_array_schema_add_attribute(ctx, array_schema, a);

	// Create array
	if (TILEDB_OK != tiledb_array_create(ctx, array_name, array_schema)) {
		PRINT_ERROR(ctx)
	}

	// Clean up
	tiledb_attribute_free(&a);
	tiledb_dimension_free(&d1);
	tiledb_dimension_free(&d2);
	tiledb_domain_free(&domain);
	tiledb_array_schema_free(&array_schema);
	tiledb_ctx_free(&ctx);
}

static void tiledb_write_array(void)
{
	// Create TileDB context
	tiledb_ctx_t *ctx;
	tiledb_ctx_alloc(NULL, &ctx);

	// Open array for writing
	tiledb_array_t *array;
	tiledb_array_alloc(ctx, array_name, &array);
	if (TILEDB_OK != tiledb_array_open(ctx, array, TILEDB_WRITE)) {
		log_fatal("Failed to open array");
		_exit(EXIT_FAILURE);
	}

	int subccols = (TILEDB_SPARSE_ROWS * TILEDB_SPARSE_COLUMNS) / SUBCOLS;

	int coords_rows[SUBCOLS] = { 0 };
	int coords_cols[SUBCOLS];
	for (int i = 0; i < SUBCOLS; i++)
		coords_cols[i] = i;
	int data[SUBCOLS] = { 0 };

	// Write some simple data to cells (1, 1), (2, 4) and (2, 3).
	// int coords_rows[] = { 1, 2, 2, 50 };
	// int coords_cols[] = { 1, 4, 3, 25 };
	uint64_t coords_size = sizeof(coords_rows);
	// int data[] = { 1, 2, 3, 5 };
	uint64_t data_size = sizeof(data);

	// for (int i = 0; i <= SUBCOLS; i++)
	// 	fprintf(stderr, "(R:%d, C:%d)", coords_rows[i], coords_cols[i]);
	// fprintf(stderr, "\n");

	for (int s_id = 0; s_id < subccols; s_id++) {
		// Create the query
		tiledb_query_t *query = NULL;

		tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
		tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);

		if (TILEDB_OK != tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size)) {
			PRINT_ERROR(ctx)
		}

		if (TILEDB_OK != tiledb_query_set_data_buffer(ctx, query, "rows", coords_rows, &coords_size)) {
			PRINT_ERROR(ctx)
		}

		if (TILEDB_OK != tiledb_query_set_data_buffer(ctx, query, "cols", coords_cols, &coords_size)) {
			PRINT_ERROR(ctx)
		}
		// Submit query
		if (TILEDB_OK != tiledb_query_submit(ctx, query)) {
			PRINT_ERROR(ctx)
		}
		// log_info("Success wrote to TileDB subcol_id: %d out of: %d ", s_id, subccols);
		tiledb_query_free(&query);
		for (int i = 0; i < SUBCOLS; i++)
			coords_cols[i] += SUBCOLS;

		if (coords_cols[SUBCOLS - 1] < TILEDB_SPARSE_COLUMNS) {
			// for (int i = 0; i < SUBCOLS; i++)
			// 	fprintf(stderr, "C:%d ", coords_cols[i]);
			// fprintf(stderr, "\n");
			continue;
		}

		for (int i = 0; i < SUBCOLS; i++) {
			coords_cols[i] = i;
			// fprintf(stderr, "R:%d\n", coords_rows[i]);
			coords_rows[i]++;
		}
	}

	// Close array
	tiledb_array_close(ctx, array);

	// Clean up
	tiledb_array_free(&array);

	tiledb_ctx_free(&ctx);
}

// static void tiledb_read_array(void)
// {
// 	// Create TileDB context
// 	tiledb_ctx_t *ctx;
// 	tiledb_ctx_alloc(NULL, &ctx);

// 	// Open array for reading
// 	tiledb_array_t *array;
// 	tiledb_array_alloc(ctx, array_name, &array);
// 	tiledb_array_open(ctx, array, TILEDB_READ);

// 	// Slice only rows 1, 2 and cols 2, 3, 4
// 	tiledb_subarray_t *subarray;
// 	tiledb_subarray_alloc(ctx, array, &subarray);
// 	// int subarray_v[] = { 1, 2, 2, 4 };
// 	int subarray_v[] = { 1, 50, 1, 25 };
// 	tiledb_subarray_set_subarray(ctx, subarray, subarray_v);

// 	// Set maximum buffer sizes
// 	uint64_t coords_size = 16;
// 	uint64_t data_size = 16;

// 	// Prepare the vector that will hold the result
// 	int *coords_rows = (int *)malloc(coords_size);
// 	int *coords_cols = (int *)malloc(coords_size);
// 	int *data = (int *)malloc(data_size);

// 	// Create query
// 	tiledb_query_t *query;
// 	tiledb_query_alloc(ctx, array, TILEDB_READ, &query);
// 	tiledb_query_set_subarray_t(ctx, query, subarray);
// 	tiledb_query_set_layout(ctx, query, TILEDB_ROW_MAJOR);
// 	tiledb_query_set_data_buffer(ctx, query, "a", data, &data_size);
// 	tiledb_query_set_data_buffer(ctx, query, "rows", coords_rows, &coords_size);
// 	tiledb_query_set_data_buffer(ctx, query, "cols", coords_cols, &coords_size);

// 	// Submit query
// 	tiledb_query_submit(ctx, query);

// 	// Close array
// 	tiledb_array_close(ctx, array);

// 	// Print out the results.
// 	int result_num = (int)(data_size / sizeof(int));
// 	for (int r = 0; r < result_num; r++) {
// 		int i = coords_rows[r];
// 		int j = coords_cols[r];
// 		int a = data[r];
// 		fprintf(stderr, "[%s:%s:%d] Cell (%d, %d) has data %d\n", __FILE__, __func__, __LINE__, i, j, a);
// 	}

// 	// Clean up
// 	free((void *)coords_rows);
// 	free((void *)coords_cols);
// 	free((void *)data);
// 	tiledb_subarray_free(&subarray);
// 	tiledb_array_free(&array);
// 	tiledb_query_free(&query);
// 	tiledb_ctx_free(&ctx);
// }

// Function to randomly update an element
static void tiledb_random_update()
{
	// Seed random number generator
	srand(-1);

	// Open the array for writing
	// Create TileDB context
	tiledb_ctx_t *ctx;
	tiledb_ctx_alloc(NULL, &ctx);
	tiledb_array_t *array;
	tiledb_array_alloc(ctx, array_name, &array);
	if (TILEDB_OK != tiledb_array_open(ctx, array, TILEDB_WRITE))
		PRINT_ERROR(ctx)

	uint64_t num_updates = (TILEDB_SPARSE_ROWS * TILEDB_SPARSE_COLUMNS * TILEDB_UPDATE_PERCENTAGE) / 100;
	// Set the coordinates

	log_info("Updating array total updates are: %lu", num_updates);
	for (uint64_t i = 0; i < num_updates; i++) {
		// Randomly choose a row and column
		int random_row = rand() % TILEDB_SPARSE_ROWS;
		int random_col = rand() % TILEDB_SPARSE_COLUMNS;
		int row_coords[] = { random_row };
		int cols_coords[] = { random_col };
		// Data to write (update)
		int data_to_write[1] = { 1983 };
		// Create the query
		tiledb_query_t *query = NULL;
		tiledb_query_alloc(ctx, array, TILEDB_WRITE, &query);
		tiledb_query_set_layout(ctx, query, TILEDB_UNORDERED);
		uint64_t data_size = sizeof(data_to_write);
		uint64_t coord_size = sizeof(row_coords);

		if (TILEDB_OK != tiledb_query_set_data_buffer(ctx, query, "a", data_to_write, &data_size))
			PRINT_ERROR(ctx)

		if (TILEDB_OK != tiledb_query_set_data_buffer(ctx, query, "rows", &row_coords[0], &coord_size))
			PRINT_ERROR(ctx)

		if (TILEDB_OK != tiledb_query_set_data_buffer(ctx, query, "cols", &cols_coords[0], &coord_size))
			PRINT_ERROR(ctx)

		// Submit the query
		if (TILEDB_OK != tiledb_query_submit(ctx, query))
			PRINT_ERROR(ctx)
		// Finalize and close
		tiledb_query_finalize(ctx, query);
		tiledb_query_free(&query);
	}
	tiledb_array_close(ctx, array);
}

int main(int argc, char **argv)
{
	char *operation = NULL;

	// Parse command-line arguments
	for (int i = 1; i < argc; i++) {
		if (strcmp(argv[i], "-w") == 0 && i + 1 < argc) {
			operation = argv[i + 1];
			i++; // skip the next argument since we've just processed it
		}
	}

	if (!operation) {
		log_fatal("Missing mandatory -w parameter");
		_exit(EXIT_FAILURE);
	}

	if (strcmp(operation, "write") != 0 && strcmp(operation, "update") != 0) {
		log_fatal("Invalid value for -w. Expected 'write' or 'update'.");
		_exit(EXIT_FAILURE);
	}

	// Get object type
	tiledb_ctx_t *ctx;
	tiledb_ctx_alloc(NULL, &ctx);
	tiledb_object_t type;
	tiledb_object_type(ctx, array_name, &type);
	tiledb_ctx_free(&ctx);

	// Now, based on the value of operation, perform your tasks
	if (strcmp(operation, "write") == 0) {
		log_info("Executing write workload");
		create_array();
		tiledb_write_array();
		return 1;
	}
	log_info("Executing update workload");
	tiledb_random_update();

	return 1;
}
