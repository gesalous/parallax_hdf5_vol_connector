#include "parallax_vol_metrics.h"
#include <stdio.h>
#include <stdlib.h>

struct parh5M_metrics {
	uint64_t dset_bytes_read;
	uint64_t dset_read_ntiles;
	uint64_t dset_bytes_written;
	uint64_t dset_write_ntiles;
	uint64_t dset_metadata_read_ops;
	uint64_t dset_metadata_bytes_read;
	uint64_t dset_metadata_write_ops;
	uint64_t dset_metadata_bytes_written;
	uint64_t dset_cache_misses;
	uint64_t dset_cache_hits;
	uint64_t dset_partially_written_tiles;
	uint64_t group_bytes_read;
	uint64_t group_read_ops;
	uint64_t group_bytes_written;
	uint64_t group_write_ops;
};

struct parh5M_metrics parallax_metrics;

void parh5M_inc_dset_bytes_read(parh5D_dataset_t dataset, uint64_t num_bytes)
{
	(void)dataset;
	__sync_fetch_and_add(&parallax_metrics.dset_bytes_read, num_bytes);
	__sync_fetch_and_add(&parallax_metrics.dset_read_ntiles, 1);
}

void parh5M_inc_dset_read_ntiles(parh5D_dataset_t dataset)
{
	(void)dataset;
	__sync_fetch_and_add(&parallax_metrics.dset_read_ntiles, 1);
}

void parh5M_inc_dset_bytes_written(parh5D_dataset_t dataset, uint64_t num_bytes)
{
	(void)dataset;
	__sync_fetch_and_add(&parallax_metrics.dset_bytes_written, num_bytes);
}

void parh5M_inc_dset_write_ntiles(parh5D_dataset_t dataset)
{
	(void)dataset;
	__sync_fetch_and_add(&parallax_metrics.dset_write_ntiles, 1);
}

void parh5M_inc_cache_hits(parh5D_dataset_t dataset)
{
	(void)dataset;
	__sync_fetch_and_add(&parallax_metrics.dset_cache_hits, 1);
}

void parh5M_inc_cache_miss(parh5D_dataset_t dataset)
{
	(void)dataset;
	__sync_fetch_and_add(&parallax_metrics.dset_cache_misses, 1);
}

void parh5M_inc_dset_metadata_bytes_read(parh5D_dataset_t dataset, uint64_t num_bytes)
{
	(void)dataset;
	__sync_fetch_and_add(&parallax_metrics.dset_metadata_bytes_read, num_bytes);
	__sync_fetch_and_add(&parallax_metrics.dset_metadata_read_ops, 1);
}

void parh5M_inc_dset_metadata_bytes_written(parh5D_dataset_t dataset, uint64_t num_bytes)
{
	(void)dataset;
	__sync_fetch_and_add(&parallax_metrics.dset_metadata_bytes_written, num_bytes);
	__sync_fetch_and_add(&parallax_metrics.dset_metadata_write_ops, 1);
}

void parh5M_inc_group_bytes_read(parh5G_group_t group, uint64_t num_bytes)
{
	(void)group;
	__sync_fetch_and_add(&parallax_metrics.group_bytes_read, num_bytes);
	__sync_fetch_and_add(&parallax_metrics.group_read_ops, 1);
}

void parh5M_inc_group_bytes_written(parh5G_group_t group, uint64_t num_bytes)
{
	(void)group;
	__sync_fetch_and_add(&parallax_metrics.group_bytes_written, num_bytes);
	__sync_fetch_and_add(&parallax_metrics.group_write_ops, 1);
}

void parh5M_inc_dset_partially_written_tile(parh5D_dataset_t dataset)
{
	(void)dataset;
	__sync_fetch_and_add(&parallax_metrics.dset_partially_written_tiles, 1);
}

const char *parh5M_dump_report(void)
{
	char *report = calloc(1UL, 8192);
	size_t idx = 0;
	size_t remaining = 8192;
	idx += snprintf(&report[idx], remaining, "Dataset bytes read: %lu\n", parallax_metrics.dset_bytes_read);
	idx += snprintf(&report[idx], remaining, "Dataset tiles read: %lu\n", parallax_metrics.dset_read_ntiles);
	idx += snprintf(&report[idx], remaining, "Dataset cache hits: %lu\n", parallax_metrics.dset_cache_hits);
	idx += snprintf(&report[idx], remaining, "Dataset cache misses: %lu\n", parallax_metrics.dset_cache_misses);
	idx += snprintf(&report[idx], remaining, "Dataset cache hit ratio: %lf\n",
			(double)parallax_metrics.dset_cache_hits / parallax_metrics.dset_cache_hits +
				parallax_metrics.dset_cache_misses);
	//
	idx += snprintf(&report[idx], remaining, "Dataset partially written tiles: %lu\n",
			parallax_metrics.dset_partially_written_tiles);
	idx += snprintf(&report[idx], remaining, "Dataset bytes written: %lu\n", parallax_metrics.dset_bytes_written);
	idx += snprintf(&report[idx], remaining, "Dataset tiles written: %lu\n", parallax_metrics.dset_write_ntiles);
	//
	idx += snprintf(&report[idx], remaining, "Group bytes read: %lu\n", parallax_metrics.group_bytes_read);
	idx += snprintf(&report[idx], remaining, "Group read ops: %lu\n", parallax_metrics.group_read_ops);
	//
	idx += snprintf(&report[idx], remaining, "Group bytes written: %lu\n", parallax_metrics.group_bytes_written);
	idx += snprintf(&report[idx], remaining, "Group write ops: %lu\n", parallax_metrics.group_write_ops);
	return report;
}
