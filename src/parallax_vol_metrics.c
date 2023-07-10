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
	uint64_t dset_read_misalinged_accesses;
	uint64_t dset_write_misalinged_accesses;
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

void parh5M_inc_read_misaligned_access(parh5D_dataset_t dataset)
{
	(void)dataset;
	__sync_fetch_and_add(&parallax_metrics.dset_read_misalinged_accesses, 1);
}

void parh5M_inc_write_misaligned_access(parh5D_dataset_t dataset)
{
	(void)dataset;
	__sync_fetch_and_add(&parallax_metrics.dset_write_misalinged_accesses, 1);
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

const char *parh5M_dump_report(void)
{
	char *report = calloc(1UL, 8192);
	size_t idx = 0;
	size_t remaining = 8192;
	idx += snprintf(&report[idx], remaining, "Dataset bytes read: %lu\n", parallax_metrics.dset_bytes_read);
	idx += snprintf(&report[idx], remaining, "Dataset tiles read: %lu\n", parallax_metrics.dset_read_ntiles);
	idx += snprintf(&report[idx], remaining, "Dataset tiles read misaligned accesses: %lu\n",
			parallax_metrics.dset_read_misalinged_accesses);
	//
	idx += snprintf(&report[idx], remaining, "Dataset bytes written: %lu\n", parallax_metrics.dset_bytes_written);
	idx += snprintf(&report[idx], remaining, "Dataset tiles written: %lu\n", parallax_metrics.dset_write_ntiles);
	idx += snprintf(&report[idx], remaining, "Dataset tiles write misaligned accesses: %lu\n",
			parallax_metrics.dset_write_misalinged_accesses);
	//
	idx += snprintf(&report[idx], remaining, "Group bytes read: %lu\n", parallax_metrics.group_bytes_read);
	idx += snprintf(&report[idx], remaining, "Group read ops: %lu\n", parallax_metrics.group_read_ops);
	//
	idx += snprintf(&report[idx], remaining, "Group bytes written: %lu\n", parallax_metrics.group_bytes_written);
	idx += snprintf(&report[idx], remaining, "Group write ops: %lu\n", parallax_metrics.group_write_ops);
	return report;
}
