#ifndef PARALLAX_VOL_METRICS_H
#define PARALLAX_VOL_METRICS_H
#include <stdint.h>
typedef struct parh5D_dataset *parh5D_dataset_t;
typedef struct parh5G_group *parh5G_group_t;

void parh5M_inc_dset_bytes_read(parh5D_dataset_t dataset, uint64_t num_bytes);

void parh5M_inc_dset_read_ntiles(parh5D_dataset_t dataset);

void parh5M_inc_dset_bytes_written(parh5D_dataset_t dataset, uint64_t num_bytes);

void parh5M_inc_dset_write_ntiles(parh5D_dataset_t dataset);

void parh5M_inc_group_bytes_read(parh5G_group_t group, uint64_t num_bytes);

void parh5M_inc_group_bytes_written(parh5G_group_t group, uint64_t num_bytes);

void parh5M_inc_dset_metadata_bytes_read(parh5D_dataset_t dataset, uint64_t num_bytes);

void parh5M_inc_cache_hits(parh5D_dataset_t dataset);

void parh5M_inc_cache_miss(parh5D_dataset_t dataset);

void parh5M_inc_dset_metadata_bytes_written(parh5D_dataset_t dataset, uint64_t num_bytes);

const char *parh5M_dump_report(void);

void parh5M_inc_dset_partially_written_tile(parh5D_dataset_t dataset);
#endif
