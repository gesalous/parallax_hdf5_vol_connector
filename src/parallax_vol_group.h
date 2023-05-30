#ifndef PARALLAX_VOL_GROUP_H
#define PARALLAX_VOL_GROUP_H
#include <H5VLconnector.h>
#include <parallax/parallax.h>
/*VOL specific plugin staff*/
void *parh5G_create(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t lcpl_id, hid_t gcpl_id,
		    hid_t gapl_id, hid_t dxpl_id, void **req);
void *parh5G_open(void *obj, const H5VL_loc_params_t *loc_params, const char *name, hid_t gapl_id, hid_t dxpl_id,
		  void **req);
herr_t parh5G_get(void *obj, H5VL_group_get_args_t *args, hid_t dxpl_id, void **req);
herr_t parh5G_specific(void *obj, H5VL_group_specific_args_t *args, hid_t dxpl_id, void **req);
herr_t parh5G_optional(void *obj, H5VL_optional_args_t *args, hid_t dxpl_id, void **req);
herr_t parh5G_close(void *grp, hid_t dxpl_id, void **req);

/*NON VOL-plugin staff*/
typedef struct parh5G_group *parh5G_group_t;

/**
* @brief  Adds a dataset in a group if it is not already there
* @param group pointer
* @param dataset_name
* @param error_message
*/
bool parh5G_add_dataset(parh5G_group_t group, const char *dataset_name, const char **error_message);

/**
 * @brief Returns the name of the group
  * @param group reference to the group
  * @return on success a non NULL string with the group's name
*/
const char *parh5G_get_group_name(parh5G_group_t group);

uint64_t parh5G_get_group_uuid(parh5G_group_t group);
par_handle parh5G_get_parallax_db(parh5G_group_t group);
#endif
