#ifndef PARALLAX_VOL_GROUP_H
#define PARALLAX_VOL_GROUP_H
#include "parallax_vol_inode.h"
#include <H5VLconnector.h>
#include <parallax/parallax.h>

typedef struct parh5F_file *parh5F_file_t;
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
 * @brief Returns the parallax db handle that this group
 * is associated.
 * @param [in] group reference to the group object
 */
par_handle parh5G_get_parallax_db(parh5G_group_t group);

/**
 * @brief Creates a new group that belongs to the file. The group is unlinked and it is
 * not saved in Parallax.
 * @param [in] file reference to the file object
 * @param [in] name the name of the file
 * @param access_pl_id [in] reference to the access property list for this group/file
 * @param create_pl_id [in] reference to the create property list for this group/file
 */
parh5G_group_t parh5G_create_group(parh5F_file_t file, const char *name, hid_t access_pl_id, hid_t create_pl_id);

/**
  * @brief Returns the inode associated with this group object.
  *  @param group reference to the group object
  *  @return reference to inode object or NULL on failure
*/
parh5I_inode_t parh5G_get_inode(parh5G_group_t group);

/**
* @brief opens a group hosted in file with inode.
  * @param [in] file reference to the file object
  * @param [in] inode reference to the inode object
  * @return Allocate and initializes the corresponding group object
* or NULL on failure
*/
parh5G_group_t parh5G_open_group(parh5F_file_t file, parh5I_inode_t inode);

/**
* @brief Returns the root inode of the set that this group
  * belongs.
  */
parh5I_inode_t parh5G_get_root_inode(parh5G_group_t group);

parh5F_file_t parh5G_get_file(parh5G_group_t group);

hid_t parh5G_get_cpl(parh5G_group_t group);

hid_t parh5G_get_apl(parh5G_group_t group);

const char *parh5G_get_group_name(parh5G_group_t group);
#endif
