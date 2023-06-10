#ifndef PARALLAX_VOL_INODE_H
#define PARALLAX_VOL_INODE_H
#include "parallax_vol_connector.h"
#include <parallax/parallax.h>
#include <stdbool.h>
#include <stdint.h>
#define PARH5I_INODE_SIZE 512
#define PARH5I_NAME_SIZE 128
typedef struct parh5I_inode *parh5I_inode_t;
/**
 * @brief Stores the inode of the group in Parallax.
 * @param [in] inode reference to the inode to store
 * @param [in] par_db handle to the Parallax DB to store the inode
 * @return true on success false on failure
 */
bool parh5I_store_inode(parh5I_inode_t inode, par_handle par_db);

/**
  * @brief returns the name of the inode
  * @param [in] inode reference to the inode object
  * @return the group name
  */
const char *parh5I_get_inode_name(parh5I_inode_t inode);

/**
 * @brief increases the inode_num, writes it to Parallax and returns the new inode num
 * @param [in] reference to the root inode
 * @param [in] handle to the Parallax db to write the new version of the root inode
 */
uint64_t parh5I_generate_inode_num(parh5I_inode_t root_inode, par_handle par_db);

/**
 * @brief Fetches the inode
 * @param [in] par_db Parallax db where the inode is stored
 * @param [in] inode_num inode number to read
 * @return pointer to the inode fetcher or NULL if it is not found
 */
parh5I_inode_t parh5I_get_inode(par_handle par_db, uint64_t inode_num);

/**
 * @brief Performs a linear search in the inode to find the next entry
 * to visit.
 * @param [in] inode pointer to the inode object
 * @param [in] pivot the name of the entry to search for
 * @return the inode number of the entry or 0 if not found
 */
uint64_t parh5I_lsearch_inode(parh5I_inode_t, const char *pivot);

/**
  * @brief Creates an inode. Caution it does not save it to Parallax
  * @param [in] name inode name
  * @param [in] type of the inode
  * @param [in] root_inode based on which it generates the next inode num
  * @param [in] par_db Parallax db where to generate the inode
  * @return pointer to the new inode or NULL on failure
  */
parh5I_inode_t parh5I_create_inode(const char *name, parh5_object_e type, parh5I_inode_t root_inode, par_handle par_db);

/**
 * @brief Adds an entry to the inode
 * @param [in] inode pointer to the inode object
 * @param [in] inode_num the inode number of the entry
 * @param [in] name the name of the entry
 * @return true on success false on failure (if node is full)
*/
bool parh5I_add_inode(parh5I_inode_t inode, uint64_t inode_num, const char *name);

/**
  * @brief returns the inode number of the inode
  * @param [in] inode reference to inode object
  * @return the inode number on success or 0 on failure
*/
uint64_t parh5I_get_inode_num(parh5I_inode_t inode);

/**
  *@brief Returns the buffer and the size inside the inode of the
  * dataset that is available for writing.
  * @param [in] inode reference to the inode object
  * @param [out] size contains the available space
  * @return the pointer to the buffer on SUCCESS. If the inode is not a
  * dataset method fails and returns NULL.
*/
char *parh5I_get_inode_buf(parh5I_inode_t inode, uint32_t *size);

/**
  * @brief Searches a path in the form of group1/.../groupN and returns the inode num
  * of groupN.
  * @param inode reference to the inode object
  * @param path_search the path to search
  * @param par_db reference to the database of parallax.
  */
uint64_t parh5I_path_search(parh5I_inode_t inode, const char *path_search, par_handle par_db);

#endif
