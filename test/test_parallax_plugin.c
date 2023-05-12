#include "../src/parallax_vol_connector.h"
#include <H5public.h>
#include <hdf5.h>
#include <stdlib.h>
#include <unistd.h>

int main(void) {

  /* Register the connector by name */
  hid_t vol_id = {0};
  if ((vol_id = H5VLregister_connector_by_name(PARALLAX_VOL_CONNECTOR_NAME,
                                               H5P_DEFAULT)) < 0) {
    fprintf(stderr, "Failed to register connector %s",
            PARALLAX_VOL_CONNECTOR_NAME);
    _exit(EXIT_FAILURE);
  }

  // Create file
  fprintf(stderr, "*****Creating file....*******\n");
  hid_t fapl_id = H5Pcreate(H5P_FILE_ACCESS);
  H5Pset_vol(fapl_id, vol_id, NULL);
  hid_t file_id =
      H5Fcreate("my_parallax_file.h5", H5F_ACC_TRUNC, H5P_DEFAULT, fapl_id);
  fprintf(stderr, "*****Creating file...SUCCESS.*******\n");

  // Create group
  fprintf(stderr, "*****Creating group...*******\n");
  hid_t lcpl_id = H5Pcreate(H5P_LINK_CREATE);
  hid_t group_id = H5Gcreate2(file_id, "/my_parallax_group", lcpl_id,
                              H5P_DEFAULT, H5P_DEFAULT);
  fprintf(stderr, "*****Creating group...SUCCESS*******\n");

  // Create dataset
  // fprintf(stderr, "*****Creating properties...*******\n");
  // fcpl_id = H5Pcreate(H5P_FILE_CREATE);
  // fprintf(stderr, "*****Creating properties...SUCCESS*******\n");

  // fprintf(stderr, "*****Creating link...*******\n");
  // H5Pset_link_creation_order(fcpl_id,
  //                            H5P_CRT_ORDER_TRACKED | H5P_CRT_ORDER_INDEXED);
  // fprintf(stderr, "*****Creating link...SUCCESS*******\n");

  fprintf(stderr, "*****Creating dataset...*******\n");
  hid_t dset_id = H5Dcreate2(file_id, "/my_group/my_dataset", H5T_NATIVE_INT,
                             group_id, H5P_DEFAULT, H5P_DEFAULT, H5P_DEFAULT);
  fprintf(stderr, "*****Creating dataset...SUCCESS*******\n");

  // Write data to dataset
  int data[10] = {1, 2, 3, 4, 5, 6, 7, 8, 9, 10};
  herr_t status =
      H5Dwrite(dset_id, H5T_NATIVE_INT, H5S_ALL, H5S_ALL, H5P_DEFAULT, data);
  if (-1 == status) {
    fprintf(stderr, "write in dataset failed");
    _exit(EXIT_FAILURE);
  }
  // Close resources
  status = H5Dclose(dset_id);
  status = H5Gclose(group_id);
  status = H5Fclose(file_id);
  // status = H5Pclose(fcpl_id);
  status = H5Pclose(fapl_id);
  status = H5Pclose(lcpl_id);
  status = H5VLunregister_connector(vol_id);

  return 0;
}
