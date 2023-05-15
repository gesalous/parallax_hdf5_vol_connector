/* * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * *
 * Copyright by The HDF Group.                                               *
 * All rights reserved.                                                      *
 *                                                                           *
 * This file is part of HDF5.  The full HDF5 copyright notice, including     *
 * terms governing use, modification, and redistribution, is contained in    *
 * the COPYING file, which can be found at the root of the source code       *
 * distribution tree, or in https://support.hdfgroup.org/ftp/HDF5/releases.  *
 * If you do not have access to either file, you may request a copy from     *
 * help@hdfgroup.org.                                                        *
 * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * * */

/* Purpose:     A simple virtual object layer (VOL) connector with almost no
 *              functionality that can serve as a template for creating other
 *              connectors.
 */

#ifndef _parallax_vol_connector_H
#define _parallax_vol_connector_H
#include <hdf5.h>
#define PARALLAX_VOLUME "/home/gesalous/parallax-test/par.dat"
#define PARALLAX_L0_SIZE (8 * 1024 * 1024);
#define PARALLAX_GROWTH_FACTOR 8
/* The value must be between 256 and 65535 (inclusive) */
#define PARALLAX_VOL_CONNECTOR_VALUE ((H5VL_class_value_t)12202)
#define PARALLAX_VOL_CONNECTOR_NAME "parallax_vol_connector"

herr_t parh5_property_list_iterator(hid_t prop_id, const char *name, void *iter_data);
#endif /* _parallax_vol_connector_H */