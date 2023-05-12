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

/* This connector's header */
#include "parallax_vol_connector.h"
#include "parallax_vol_dataset.h"
#include "parallax_vol_file.h"
#include "parallax_vol_group.h"
#include "parallax_vol_introspect.h"
#include "parallax_vol_object.h"
#include <H5PLextern.h>
#include <assert.h>
#include <bits/pthreadtypes.h>
#include <hdf5.h>
#include <log.h>
#include <pthread.h>
#include <stdlib.h>

struct parh5_file_map {

} struct parh5_connector_state {
  pthread_rwlock_t file_map_lock;
};

struct parh5_connector_state *connector;

herr_t parh5_property_list_iterator(hid_t prop_id, const char *name,
                                    void *iter_data) {
  (void)iter_data;

  size_t prop_size = 0;
  H5Pget_size(prop_id, name, &prop_size);

  log_debug("Property Name: %s value size %zu", name, prop_size);

  return 0; // Continue iterating
}

herr_t parh5_initialize(hid_t vipl_id) {
  connector = calloc(1, sizeof(*connector));
  pthread_rwlock_init(&connector->file_map_lock, NULL);
  log_debug("Initialized parallax plugin");
  return 1;
}

herr_t parh5_terminate(void) {
  free(connector);
  connector = NULL;
  log_debug("Closed parallax plugin");
  return 1;
}

/* The VOL class struct */
static const H5VL_class_t template_class_g = {
    3,                            /* VOL class struct version */
    PARALLAX_VOL_CONNECTOR_VALUE, /* value                    */
    PARALLAX_VOL_CONNECTOR_NAME,  /* name                     */
    0,                            /* version                  */
    0,                            /* capability flags         */
    parh5_initialize,             /* initialize               */
    parh5_terminate,              /* terminate                */
    {
        /* info_cls */
        (size_t)0, /* size    */
        NULL,      /* copy    */
        NULL,      /* compare */
        NULL,      /* free    */
        NULL,      /* to_str  */
        NULL,      /* from_str */
    },
    {
        /* wrap_cls */
        NULL, /* get_object   */
        NULL, /* get_wrap_ctx */
        NULL, /* wrap_object  */
        NULL, /* unwrap_object */
        NULL, /* free_wrap_ctx */
    },
    {
        /* attribute_cls */
        NULL, /* create       */
        NULL, /* open         */
        NULL, /* read         */
        NULL, /* write        */
        NULL, /* get          */
        NULL, /* specific     */
        NULL, /* optional     */
        NULL  /* close        */
    },
    {
        /* dataset_cls */
        parh5D_create,   /* create       */
        parh5D_open,     /* open         */
        parh5D_read,     /* read         */
        parh5D_write,    /* write        */
        parh5D_get,      /* get          */
        parh5D_specific, /* specific     */
        parh5D_optional, /* optional     */
        parh5D_close     /* close        */
    },
    {
        /* datatype_cls */
        NULL, /* commit       */
        NULL, /* open         */
        NULL, /* get_size     */
        NULL, /* specific     */
        NULL, /* optional     */
        NULL  /* close        */
    },
    {
        /* file_cls */
        parh5F_create,   /* create       */
        parh5F_open,     /* open         */
        parh5F_get,      /* get          */
        parh5F_specific, /* specific     */
        parh5F_optional, /* optional     */
        parh5F_close     /* close        */
    },
    {
        /* group_cls */
        parh5G_create,   /* create       */
        parh5G_open,     /* open         */
        parh5G_get,      /* get          */
        parh5G_specific, /* specific     */
        parh5G_optional, /* optional     */
        parh5G_close     /* close        */
    },
    {
        /* link_cls */
        NULL, /* create       */
        NULL, /* copy         */
        NULL, /* move         */
        NULL, /* get          */
        NULL, /* specific     */
        NULL  /* optional     */
    },
    {
        /* object_cls */
        parh5_open,     /* open         */
        parh5_copy,     /* copy         */
        parh5_get,      /* get          */
        parh5_specific, /* specific     */
        parh5_optional  /* optional     */
    },
    {
        /* introscpect_cls */
        parh5_get_conn_cls,  /* get_conn_cls  */
        parh5_get_cap_flags, /* get_cap_flags */
        parh5_opt_query      /* opt_query     */
    },
    {
        /* request_cls */
        NULL, /* wait         */
        NULL, /* notify       */
        NULL, /* cancel       */
        NULL, /* specific     */
        NULL, /* optional     */
        NULL  /* free         */
    },
    {
        /* blob_cls */
        NULL, /* put          */
        NULL, /* get          */
        NULL, /* specific     */
        NULL  /* optional     */
    },
    {
        /* token_cls */
        NULL, /* cmp          */
        NULL, /* to_str       */
        NULL  /* from_str     */
    },
    NULL /* optional     */
};

/* These two functions are necessary to load this plugin using
 * the HDF5 library.
 */

H5PL_type_t H5PLget_plugin_type(void) { return H5PL_TYPE_VOL; }
const void *H5PLget_plugin_info(void) { return &template_class_g; }
