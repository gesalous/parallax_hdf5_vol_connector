#include <H5Fpublic.h>
#include <H5Tpublic.h>
#include <log.h>
#include <stdlib.h>
#include <unistd.h>
hid_t ctp_class_to_memtype(H5T_class_t class)
{
	switch (class) {
	case H5T_INTEGER:
		return H5T_NATIVE_INT;
	case H5T_FLOAT:
		return H5T_NATIVE_FLOAT;
	case H5T_TIME:
	case H5T_STRING:
	case H5T_BITFIELD:
	case H5T_OPAQUE:
	case H5T_COMPOUND:
	case H5T_REFERENCE:
	case H5T_ENUM:
	case H5T_VLEN:
	case H5T_ARRAY:
		log_fatal("Sorry currently unsupported");
		_exit(EXIT_FAILURE);
	default:
		log_fatal("Unknown class");
		_exit(EXIT_FAILURE);
	}
}
