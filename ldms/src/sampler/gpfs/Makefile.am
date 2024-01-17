pkglib_LTLIBRARIES =
lib_LTLIBRARIES =
dist_man7_MANS =
dist_man1_MANS =

AM_CPPFLAGS = @OVIS_INCLUDE_ABS@
AM_LDFLAGS = @OVIS_LIB_ABS@
COMMON_LIBADD = -lsampler_base -lldms -lovis_util -lcoll \
		@LDFLAGS_GETTIME@

if ENABLE_GPFS
dist_man7_MANS += Plugin_gpfs.man
libprocnet_la_SOURCES = gpfs.c
libprocnet_la_LIBADD = $(COMMON_LIBADD)
pkglib_LTLIBRARIES += libgpfs.la
endif
