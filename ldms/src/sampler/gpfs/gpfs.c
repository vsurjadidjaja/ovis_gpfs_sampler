/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2021 National Technology & Engineering Solutions
 * of Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
 * NTESS, the U.S. Government retains certain rights in this software.
 * Copyright (c) 2821 Open Grid Computing, Inc. All rights reserved.
 *
 * This software is available to you under a choice of one of two
 * licenses.  You may choose to be licensed under the terms of the GNU
 * General Public License (GPL) Version 2, available from the file
 * COPYING in the main directory of this source tree, or the BSD-type
 * license below:
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *      Redistributions of source code must retain the above copyright
 *      notice, this list of conditions and the following disclaimer.
 *
 *      Redistributions in binary form must reproduce the above
 *      copyright notice, this list of conditions and the following
 *      disclaimer in the documentation and/or other materials provided
 *      with the distribution.
 *
 *      Neither the name of Sandia nor the names of any contributors may
 *      be used to endorse or promote products derived from this software
 *      without specific prior written permission.
 *
 *      Neither the name of Open Grid Computing nor the names of any
 *      contributors may be used to endorse or promote products derived
 *      from this software without specific prior written permission.
 *
 *      Modified source versions must be plainly marked as such, and
 *      must not be misrepresented as being the original software.
 *
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */
/**
 * \file gpfs.c
 * \brief GPFS Sampler
 */
#include <inttypes.h>
#include <unistd.h>
#include <sys/errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/types.h>
#include <time.h>
#include <sys/time.h>
#include "ldms.h"
#include "ldmsd.h"
#include "sampler_base.h"
#include <pthread.h>

#ifndef ARRAY_SIZE
#define ARRAY_SIZE(a) (sizeof(a) / sizeof(*a))
#endif

static pthread_mutex_t cfg_lock = PTHREAD_MUTEX_INITIALIZER;

#define GPFS_FILE "gpfs"
static char *gpfs_file = GPFS_FILE;
#define NVARS 15
static char *varname[] = {"name","ip","node_name","timestamp", "cluster", "filesystem", "disks", "bytes_read", 
							"bytes_written","opens", "closes", "reads", "writes", "read_dir", "inode_updates"};

struct gpfs {
	char name[20]; /* device name */
	ldms_set_t set;
	int meta_done; /* have we set the device metric */
	uint64_t update;
};

#define MAXIFACE 10
static struct gpfs gpfs[MAXIFACE];
int n_gpfs;
static int configured;
static int termed;

#define SAMP "gpfs"
static FILE *mf = NULL;
static int metric_offset;
static base_data_t base;

static ovis_log_t mylog;

struct kw {
	char *token;
	int (*action)(struct attr_value_list *kwl, struct attr_value_list *avl, void *arg);
};

static ldms_set_t get_set(struct ldmsd_sampler *self)
{
	return NULL;
}

static void gpfs_reset()
{
	if (mf)
		fclose(mf);
	mf = NULL;
	int j;
	for (j = 0; j < n_gpfs; j++) {
		ldms_set_t set = gpfs[j].set;
		if (set) {
                        const char *tmp = ldms_set_instance_name_get(set);
                        ldmsd_set_deregister(tmp, base->pi_name);
                        ldms_set_unpublish(set);
                        ldms_set_delete(set);
		}
		memset(&gpfs[j], 0, sizeof(gpfs[0]));
	}
	if (base) {
		free(base->instance_name);
		base->instance_name = NULL;
		base_del(base);
		base = NULL;
	}
	metric_offset = 0;
	n_gpfs = 0;
	configured = 0;
}

static int add_gpfs(const char *name)
{
	ovis_log(mylog, OVIS_LDEBUG, "adding gpfs %s.\n", name);
	/* temporarily override default instance name behavior */
	char *tmp = base->instance_name;
	size_t len = strlen(tmp);
	int rc;
	base->instance_name = malloc( len + 20);
	if (!base->instance_name) {
		base->instance_name = tmp;
		rc = ENOMEM;
		goto err;
	}
	/* override single set assumed in sampler_base api */
	snprintf(base->instance_name, len + 20, "%s/%s", tmp, name);
	ldms_set_t set = base_set_new(base);
	if (!set) {
		ovis_log(mylog, OVIS_LERROR, "failed to make %s set for %s\n",
			name, SAMP);
		rc = errno;
		base->instance_name = tmp;
		goto err;
	}
	base_auth_set(&base->auth, set);
	gpfs[n_gpfs].set = set;
	strncpy(gpfs[n_gpfs].name, name, sizeof(gpfs[n_gpfs].name));
	base->set = NULL;
	free(base->instance_name);
	base->instance_name = tmp;
	n_gpfs++;
	rc = 0;
err:
	return rc;
}

static int create_metric_schema(base_data_t base)
{
	int rc;
	ldms_schema_t schema;
	int  j;

	mf = fopen(gpfs_file, "r");
	if (!mf) {
		ovis_log(mylog, OVIS_LERROR, "Could not open " SAMP " file "
				"'%s'...exiting\n",
				gpfs_file);
		return ENOENT;
	}

	/* Create a metric set of the required size */
	schema = base_schema_new(base);
	if (!schema) {
		ovis_log(mylog, OVIS_LERROR,
		       "%s: The schema '%s' could not be created, errno=%d.\n",
		       __FILE__, base->schema_name, errno);
		rc = EINVAL;
		goto err;
	}

	/* Location of first metric from gpfs file */
	metric_offset = ldms_schema_metric_count_get(schema);

	rc = ldms_schema_metric_array_add(schema, "gpfs", LDMS_V_CHAR_ARRAY, sizeof(gpfs[0].name));
	if (rc < 0) {
		ovis_log(mylog, OVIS_LERROR, "out of memory: device\n");
		rc = ENOMEM;
		goto err;
	}
	rc = ldms_schema_metric_add(schema, "update", LDMS_V_U64);
	if (rc < 0) {
		ovis_log(mylog, OVIS_LERROR, "out of memory: update\n");
		rc = ENOMEM;
		goto err;
	}
	for (j = 0; j < NVARS; j++) {
		rc = ldms_schema_metric_add(schema, varname[j], LDMS_V_U64);
		if (rc < 0) {
			rc = ENOMEM;
			goto err;
		}
	}
	return 0;

err:

	if (mf)
		fclose(mf);
	mf = NULL;

	return rc;
}


/**
 * check for invalid flags, with particular emphasis on warning the user about
 */
static int config_check(struct attr_value_list *kwl, struct attr_value_list *avl, void *arg)
{
	char *value;
	int i;

	char* deprecated[]={"set"};

	for (i = 0; i < ARRAY_SIZE(deprecated); i++){
		value = av_value(avl, deprecated[i]);
		if (value){
			ovis_log(mylog, OVIS_LERROR, "config argument %s has been deprecated.\n",
			       deprecated[i]);
			return EINVAL;
		}
	}

	return 0;
}

static const char *usage(struct ldmsd_plugin *self)
{
	return "config name=" SAMP " exclude_ports=<devs>\n" \
		BASE_CONFIG_USAGE \
		"    <devs>          A comma-separated list of interfaces to be ignored.\n"
		"                    By default all active interfaces discovered will be reported.\n";
}

static int config(struct ldmsd_plugin *self, struct attr_value_list *kwl, struct attr_value_list *avl)
{
	char* ignorelist = NULL;
	char* pch = NULL;
	char *saveptr = NULL;
	char *ivalue = NULL;
	void *arg = NULL;
	int rc;

	rc = config_check(kwl, avl, arg);
	if (rc != 0){
		return rc;
	}
	pthread_mutex_lock(&cfg_lock);

	if (termed) {
		rc = 0;
		goto err1;
	}
	if (configured) {
		gpfs_reset();
		ovis_log(mylog, OVIS_LDEBUG, "reconfiguring.\n");
	}
	n_ignored = 0;
	ivalue = av_value(avl, "exclude_ports");
	if (ivalue == NULL)
		ivalue = "";
	ignorelist = strdup(ivalue);
	if (!ignorelist) {
		ovis_log(mylog, OVIS_LERROR, "out of memory\n");
		goto err;
	}
	pch = strtok_r(ignorelist, ",", &saveptr);
	while (pch != NULL){
		if (n_ignored >= (MAXIFACE-1)) {
			ovis_log(mylog, OVIS_LERROR, "too many devices being ignored: <%s>\n",
				pch);
			goto err;
		}
		snprintf(ignore_port[n_ignored], sizeof(ignore_port[0]), "%s", pch);
		ovis_log(mylog, OVIS_LDEBUG, "ignoring net device <%s>\n", pch);
		n_ignored++;
		pch = strtok_r(NULL, ",", &saveptr);
	}
	free(ignorelist);
	ignorelist = NULL;

	base = base_config(avl, SAMP, SAMP, mylog);
	if (!base) {
		rc = EINVAL;
		goto err;
	}

	rc = create_metric_schema(base);
	if (rc) {
		ovis_log(mylog, OVIS_LERROR, "failed to create a metric schema.\n");
		goto err;
	}
	pthread_mutex_unlock(&cfg_lock);
	configured = 1;
	return 0;

err:
	free(ignorelist);
	base_del(base);
err1:
	pthread_mutex_unlock(&cfg_lock);
	return rc;
}

static int update_gpfs(int j, uint64_t msum, union ldms_value *v)
{
	ldms_set_t set = gpfs[j].set;
	if (!set || j < 0 || j >= MAXIFACE)
		return EINVAL;
	base->set = set;
	int metric_no = metric_offset;
	base_sample_begin(base);
	if (!gpfs[j].meta_done) {
		ldms_metric_array_set_str( set, metric_no,
			gpfs[j].name);
		gpfs[j].meta_done = 1;
	}
	metric_no++;
	ldms_metric_set_u64(set, metric_no, gpfs[j].update);
	metric_no++;
	int i;
	for (i = 0; i < NVARS; i++) {
		ldms_metric_set(set, metric_no++, &v[i]);
	}
	gpfs[j].update++;
	base_sample_end(base);
	base->set = NULL;
	return 0;
}

static int sample(struct ldmsd_sampler *self)
{
	char *s;
	char lbuf[256];
	char curriface[20];
	union ldms_value v[NVARS];
	int j;
	int rc;

	pthread_mutex_lock(&cfg_lock);

	if (!configured) {
		ovis_log(mylog, OVIS_LDEBUG, "plugin not initialized\n");
		rc = 0;
		goto err;
	}

	if (!mf)
		mf = fopen(gpfs_file, "r");
	if (!mf) {
		ovis_log(mylog, OVIS_LERROR, "Could not open /gpfs file "
				"'%s'...exiting\n", gpfs_file);
		rc = ENOENT;
		goto err;
	}

	/* parse all data */
	do {
		s = fgets(lbuf, sizeof(lbuf), mf);
        char** args = (char**)malloc(NVARS*sizeof(char*));
        int i = 0;

        char* token = strtok(s, " \t");
        for (int k = 0; token != NULL; k++) {
                args[k] = strdup(token);
                token = strtok(NULL, " \t");
                if(k%2 == 0){
                    v[i] = args[k];
                }
        }

        /* add discovered interface if it's active. */
        rc = add_gpfs(curriface);
        if (rc)
            goto err;
        rc = update_gpfs(n_gpfs-1, 0, v);
        if (rc)
            goto err;
	
	skip:
		continue;
	} while (s);
	rc = 0;
err:
	pthread_mutex_unlock(&cfg_lock);
	return rc;
}


static void term(struct ldmsd_plugin *self)
{
	(void)self;
	pthread_mutex_lock(&cfg_lock);
	gpfs_reset();
	termed = 1;
	pthread_mutex_unlock(&cfg_lock);
	if (mylog)
		ovis_log_destroy(mylog);
}

static struct ldmsd_sampler gpfs_plugin = {
	.base = {
		.name = SAMP,
		.type = LDMSD_PLUGIN_SAMPLER,
		.term = term,
		.config = config,
		.usage = usage,
	},
	.get_set = get_set,
	.sample = sample,
};

struct ldmsd_plugin *get_plugin()
{
	int rc;
	mylog = ovis_log_register("sampler."SAMP, "The log subsystem of the " SAMP " plugin");
	if (!mylog) {
		rc = errno;
		ovis_log(NULL, OVIS_LWARN, "Failed to create the subsystem "
				"of '" SAMP "' plugin. Error %d\n", rc);
	}
	return &gpfs_plugin.base;
}

static void __attribute__ ((destructor)) gpfs_plugin_fini(void);
static void gpfs_plugin_fini()
{
	term(NULL);
}
