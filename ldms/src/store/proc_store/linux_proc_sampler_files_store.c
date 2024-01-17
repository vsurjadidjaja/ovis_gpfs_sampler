/* -*- c-basic-offset: 8 -*-
 * Copyright (c) 2022,2023 National Technology & Engineering Solutions
 * of Sandia, LLC (NTESS). Under the terms of Contract DE-NA0003525 with
 * NTESS, the U.S. Government retains certain rights in this software.
 * Copyright (c) 2021,2023 Open Grid Computing, Inc. All rights reserved.
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
 *	Redistributions of source code must retain the above copyright
 *	notice, this list of conditions and the following disclaimer.
 *
 *	Redistributions in binary form must reproduce the above
 *	copyright notice, this list of conditions and the following
 *	disclaimer in the documentation and/or other materials provided
 *	with the distribution.
 *
 *	Neither the name of Sandia nor the names of any contributors may
 *	be used to endorse or promote products derived from this software
 *	without specific prior written permission.
 *
 *	Neither the name of Open Grid Computing nor the names of any
 *	contributors may be used to endorse or promote products derived
 *	from this software without specific prior written permission.
 *
 *	Modified source versions must be plainly marked as such, and
 *	must not be misrepresented as being the original software.
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
#include <inttypes.h>
#include <unistd.h>
#include <sys/errno.h>
#include <sys/stat.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <time.h>
#include <pthread.h>
#include <assert.h>
#include <coll/htbl.h>
#include <sos/sos.h>
#include <openssl/sha.h>
#include <math.h>
#include <ovis_json/ovis_json.h>
#include <ovis_log/ovis_log.h>
#include "ldms.h"
#include "ldmsd.h"
#include "lpss_common.h"

#define UUID "31654df7-03f3-48a3-975c-f5c2ce708711"
#define STREAM "linux_proc_sampler_files"
#define LISTNAME "data"

static ovis_log_t mylog;
static sos_schema_t app_schema;
static char *stream;
static char *root_path;
static struct ldmsd_plugin stream_store;

static const char *time_job_component_attrs[] = { "timestamp", "job_id", "component_id", "file", "pid" };
static const char *job_component_timestamp_attrs[] = { "job_id", "component_id", "timestamp", "pid", "file" };
static const char *file_job_time_attrs[] = { "file", "job_id", "timestamp", "component_id", "pid" };


/*
 * Example JSON object:
 *
 * { \"job_id\" : 0,
 *   \"component_id\" : 0,
 *   \"producerName\" : \"nid00021\",
 *   \"pid\" : 241011,
 *   \"timestamp\" : \"163000348.3312\",
 *   \"task_rank\" : -1,
 *   \"parent\" : 31585,
 *   \"is_thread\" : 1,
 *   \"exe\" : \"/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.222.b10-0.el7_6.aarch64/jre/bin/java\",
 *   \"data\" :
 *	[
 *	  { \"file\" : \"/lustre/sierra/ascdojenkins/stria-login2/remoting.jar\",
 *	    \"st_dev\" : \"[a1028fc8]\",
 *	    \"st_ino\" : 144116379124052328,
 *	    \"st_mode\" : 100640,
 *	    \"st_size\" : 1524239,
 *	    \"mtime\" : \"1656473756.000000000\",
 *	    \"state\" : \"closed\"
 *	  }
 *     ]
 *  }
 */

static struct sos_schema_template proc_template = {
	.name = STREAM,
	.uuid = UUID,
	.attrs = {
		{ .name = "job_id", .type = SOS_TYPE_UINT64 },
		{ .name = "component_id", .type = SOS_TYPE_UINT64 },
		{ .name = "producerName", .type = SOS_TYPE_STRING },
		{ .name = "pid", .type = SOS_TYPE_UINT64 },
		{ .name = "timestamp", .type = SOS_TYPE_STRING },
		{ .name = "task_rank", .type = SOS_TYPE_INT64 },
		{ .name = "parent", .type = SOS_TYPE_UINT64 },
		{ .name = "is_thread", .type = SOS_TYPE_UINT64 },
		{ .name = "exe", .type = SOS_TYPE_STRING },
		{ .name = "file", .type = SOS_TYPE_STRING },
		{ .name = "st_dev", .type = SOS_TYPE_STRING },
		{ .name = "st_ino", .type = SOS_TYPE_UINT64 },
		{ .name = "st_mode", .type = SOS_TYPE_UINT64 },
		{ .name = "st_size", .type = SOS_TYPE_UINT64 },
		{ .name = "mtime", .type = SOS_TYPE_STRING },
		{ .name = "state", .type = SOS_TYPE_STRING },
		{ .name = "time_job_component", .type = SOS_TYPE_JOIN,
		  .size = ARRAY_SIZE(time_job_component_attrs),
		  .indexed = 1,
		  .join_list = time_job_component_attrs
		},
		{ .name = "job_component_timestamp", .type = SOS_TYPE_JOIN,
		  .size = ARRAY_SIZE(job_component_timestamp_attrs),
		  .indexed = 1,
		  .join_list = job_component_timestamp_attrs
		},
		{ .name = "file_job_time", .type = SOS_TYPE_JOIN,
		  .size = ARRAY_SIZE(file_job_time_attrs),
		  .indexed = 1,
		  .join_list = file_job_time_attrs
		},

		{ 0 }
	}
};


enum attr_ids {
       JOB_ID,
       COMPONENT_ID,
       PRODUCERNAME_ID,
       PID_ID,
       TIMESTAMP_ID,
       TASK_RANK_ID,
       PARENT_ID,
       IS_THREAD_ID,
       EXE_ID,
       FILE_ID,
       ST_DEV_ID,
       ST_INO_ID,
       ST_MODE_ID,
       ST_SIZE_ID,
       MTIME_ID,
       STATE_ID,
};

static int create_schema(sos_t sos, sos_schema_t *app)
{
	int rc;
	sos_schema_t schema;

	/* Create and add the App schema */
	schema = sos_schema_from_template(&proc_template);
	if (!schema) {
		ovis_log(mylog, OVIS_LERROR, "%s: Error creating data schema %s: %s\n",
		       stream_store.name, STREAM, STRERROR(errno));
		rc = errno;
		goto err;
	}
	rc = sos_schema_add(sos, schema);
	if (rc) {
		ovis_log(mylog, OVIS_LERROR, "%s: Error %s adding stream %s data schema.\n",
				stream_store.name, STRERROR(rc), stream);
		goto err;
	}
	*app = schema;
	return 0;

err:
	if (schema) {
		free(schema);
	}
	return rc;

}

static int container_mode = 0660;	/* Default container permission bits */
static sos_t sos;
static int reopen_container(char *path)
{
	int rc = 0;
	ovis_log(mylog, OVIS_LDEBUG, "reopen '%s'\n", path);

	/* Close the container if it already exists */
	if (sos)
		sos_container_close(sos, SOS_COMMIT_ASYNC);


	/* Creates the container if it doesn't already exist  */
	sos = sos_container_open(path, SOS_PERM_RW|SOS_PERM_CREAT, container_mode);
	if (!sos) {
		rc = errno;
		if (rc == ENOENT) {
			int drc = f_mkdir_p(path, container_mode);
			if (drc != 0 && drc != EEXIST) {
				ovis_log(mylog, OVIS_LERROR, "Error creating the directory '%s': %s\n",
					path, STRERROR(drc));
				return drc;
			}
			int nrc = sos_container_new(path, container_mode);
			if (nrc) {
				ovis_log(mylog, OVIS_LERROR, "Error creating the container at '%s': %s\n",
				       path, STRERROR(nrc));
				return nrc;
			}
			sos = sos_container_open(path, SOS_PERM_RW|SOS_PERM_CREAT, container_mode);
			if (!sos) {
				rc = errno;
				ovis_log(mylog, OVIS_LERROR, "Error opening the new container at '%s': %s\n",
				       path, STRERROR(rc));
				return rc;
			}
		} else {
			ovis_log(mylog, OVIS_LERROR, "Error opening the old container at '%s': %s\n",
			       path, STRERROR(rc));
			return rc;
		}
	}

	app_schema = sos_schema_by_name(sos, proc_template.name);
	if (!app_schema) {
		rc = create_schema(sos, &app_schema);
		if (rc) {
			ovis_log(mylog, OVIS_LERROR, "Error creating schema %s in path %s: %s\n",
			       proc_template.name, path, STRERROR(rc));
			goto err;
	}
	}
	return 0;
err:
	sos_container_close(sos, SOS_COMMIT_ASYNC);
	sos = NULL;
	return rc;
}

static const char *usage(struct ldmsd_plugin *self)
{
	return	"config name=" STREAM "_store path=<path> port=<port_no> log=<path>\n"
		"     path	The path to the root of the SOS container store (required).\n"
		"     stream	The stream name to subscribe to (defaults to " STREAM ").\n"
		"     mode	The container permission mode for create, (defaults to 0660).\n";
}

static int stream_recv_cb(ldms_stream_event_t ev, void *ctxt);

static int config(struct ldmsd_plugin *self, struct attr_value_list *kwl, struct attr_value_list *avl)
{
	char *value;
	int rc;
	value = av_value(avl, "mode");
	if (value)
		container_mode = strtol(value, NULL, 0);
	if (!container_mode) {
		ovis_log(mylog, OVIS_LERROR,
		       "%s: ignoring bogus container permission mode of %s, using 0660.\n",
		       stream_store.name, value);
	}

	value = av_value(avl, "stream");
	if (value)
		stream = strdup(value);
	else
		stream = strdup(STREAM);
	ldms_stream_subscribe(stream, 0, stream_recv_cb, self, "linux_proc_sampler_files_store");

	value = av_value(avl, "path");
	if (!value) {
		ovis_log(mylog, OVIS_LERROR,
		       "%s: the path to the container (path=) must be specified.\n",
		       stream_store.name);
		return ENOENT;
	}

	if (root_path)
		free(root_path);
	root_path = strdup(value);
	if (!root_path) {
		ovis_log(mylog, OVIS_LERROR,
		       "%s: Error allocating %d bytes for the container path.\n",
		       strlen(value) + 1);
		return ENOMEM;
	}

	ovis_log(mylog, OVIS_LDEBUG, "config %s %s %o\n", root_path, stream, container_mode);
	rc = reopen_container(root_path);
	if (rc) {
		ovis_log(mylog, OVIS_LERROR, "%s: Error opening %s.\n",
		       stream_store.name, root_path);
		return ENOENT;
	}
	return 0;
}

static int get_json_value(json_entity_t e, char *name, int expected_type, json_entity_t *out)
{
	int v_type;
	json_entity_t a = json_attr_find(e, name);
	json_entity_t v;
	if (!a) {
		ovis_log(mylog, OVIS_LERROR,
		       "%s: The JSON entity is missing the '%s' attribute.\n",
		       stream_store.name,
		       name);
		return EINVAL;
	}
	v = json_attr_value(a);
	v_type = json_entity_type(v);
	if (v_type != expected_type) {
		ovis_log(mylog, OVIS_LERROR,
		       "%s: The '%s' JSON entity is the wrong type. "
		       "Expected %d, received %d\n",
		       stream_store.name,
		       name, expected_type, v_type);
		return EINVAL;
	}
	*out = v;
	return 0;
}


static int stream_recv_cb(ldms_stream_event_t ev, void *ctxt)
{
	int rc, task_rank;
	json_entity_t v, list, item;
	uint64_t job_id, component_id, pid, parent, is_thread;
	uint64_t st_ino, st_mode, st_size;
	char *producer_name, *timestamp, *exec;
	char *st_dev, *mtime, *state, *file_name;
	char *field;

	if (ev->type != LDMS_STREAM_EVENT_RECV)
		return 0;

	if (!ev->recv.json) {
		ovis_log(mylog, OVIS_LERROR,
		       "NULL entity received in stream callback.\n");
		return 0;
	}

	rc = get_json_value(ev->recv.json, field="job_id", JSON_INT_VALUE, &v);
	if (rc)
		goto err;
	job_id = json_value_int(v);

	rc = get_json_value(ev->recv.json, field="component_id", JSON_INT_VALUE, &v);
	if (rc)
		goto err;
	component_id = json_value_int(v);

	rc = get_json_value(ev->recv.json, field="producerName", JSON_STRING_VALUE, &v);
	if (rc)
		goto err;
	producer_name = json_value_str(v)->str;

	rc = get_json_value(ev->recv.json, field="pid", JSON_INT_VALUE, &v);
	if (rc)
		goto err;
	pid = json_value_int(v);

	rc = get_json_value(ev->recv.json, field="timestamp", JSON_STRING_VALUE, &v);
	if (rc)
		goto err;
	timestamp = json_value_str(v)->str;

	rc = get_json_value(ev->recv.json, field="task_rank", JSON_INT_VALUE, &v);
	if (rc)
		goto err;
	task_rank = json_value_int(v);

	rc = get_json_value(ev->recv.json, field="parent", JSON_INT_VALUE, &v);
	if (rc)
		goto err;
	parent = json_value_int(v);

	rc = get_json_value(ev->recv.json, field="is_thread", JSON_INT_VALUE, &v);
	if (rc)
		goto err;
	is_thread = json_value_int(v);

	rc = get_json_value(ev->recv.json, field="exe", JSON_STRING_VALUE, &v);
	if (rc)
		goto err;
	exec = json_value_str(v)->str;

	rc = get_json_value(ev->recv.json, field=LISTNAME, JSON_LIST_VALUE, &list);
	if (rc)
		goto err;
	for (item = json_item_first(list); item; item = json_item_next(item)) {

		if (json_entity_type(item) != JSON_DICT_VALUE) {
			ovis_log(mylog, OVIS_LERROR,
			       "%s: Items in segment must all be dictionaries.\n",
			       stream_store.name);
			rc = EINVAL;
			goto err;
		}

		rc = get_json_value(item, field="file", JSON_STRING_VALUE, &v);
		if (rc)
			goto err;
		file_name = json_value_str(v)->str;

		rc = get_json_value(item, field="st_dev", JSON_STRING_VALUE, &v);
		if (rc)
			goto err;
		st_dev = json_value_str(v)->str;

		rc = get_json_value(item, field="st_ino", JSON_INT_VALUE, &v);
		if (rc)
			goto err;
		st_ino = json_value_int(v);

		rc = get_json_value(item, field="st_mode", JSON_INT_VALUE, &v);
		if (rc)
			goto err;
		st_mode = json_value_int(v);

		rc = get_json_value(item, field="st_size", JSON_INT_VALUE, &v);
		if (rc)
			goto err;
		st_size = json_value_int(v);

		rc = get_json_value(item, field="mtime", JSON_STRING_VALUE, &v);
		if (rc)
			goto err;
		mtime = json_value_str(v)->str;

		rc = get_json_value(item, field="state", JSON_STRING_VALUE, &v);
		if (rc)
			goto err;
		state = json_value_str(v)->str;

		sos_obj_t obj = sos_obj_new(app_schema);
		if (!obj) {
			rc = errno;
			ovis_log(mylog, OVIS_LERROR,
			       "%s: Error %d creating Darshan data object.\n",
			       stream_store.name, errno);
			goto err;
		}

		ovis_log(mylog, OVIS_LDEBUG, "%s: Got a record from stream (%s)\n",
				stream_store.name, stream);


		sos_obj_attr_by_id_set(obj, JOB_ID, job_id);
		sos_obj_attr_by_id_set(obj, COMPONENT_ID, component_id);
		sos_obj_attr_by_id_set(obj, PRODUCERNAME_ID, strlen(producer_name)+1, producer_name);
		sos_obj_attr_by_id_set(obj, PID_ID, pid);
		sos_obj_attr_by_id_set(obj, TIMESTAMP_ID, strlen(timestamp)+1, timestamp);
		sos_obj_attr_by_id_set(obj, TASK_RANK_ID, task_rank);
		sos_obj_attr_by_id_set(obj, PARENT_ID, parent);
		sos_obj_attr_by_id_set(obj, IS_THREAD_ID, is_thread);
		sos_obj_attr_by_id_set(obj, EXE_ID, strlen(exec)+1,  exec);
		sos_obj_attr_by_id_set(obj, FILE_ID, strlen(file_name)+1, file_name);
		sos_obj_attr_by_id_set(obj, ST_DEV_ID, strlen(st_dev)+1, st_dev);
		sos_obj_attr_by_id_set(obj, ST_INO_ID, st_ino);
		sos_obj_attr_by_id_set(obj, ST_MODE_ID, st_mode);
		sos_obj_attr_by_id_set(obj, ST_SIZE_ID, st_size);
		sos_obj_attr_by_id_set(obj, MTIME_ID, strlen(mtime)+1, mtime);
		sos_obj_attr_by_id_set(obj, STATE_ID, strlen(state)+1, state);


		sos_obj_index(obj);
		sos_obj_put(obj);
	}
	rc = 0;
 err:
	if (rc)
		ovis_log(mylog, OVIS_LDEBUG, "got bad message: field=%s err=%d\n", field, rc);
	return rc;
}

static void term(struct ldmsd_plugin *self)
{
	ovis_log(mylog, OVIS_LDEBUG, "term %s\n", self->name);
	if (sos)
		sos_container_close(sos, SOS_COMMIT_ASYNC);
	free(root_path);
	root_path = NULL;
	free(stream);
	stream = NULL;
}

static struct ldmsd_plugin stream_store = {
	.name = STREAM "_store",
	.term = term,
	.config = config,
	.usage = usage,
};

struct ldmsd_plugin *get_plugin()
{
	int rc;
	mylog = ovis_log_register("store.linux_proc_sampler_files_store",
				"log subsystem of 'linux_proc_sampler_files_store' plugin");
	if (!mylog) {
		rc = errno;
		ovis_log(NULL, OVIS_LWARN, "Faild to create the log subsystem "
				"of 'linux_proc_sampler_files_store' plugin. Error %d\n", rc);
	}
	return &stream_store;
}
