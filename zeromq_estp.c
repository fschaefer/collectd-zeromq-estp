/**
 * collectd - src/zeromq_estp.c
 * Copyright (C) 2005-2010  Florian octo Forster
 * Copyright (C) 2009       Aman Gupta
 * Copyright (C) 2010       Julien Ammous
 * Copyright (C) 2012       Paul Colomiets
 * Copyright (C) 2014       Florian Schäfer
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; only version 2 of the License is applicable.
 *
 * This program is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin St, Fifth Floor, Boston, MA  02110-1301 USA
 *
 * Authors:
 *   Florian octo Forster <octo at verplant.org>
 *   Aman Gupta <aman at tmm1.net>
 *   Julien Ammous
 **/

#define _GNU_SOURCE
#include "collectd.h"
#include "common.h" /* auxiliary functions */
#include "plugin.h" /* plugin_register_*, plugin_dispatch_values */
#include "utils_avltree.h"
#include "network.h"

/* for htons() */
#if HAVE_ARPA_INET_H
# include <arpa/inet.h>
#endif
#include <pthread.h>
#include <alloca.h>
#include <time.h>
#include <stdio.h>

#include <czmq.h>

typedef struct estp_socket_s {
    void *zeromq_socket;
    pthread_mutex_t mutex;
} estp_socket_t;

typedef struct estp_cache_entry_s {
    char *fullname;
    char *timestamp;
    char *type;
    char *items;
    int nitems;
    unsigned long mask;
    value_t values[];
} estp_cache_entry_t;

static zctx_t *zeromq_context = NULL;
static c_avl_tree_t *staging = NULL;

static pthread_t *estp_receive_thread_ids = NULL;
static size_t estp_receive_thread_num = 0;
static int sending_sockets_num = 0;
static pthread_mutex_t staging_mutex;

// private data
static int thread_running = 1;
static pthread_t listen_thread_id;

static void
estp_close_callback(estp_socket_t *estp_socket)
{
    WARNING("ZeroMQ-ESTP: closing socket");
    if (zeromq_context != NULL && estp_socket->zeromq_socket != NULL) {
        zsocket_destroy(zeromq_context, estp_socket->zeromq_socket);
    }
    pthread_mutex_destroy(&estp_socket->mutex);
    free(estp_socket);
}

static void
dispatch_entry(char *fullname, char *timestamp, time_t interval,
    char *vtype, int values_len, value_t *values)
{

    struct tm timest;
    value_list_t vl;
    strcpy(vl.type, vtype);

    if (!strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ", &timest)) {
        WARNING("ZeroMQ-ESTP: can't parse timestamp");
        return;
    }
    // Hostname
    char *cpos = fullname;
    char *end = strchr(cpos, ':');
    if (!end) {
        WARNING("ZeroMQ-ESTP: No delimiter after hostname");
        return;
    }
    if (end - cpos > 63) {
        WARNING("ZeroMQ-ESTP: Too long hostname");
        return;
    }
    memcpy(vl.host, cpos, end-cpos);
    vl.host[end-cpos] = 0;

    // Plugin name
    cpos = end+1;
    end = strchr(cpos, ':');
    if (!end) {
        WARNING("ZeroMQ-ESTP: No delimiter after application/subsystem name");
        return;
    }
    if (end - cpos > 63) {
        WARNING("ZeroMQ-ESTP: Too long application/subsystem name");
        return;
    }
    memcpy(vl.plugin, cpos, end-cpos);
    vl.plugin[end-cpos] = 0;

    // Plugin instance
    cpos = end+1;
    end = strchr(cpos, ':');
    if (!end) {
        WARNING("ZeroMQ-ESTP: No delimiter after resource name");
        return;
    }
    if (end - cpos > 63) {
        WARNING("ZeroMQ-ESTP: Too long resource name");
        return;
    }
    memcpy(vl.plugin_instance, cpos, end-cpos);
    vl.plugin_instance[end-cpos] = 0;

    // Type instance
    cpos = end+1;
    end = strchr(cpos, ':');
    if (!end) {
        WARNING("ZeroMQ-ESTP: No delimiter after metric name");
        return;
    }
    if (end - cpos > 63) {
        WARNING("ZeroMQ-ESTP: Too long metric name");
        return;
    }
    memcpy(vl.type_instance, cpos, end-cpos);
    vl.type_instance[end-cpos] = 0;

    vl.time = TIME_T_TO_CDTIME_T(timegm(&timest));
    vl.interval = TIME_T_TO_CDTIME_T(interval);
    vl.values = values;
    vl.values_len = values_len;
    vl.meta = NULL;

    plugin_dispatch_values(&vl);
}

static char*
find_suffix(char *fullname)
{
    char *endsuffix = fullname + strlen(fullname)-1;
    if (*endsuffix != ':') {
        WARNING("ZeroMQ-ESTP: Metric full name not ends with ':'");
        return NULL;
    }
    char *suffix = endsuffix-1;
    while (suffix > fullname) {
        if (*suffix == '.' || *suffix == ':') {
            return suffix+1;
        }
        --suffix;
    }
    return NULL;
}

static int
find_field(char *suffix, char *items)
{
    int suffixlen = strlen(suffix) - 1  /* colon at the end */;
    int idx = 0;
    if (!strncmp(suffix, items, suffixlen) && (items[suffixlen] == 0 || items[suffixlen] == ',')) {
        return 0;
    }
    char *c;
    for (c = items; *c; ++c) {
        if (*c == ',') {
            c += 1;
            idx += 1;
            if (!strncmp(suffix, c, suffixlen) && (c[suffixlen] == 0 || c[suffixlen] == ',')) {
                return idx;
            }
        }
    }
    WARNING("ZeroMQ-ESTP: Can't find suffix in list of suffixes");
    return -1;
}

static void
dispatch_multi_entry(char *fullname, char *timestamp, time_t interval,
    char *type, char *items, value_t value)
{
    char *suffix = find_suffix(fullname);
    if (!suffix)
        return;
    int myindex = find_field(suffix, items);
    if (myindex < 0)
        return;

    int clen = suffix-fullname+2;
    char cutname[clen];
    memcpy(cutname, fullname, clen-2);
    cutname[clen-2] = ':';
    cutname[clen-1] = 0;

    estp_cache_entry_t *entry;
    int rc = c_avl_get(staging, cutname, (void *)&entry);

    if (rc == 0) {
        if (!strcmp(entry->timestamp, timestamp)
                && !strcmp(entry->type, type)
                && !strcmp(entry->items, items)) {
            entry->values[myindex] = value;
            entry->mask |= 1 << myindex;
            if (entry->mask == (1 << entry->nitems)-1) {
                dispatch_entry(cutname, timestamp, interval, type,
                        entry->nitems, entry->values);
                c_avl_remove(staging, cutname, NULL, NULL);
                free(entry);
            }
            return;
        }
        else {
            INFO("ZeroMQ-ESTP: Clearing cache entry as it is stale\n");
            c_avl_remove(staging, cutname, NULL, NULL);
            free(entry);
        }
    }
    // Ok, let's create new entry
    int tlen = strlen(timestamp) + 1;
    int ylen = strlen(type) + 1;
    int ilen = strlen(items) + 1;
    int nitems = 1;
    char *c;
    for (c = items; *c; ++c) {
        if (*c == ',') {
            nitems += 1;
        }
    }

    entry = smalloc(sizeof(estp_cache_entry_t) + nitems * sizeof(value_t) + clen + tlen + ylen + ilen);
    if (!entry) {
        WARNING("Not enough memory for new entry");
        return;
    }
    char *curbuf = ((char *)entry) + sizeof(estp_cache_entry_t) + nitems*sizeof(value_t);
    memcpy(curbuf, cutname, clen);
    entry->fullname = curbuf;
    curbuf += clen;
    memcpy(curbuf, timestamp, tlen);
    entry->timestamp = curbuf;
    curbuf += tlen;
    memcpy(curbuf, type, ylen);
    entry->type = curbuf;
    curbuf += ylen;
    memcpy(curbuf, items, ilen);
    entry->items = curbuf;
    curbuf += ilen;
    entry->nitems = nitems;
    entry->mask = 1 << myindex;
    entry->values[myindex] = value;

    c_avl_insert(staging, entry->fullname, entry);
}

static void
parse_message(char *data, int dlen)
{
    char fullname[300];
    char timestamp[32];
    unsigned long interval;
    char value[64];

    char *endline = memchr(data, '\n', dlen);
    int headlen = dlen;
    if (endline) {
        headlen = endline - data;
    }
    char *hdata = alloca(headlen+1);
    memcpy(hdata, data, headlen);
    hdata[headlen] = 0;

    int rc = sscanf(hdata, "ESTP:%300s %31s %lu %63s", fullname, timestamp, &interval, value);
    if (rc != 4) {
        WARNING("ZeroMQ-ESTP: message has wrong format");
        return;
    }

    int vdouble = 0;
    double dvalue;
    char *end;
    long lvalue = strtol(value, &end, 10);

    if (*end == '.') {
        dvalue = strtod(value, &end);
        vdouble = 1;
    }
    if (end == hdata) {
        WARNING("ZeroMQ-ESTP: wrong value");
        return;
    }

    char vtype[64];
    value_t val;

    // Parse type
    if (*end == ':') {
        ++end;
        if (*end == 'd') {
            strcpy(vtype, "derive");  // FIXME: derive can be negative
            if (vdouble) {
                val.derive = dvalue;
            }
            else {
                val.derive = lvalue;
            }
        }
        else if (*end == 'c') {
            strcpy(vtype, "derive");  // non-wrapping counter
            if (vdouble) {
                val.counter = dvalue;
            }
            else {
                val.counter = lvalue;
            }
        }
        else if (*end == 'a') {
            strcpy(vtype, "absolute");
            if (vdouble) {
                val.absolute = dvalue;
            }
            else {
                val.absolute = lvalue;
            }
        }
        else {
            WARNING("ZeroMQ-ESTP: Unknown type");
            return;
        }
    }
    else {
        strcpy(vtype, "gauge");
        if (vdouble) {
            val.gauge = dvalue;
        }
        else {
            val.gauge = lvalue;
        }
    }

    //  Let's find extension
    if (endline) { //  there is extension data
        char *ext = memmem(endline, dlen - headlen, "\n :collectd:", 12);
        if (ext) { //  collectd extension found
            ext += 12;
            char *endext = memchr(ext, '\n', data + dlen - ext);
            int elen;
            if (!endext) {
                elen = data + dlen - ext;
            }
            else {
                elen = endext - ext;
            }
            char *fullext = alloca(elen+1);
            memcpy(fullext, ext, elen);
            fullext[elen] = 0;
            while (1) {
                char ekey[24];
                char evalue[elen];
                int chars;
                if (sscanf(fullext, " %23[^=]=%s%n", ekey, evalue, &chars) < 2) {
                    break;
                }

                fullext += chars;

                if (!strcmp(ekey, "type")) {
                    strncpy(vtype, evalue, 64);
                }
                else if (!strcmp(ekey, "items")) {
                    pthread_mutex_lock(&staging_mutex);
                    dispatch_multi_entry(fullname, timestamp, interval,
                            vtype, evalue, val);
                    pthread_mutex_unlock(&staging_mutex);
                    return;
                }
            }
        }
    }

    dispatch_entry(fullname, timestamp, interval, vtype, 1, &val);
}

static void*
estp_receive_thread(void *zeromq_socket)
{
    assert(zeromq_socket != NULL);

    while (thread_running) {

        char *msg = NULL;
        msg = zstr_recv(zeromq_socket);

        if (msg == NULL) {
            if ((errno == EAGAIN) || (errno == EINTR)) {
                continue;
            }

            ERROR("ZeroMQ-ESTP: zstr_recv failed: %s", zmq_strerror(errno));
            break;
        }

        parse_message(msg, strlen(msg));

        free(msg);

        DEBUG("ZeroMQ-ESTP: received data, parse returned %d", status);

    }

    DEBUG("ZeroMQ-ESTP: Receive thread is terminating.");
    zsocket_destroy(zeromq_context, zeromq_socket);

    return NULL;
}

#define PACKAGE_SIZE 640

static int
put_single_value(estp_socket_t *sockstr, const char *name, value_t value,
    const value_list_t *vl, const data_source_t *ds, char *extdata)
{
    char data[PACKAGE_SIZE];
    char tstring[32];
    time_t timeval = CDTIME_T_TO_TIME_T(vl->time);
    unsigned interval = CDTIME_T_TO_TIME_T(vl->interval);
    struct tm tstruct;
    gmtime_r(&timeval, &tstruct);
    strftime(tstring, 32, "%Y-%m-%dT%H:%M:%SZ", &tstruct);

    if (ds->type == DS_TYPE_COUNTER) {
        snprintf(data, PACKAGE_SIZE, "ESTP:%s:%s:%s:%s: %s %d %llu:c%s\n",
                vl->host, vl->plugin, vl->plugin_instance, name,
                tstring, interval, value.counter, extdata);
    }
    else if (ds->type == DS_TYPE_GAUGE) {
        snprintf(data, PACKAGE_SIZE, "ESTP:%s:%s:%s:%s: %s %d %lf%s\n",
                vl->host, vl->plugin, vl->plugin_instance, name,
                tstring, interval, value.gauge, extdata);
    }
    else if (ds->type == DS_TYPE_DERIVE) {
        snprintf(data, PACKAGE_SIZE, "ESTP:%s:%s:%s:%s: %s %d %ld:d%s\n",
                vl->host, vl->plugin, vl->plugin_instance, name,
                tstring, interval, (long int)value.derive, extdata);
    }
    else if (ds->type == DS_TYPE_ABSOLUTE) {
        snprintf(data, PACKAGE_SIZE, "ESTP:%s:%s:%s:%s: %s %d %lu:a%s\n",
                vl->host, vl->plugin, vl->plugin_instance, name,
                tstring, interval, (long unsigned int)value.absolute, extdata);
    }
    else {
        WARNING("ZeroMQ-ESTP: Unknown type");
        return -1;
    }

    pthread_mutex_lock(&sockstr->mutex);
    int rc = zstr_send(sockstr->zeromq_socket, data);
    pthread_mutex_unlock(&sockstr->mutex);
    if (rc != 0) {
        if (errno == EAGAIN) {
            WARNING("ZeroMQ-ESTP: Unable to queue message, queue may be full");
            return -1;
        }
        else {
            ERROR("ZeroMQ-ESTP: zstr_send : %s", zmq_strerror(errno));
            return -1;
        }
    }

    DEBUG("ZeroMQ-ESTP:: data sent");

    return 0;
}

static int
estp_write_value(const data_set_t *ds, const value_list_t *vl, user_data_t *user_data)
{
    assert(vl->values_len == ds->ds_num);
    if (ds->ds_num > 1) {
        int i;

        int elen = 100; // prefix and type
        for (i = 0; i < ds->ds_num; ++i)
            elen += strlen(ds->ds[i].name) + 1;
        char *extdata = alloca(elen);
        char *cur = extdata + sprintf(extdata, "\n :collectd: type=%s items=%s", ds->type, ds->ds[0].name);
        for (i = 1; i < ds->ds_num; ++i) {
            *cur++ = ',';
            int len = strlen(ds->ds[i].name);
            memcpy(cur, ds->ds[i].name, len);
            cur += len;
        }
        *cur = 0;

        for (i = 0; i < ds->ds_num; ++i) {
            if (*vl->type_instance) {
                char name[64];
                int len;
                len = snprintf(name, 63, "%s.%s",
                        vl->type_instance, ds->ds[i].name);
                name[len] = 0;
                put_single_value((estp_socket_t *)user_data->data,
                        name, vl->values[i], vl, &ds->ds[i], extdata);
            }
            else {
                put_single_value((estp_socket_t *)user_data->data,
                        ds->ds[i].name, vl->values[i], vl, &ds->ds[i], extdata);
            }
        }
    }
    else {
        char extdata[100] = "";
        if (strcmp(ds->type, "gauge")
                && strcmp(ds->type, "counter")
                && strcmp(ds->type, "derive")
                && strcmp(ds->type, "absolute")) {
            sprintf(extdata, "\n :collectd: type=%s", ds->type);
        }
        put_single_value((estp_socket_t *)user_data->data,
                vl->type_instance, vl->values[0], vl, &ds->ds[0], extdata);
    }
    return 0;
}


static int
estp_config_mode(oconfig_item_t *ci)
{
    char buffer[64] = "";
    int status;

    status = cf_util_get_string_buffer(ci, buffer, sizeof (buffer));
    if (status != 0) {
        return -1;
    }
    else if (strcasecmp("PUB", buffer) == 0) {
        return ZMQ_PUB;
    }
    else if (strcasecmp("SUB", buffer) == 0) {
        return ZMQ_SUB;
    }
    else if (strcasecmp("PUSH", buffer) == 0) {
        return ZMQ_PUSH;
    }
    else if (strcasecmp("PULL", buffer) == 0) {
        return ZMQ_PULL;
    }

    ERROR("ZeroMQ-ESTP: Unrecognized communication pattern: \"%s\"", buffer);
    return -1;
}

static int
estp_config_socket(oconfig_item_t *ci)
{
    int type;
    int status;
    int i;
    int endpoints_num;
    void *zeromq_socket;

    type = estp_config_mode(ci);
    if (type < 0) {
        return -1;
    }

    if (zeromq_context == NULL) {

        zeromq_context = zctx_new();

        if (zeromq_context == NULL) {
            ERROR("ZeroMQ-ESTP: Initializing ZeroMQ failed: %s", zmq_strerror(errno));
            return -1;
        }

        INFO("ZeroMQ-ESTP: Initializing ZeroMQ context");
    }

    /* Create a new socket */
    zeromq_socket = zsocket_new(zeromq_context, type);
    if (zeromq_socket == NULL) {
        ERROR("ZeroMQ-ESTP: zsocket_new failed: %s", zmq_strerror(errno));
        return -1;
    }

    zsocket_set_linger(zeromq_socket, 0);
    zsocket_set_sndtimeo(zeromq_socket, 1000);

    if (type == ZMQ_SUB) {
        /* Subscribe to all messages */
        /* TODO(tailhook) implement subscription configuration */
        zsocket_set_subscribe(zeromq_socket, "");
    }

    /* Iterate over all children and do all the binds and connects requested. */
    endpoints_num = 0;
    for (i = 0; i < ci->children_num; i++) {
        oconfig_item_t *child = ci->children + i;

        if (strcasecmp("BIND", child->key) == 0) {

            char *endpoint = NULL;

            status = cf_util_get_string(child, &endpoint);
            if (status != 0) {
                continue;
            }

            INFO("Binding to %s", endpoint);
            status = zsocket_bind(zeromq_socket, endpoint);
            if (status == -1) {
                ERROR("ZeroMQ-ESTP: zsocket_bind(\"%s\") failed: %s", endpoint, zmq_strerror(errno));
                sfree(endpoint);
                continue;
            }

            sfree(endpoint);
            endpoints_num++;

            continue;

        } /* BIND */
        else if (strcasecmp("CONNECT", child->key) == 0) {

            char *endpoint = NULL;

            status = cf_util_get_string(child, &endpoint);
            if (status != 0) {
                continue;
            }

            INFO("Connecting to %s", endpoint);
            status = zsocket_connect(zeromq_socket, endpoint);
            if (status != 0) {
                ERROR("ZeroMQ-ESTP: zsocket_connect(\"%s\") failed: %s", endpoint, zmq_strerror(errno));
                sfree(endpoint);
                continue;
            }

            sfree(endpoint);
            endpoints_num++;

            continue;

        } /* CONNECT */
        else if (strcasecmp("SNDHWM", child->key) == 0) {

            int hwm;
            status = cf_util_get_int(child, &hwm);
            if (status != 0) {
                continue;
            }

            zsocket_set_sndhwm(zeromq_socket, hwm);

            continue;

        } /* SNDHWM */
        else if (strcasecmp("RCVHWM", child->key) == 0) {

            int hwm;
            status = cf_util_get_int(child, &hwm);
            if (status != 0) {
                continue;
            }

            zsocket_set_rcvhwm(zeromq_socket, hwm);

            continue;

        } /* RCVHWM */
        else if (strcasecmp("LINGER", child->key) == 0) {

            int linger;
            status = cf_util_get_int(child, &linger);
            if (status != 0) {
                continue;
            }

            zsocket_set_linger(zeromq_socket, linger);

            continue;

        } /* LINGER */
        else if (strcasecmp("SNDTIMEO", child->key) == 0) {

            int sndtimeo;
            status = cf_util_get_int(child, &sndtimeo);
            if (status != 0) {
                continue;
            }

            zsocket_set_sndtimeo(zeromq_socket, sndtimeo);

            continue;

        } /* SNDTIMEO */
        else if (strcasecmp("RCVTIMEO", child->key) == 0) {

            int rcvtimeo;
            status = cf_util_get_int(child, &rcvtimeo);
            if (status != 0) {
                continue;
            }

            zsocket_set_rcvtimeo(zeromq_socket, rcvtimeo);

            continue;

        } /* RCVTIMEO */
        else if (strcasecmp("SUBSCRIBE", child->key) == 0) {

            char *filter = NULL;

            status = cf_util_get_string(child, &filter);
            if (status != 0) {
                continue;
            }

            if (type == ZMQ_SUB) {
                zsocket_set_unsubscribe(zeromq_socket, "");
                zsocket_set_subscribe(zeromq_socket, filter);
            }

            sfree(filter);

            continue;

        } /* SUBSCRIBE */
        else {
            ERROR("ZeroMQ-ESTP: The \"%s\" config option is now allowed here.", child->key);
        }
    } /* for (i = 0; i < ci->children_num; i++) */

    if (endpoints_num == 0) {
        ERROR("ZeroMQ-ESTP: No (valid) \"Bind\"/\"Connect\" option was found in this \"Socket\" block.");
        zsocket_destroy(zeromq_context, zeromq_socket);
        return -1;
    }

    /* If this is a receiving socket, create a new receive thread */
    if ((type == ZMQ_SUB) || (type == ZMQ_PULL)) {
        pthread_t *thread_ptr;

        thread_ptr = realloc(estp_receive_thread_ids, sizeof(*estp_receive_thread_ids) * (estp_receive_thread_num + 1));
        if (thread_ptr == NULL) {
            ERROR("ZeroMQ-ESTP: realloc failed.");
            return -1;
        }
        estp_receive_thread_ids = thread_ptr;
        thread_ptr = estp_receive_thread_ids + estp_receive_thread_num;

        status = pthread_create(thread_ptr,
                /* attr = */ NULL,
                /* func = */ estp_receive_thread,
                /* args = */ zeromq_socket);
        if (status != 0) {
            char errbuf[1024];
            ERROR("ZeroMQ-ESTP: pthread_create failed: %s", sstrerror(errno, errbuf, sizeof(errbuf)));
            zsocket_destroy(zeromq_context, zeromq_socket);
            return -1;
        }

        estp_receive_thread_num++;
    }

    /* If this is a sending socket, register a new write function */
    else if ((type == ZMQ_PUB) || (type == ZMQ_PUSH)) {
        user_data_t userdata = { NULL, NULL };
        estp_socket_t *estp_socket = smalloc(sizeof(estp_socket_t));

        estp_socket->zeromq_socket = zeromq_socket;

        pthread_mutex_init(&estp_socket->mutex, NULL);

        userdata.data = estp_socket;
        userdata.free_func = (void*) estp_close_callback;

        char name[20];
        ssnprintf(name, sizeof(name), "zeromq/%i", sending_sockets_num);
        sending_sockets_num++;

        INFO("ZeroMQ-ESTP: Registering thread : %s", name);

        plugin_register_write(name, estp_write_value, &userdata);
    }

    return 0;
}

/*
 * Config schema:
 *
 * <Plugin "zeromq_estp">
 *
 *   <Socket PUB>
 *     HWM 300
 *     Connect "tcp://localhost:6666"
 *   </Socket>
 *   <Socket SUB>
 *     Bind "tcp://eth0:6666"
 *     Bind "tcp://collectd.example.com:6666"
 *   </Socket>
 * </Plugin>
 */
static int
estp_config(oconfig_item_t *ci)
{
    int i;
    for (i = 0; i < ci->children_num; i++) {
        oconfig_item_t *child = ci->children + i;

        if (strcasecmp("SOCKET", child->key) == 0) {
            estp_config_socket(child);
        }
        else {
            WARNING("ZeroMQ-ESTP: The \"%s\" config option is not allowed here.", child->key);
        }
    }

    return 0;
}

static int
estp_init()
{
    int major, minor, patch;
    zmq_version(&major, &minor, &patch);
    INFO("ZeroMQ-ESTP: loaded (ZeroMQ v%d.%d.%d).", major, minor, patch);

    pthread_mutex_init(&staging_mutex, NULL);

    staging = c_avl_create((void *) strcmp);
    if (staging == NULL) {
        ERROR("ZeroMQ-ESTP : c_avl_create failed: %s", strerror(errno));
        return -1;
    }

    return 0;
}

static int
estp_shutdown()
{
    if (zeromq_context) {
        thread_running = 0;
        DEBUG("ZeroMQ: shutting down");
        zctx_destroy(&zeromq_context);
        pthread_join(listen_thread_id, NULL);
    }

    if (staging) {
        while (1) {
            char *key = NULL;
            estp_cache_entry_t *value = NULL;

            int rc = c_avl_pick(staging, (void *) &key, (void *) &value);
            if (rc != 0)
                break;

            free(key);
            free(value);
        }

        c_avl_destroy(staging);
    }

    pthread_mutex_destroy(&staging_mutex);

    return 0;
}

void
module_register (void)
{
    plugin_register_complex_config("zeromq_estp", estp_config);
    plugin_register_init("zeromq_estp", estp_init);
    plugin_register_shutdown("zeromq_estp", estp_shutdown);
}
