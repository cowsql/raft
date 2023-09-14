#include <assert.h>
#include <fcntl.h>
#include <linux/perf_event.h>
#include <stdbool.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mman.h>
#include <sys/resource.h>
#include <sys/syscall.h>
#include <time.h>
#include <unistd.h>

#include "profiler.h"

#define PATH_TEMPLATE "/sys/kernel/tracing/events/%s/enable"

#define PAGE_SIZE 4096
#define N_SAMPLE_PAGES 1024
#define MMAP_SIZE (PAGE_SIZE * (1 /* header pager */ + N_SAMPLE_PAGES))

#define DEVICE_NAME_LEN 32

#define u8 uint8_t
#define u16 uint16_t
#define u32 uint32_t
#define u64 uint64_t

/* Datasource types. */
enum {
    NVME = 0,
    BLOCK,
};

/* Tracepoint types. */
enum {
    NVME_SETUP_CMD = 0,
    NVME_COMPLETE_RQ,
};

enum {
    BLOCK_BIO_QUEUE = 0,
    BLOCK_RQ_COMPLETE,
    BLOCK_BIO_COMPLETE,
};

struct sample
{
    struct perf_event_header header;
    u64 time;     /* if PERF_SAMPLE_TIME */
    u32 cpu, res; /* if PERF_SAMPLE_CPU */
    u32 size;     /* if PERF_SAMPLE_RAW */
};

struct tracepoint
{
    u16 common_type;
    u8 common_flag;
    u8 common_preempt_count;
    u32 common_pid;
};

struct nvmeSetupCmd
{
    struct tracepoint tp;
    char device[DEVICE_NAME_LEN];
    int ctrl_id;
    int qid;
    u8 opcode;
    u8 flags;
    u8 fctype;
    u16 cid;
    u32 nsid;
    bool metdata;
    u8 cdw10[24];
};

struct nvmeCompleteRq
{
    struct tracepoint tp;
    char device[DEVICE_NAME_LEN];
    int ctrl_id;
    int qid;
    int cid;
    u64 result;
    u8 retries;
    u8 flags;
    u16 status;
};

struct blockBioQueue
{
    struct tracepoint tp;
    u32 dev;
    u64 sector;
    u32 nr_sector;
};

struct blockRqComplete
{
    struct tracepoint tp;
    u32 dev;
    u64 sector;
    u32 nr_sector;
};

struct blockBioComplete
{
    struct tracepoint tp;
    u32 dev;
    u64 sector;
    u32 nr_sector;
};

static int perf_event_open(struct perf_event_attr *hw_event,
                           pid_t pid,
                           int cpu,
                           int group_fd,
                           unsigned long flags)
{
    int rv;
    rv = (int)syscall(SYS_perf_event_open, hw_event, pid, cpu, group_fd, flags);
    return rv;
}

/* Return the exact size of the PERF_RECORD_SAMPLE struct, without alignment. */
static size_t sizeofSample(void)
{
    return sizeof(struct perf_event_header) +
           sizeof(u64) + /* PERF_SAMPLE_TIME */
           sizeof(u64) + /* PERF_SAMPLE_CPU */
           sizeof(u32) /* if PERF_SAMPLE_RAW */;
}

static unsigned eventId(const char *name)
{
    char path[1024];
    int fd;
    int rv;
    char n[32];

    sprintf(path, "/sys/kernel/tracing/events/%s/id", name);
    fd = open(path, O_RDONLY);
    if (fd < 0) {
        perror("open tracefs id");
        exit(1);
    }

    rv = (int)read(fd, n, sizeof n);
    if (rv < 0) {
        perror("read tracefs id");
        exit(1);
    }

    close(fd);

    return (unsigned)atoi(n);
}

static int profilerEventGroupAdd(struct ProfilerEventGroup *g, unsigned type)
{
    struct perf_event_attr attr;
    unsigned i = g->n_events;
    unsigned long flags;
    bool is_leader;
    int group_fd;

    is_leader = g->n_events == 0;

    g->n_events++;
    g->events = realloc(g->events, sizeof *g->events * g->n_events);
    assert(g->events != NULL);

    memset(&attr, 0, sizeof attr);

    attr.type = PERF_TYPE_TRACEPOINT;
    attr.size = sizeof attr;
    attr.config = type;
    attr.sample_type = (PERF_SAMPLE_TIME | PERF_SAMPLE_CPU | PERF_SAMPLE_RAW);
    attr.sample_period = 1;
    attr.disabled = is_leader ? 1 : 0;
    attr.use_clockid = 1;
    attr.clockid = CLOCK_MONOTONIC;
    attr.read_format = is_leader ? PERF_FORMAT_GROUP : 0;

    group_fd = is_leader ? -1 : g->events[0];
    flags = is_leader ? 0 : PERF_FLAG_FD_OUTPUT;

    g->events[i] = perf_event_open(&attr, -1, g->cpu, group_fd, flags);
    if (g->events[i] == -1) {
        perror("perf_event_open child");
        return -1;
    }

    if (is_leader) {
        g->data = mmap(NULL, MMAP_SIZE, PROT_READ, MAP_SHARED, g->events[0], 0);
        if (g->data == NULL) {
            printf("perf: mmap error\n");
            return -1;
        }
    }

    return 0;
}

static int profilerEventGroupInit(struct ProfilerEventGroup *g,
                                  int cpu,
                                  struct Profiler *p)
{
    int rv;

    g->cpu = cpu;

    g->events = NULL;
    g->n_events = 0;
    g->p = p;

    rv = profilerEventGroupAdd(g, p->nvme.types[NVME_SETUP_CMD]);
    if (rv != 0) {
        return -1;
    }

    rv = profilerEventGroupAdd(g, p->nvme.types[NVME_COMPLETE_RQ]);
    if (rv != 0) {
        return -1;
    }

    rv = profilerEventGroupAdd(g, p->block.types[BLOCK_BIO_QUEUE]);
    if (rv != 0) {
        return -1;
    }

    rv = profilerEventGroupAdd(g, p->block.types[BLOCK_RQ_COMPLETE]);
    if (rv != 0) {
        return -1;
    }

    rv = profilerEventGroupAdd(g, p->block.types[BLOCK_BIO_COMPLETE]);
    if (rv != 0) {
        return -1;
    }

    return 0;
}

static void profilerInitDataSource(struct ProfilerDataSource *d, int code)
{
    memset(d, 0, sizeof *d);

    d->n_commands = 0;

    switch (code) {
        case NVME:
            d->types[NVME_SETUP_CMD] = eventId("nvme/nvme_setup_cmd");
            d->types[NVME_COMPLETE_RQ] = eventId("nvme/nvme_complete_rq");
            break;
        case BLOCK:
            d->types[BLOCK_BIO_QUEUE] = eventId("block/block_bio_queue");
            d->types[BLOCK_RQ_COMPLETE] = eventId("block/block_rq_complete");
            d->types[BLOCK_BIO_COMPLETE] = eventId("block/block_bio_complete");
            break;
        default:
            assert(0);
            break;
    }
}

static int profilerInitPerf(struct Profiler *p)
{
    unsigned i;
    int rv;

    profilerInitDataSource(&p->nvme, NVME);
    profilerInitDataSource(&p->block, BLOCK);

    p->n_groups = (unsigned)sysconf(_SC_NPROCESSORS_ONLN);
    assert(p->n_groups > 0);

    p->groups = malloc(sizeof *p->groups * p->n_groups);
    assert(p->groups != NULL);

    for (i = 0; i < p->n_groups; i++) {
        rv = profilerEventGroupInit(&p->groups[i], (int)i, p);
        if (rv != 0) {
            return -1;
        }
    }

    return 0;
}

void ProfilerInit(struct Profiler *p, struct FsFileInfo *device)
{
    p->device = device;
    p->n_traces = 0;
    p->n_groups = 0;
    p->switches = 0;
}

int ProfilerPerf(struct Profiler *p)
{
    int rv;
    rv = profilerInitPerf(p);
    if (rv != 0) {
        return rv;
    }
    return 0;
}

static void profilerEventGroupClose(struct ProfilerEventGroup *g)
{
    unsigned i;

    for (i = 0; i < g->n_events; i++) {
        close(g->events[i]);
    }
    free(g->events);

    munmap(g->data, MMAP_SIZE);
}

void ProfilerClose(struct Profiler *p)
{
    unsigned i;

    for (i = 0; i < p->n_groups; i++) {
        profilerEventGroupClose(&p->groups[i]);
    }

    free(p->groups);
}

void ProfilerTrace(struct Profiler *p, const char *name)
{
    p->traces[p->n_traces] = name;
    p->n_traces++;
}

static int profilerTraceFsWrite(const char *name, const char *text)
{
    char path[1024];
    FILE *file;
    sprintf(path, PATH_TEMPLATE, name);
    file = fopen(path, "w");

    if (file == NULL) {
        perror("fopen tracefs");
        return -1;
    }
    fprintf(file, "%s", text);
    fclose(file);

    return 0;
}

static int profilerTraceFsStart(const char *name)
{
    return profilerTraceFsWrite(name, "1");
}

static int contextSwitchCounterStart(unsigned *counter)
{
    struct rusage usage;
    int rv;

    rv = getrusage(RUSAGE_SELF, &usage);
    if (rv != 0) {
        return -1;
    }

    *counter = (unsigned)usage.ru_nvcsw;

    return 0;
}

static int profilerEventGroupStart(struct ProfilerEventGroup *g)
{
    int rv;

    rv = ioctl(g->events[0], PERF_EVENT_IOC_RESET, 0);
    if (rv != 0) {
        perror("ioctl reset perf");
        return -1;
    }

    rv = ioctl(g->events[0], PERF_EVENT_IOC_ENABLE, 0);
    if (rv != 0) {
        perror("ioctl enable perf");
        return -1;
    }

    return 0;
}

int ProfilerStart(struct Profiler *p)
{
    unsigned i;
    int rv;

    for (i = 0; i < p->n_traces; i++) {
        rv = profilerTraceFsStart(p->traces[i]);
        if (rv != 0) {
            return rv;
        }
    }

    rv = contextSwitchCounterStart(&p->switches);
    if (rv != 0) {
        return rv;
    }

    if (p->device->driver != FS_DRIVER_GENERIC) {
        for (i = 0; i < p->n_groups; i++) {
            rv = profilerEventGroupStart(&p->groups[i]);
            if (rv != 0) {
                return -1;
            }
        }
    }

    return 0;
}

static int profilerTraceFsStop(const char *name)
{
    return profilerTraceFsWrite(name, "0");
}

static int contextSwitchCounterStop(unsigned *counter)
{
    struct rusage usage;
    int rv;

    rv = getrusage(RUSAGE_SELF, &usage);
    if (rv != 0) {
        return -1;
    }

    *counter = (unsigned)usage.ru_nvcsw - *counter;

    return 0;
}

static int processNvmeSetupCmd(struct ProfilerEventGroup *g,
                               struct sample *s,
                               struct nvmeSetupCmd *t)
{
    struct ProfilerDataSource *data = &g->p->nvme;
    struct FsFileInfo *device = g->p->device;
    unsigned i = data->n_commands;
    u64 slba = *(u64 *)(t->cdw10);
    u16 control = *(u16 *)(t->cdw10 + 10);
    u16 flags;

    /* Skip writes targeted to other partitions. */
    if (slba < device->block_dev_start || slba >= device->block_dev_end) {
        return 0;
    }

    if (t->opcode != 0x01 /* Write opcode, from linux/nvme.h */) {
        printf("unexpected nvme opcode 0x%x\n", t->opcode);
        return -1;
    }

    /* When the device has power-loss protection, we expect to have no flush
     * flags in the NVMe command. */
    if (device->block_dev_write_through) {
        flags = 0x0;
    } else {
        flags = 0x4000;
    }

    if (control != flags /* Possible flush flag (check trace output) */) {
        printf("unexpected nvme control 0x%x vs 0x%x\n", control, flags);
        return -1;
    }

    data->commands[i].id = t->cid;
    data->commands[i].start = s->time;
    data->n_commands++;
    return 0;
}

static int processNvmeCompleteRq(struct ProfilerEventGroup *g,
                                 struct sample *s,
                                 struct nvmeCompleteRq *t)
{
    struct ProfilerDataSource *data = &g->p->nvme;
    unsigned i;

    for (i = 0; i < data->n_commands; i++) {
        if (data->commands[i].id != (unsigned long long)t->cid) {
            continue;
        }
        data->commands[i].duration = s->time - data->commands[i].start;
        return 0;
    }

    /* The setup for this completion was probably skipped. TODO: make sure of
     * that. */

    return 0;
}

static int processBlockBioQueue(struct ProfilerEventGroup *g,
                                struct sample *s,
                                struct blockBioQueue *t)
{
    struct ProfilerDataSource *data = &g->p->block;
    struct FsFileInfo *device = g->p->device;
    unsigned i = data->n_commands;

    /* Skip writes targeted to other partitions. */
    if (t->sector < device->block_dev_start ||
        t->sector >= device->block_dev_end) {
        return 0;
    }

    data->commands[i].id = t->sector;
    data->commands[i].start = s->time;
    data->commands[i].duration = 0;
    data->n_commands++;

    return 0;
}

static int processBlockRqComplete(struct ProfilerEventGroup *g,
                                  struct sample *s,
                                  struct blockRqComplete *t)
{
    struct ProfilerDataSource *data = &g->p->block;
    struct FsFileInfo *device = g->p->device;
    unsigned i;

    /* Skip writes targeted to other partitions. */
    if (t->sector < device->block_dev_start ||
        t->sector >= device->block_dev_end) {
        return 0;
    }

    for (i = 0; i < data->n_commands; i++) {
        if (data->commands[i].id != t->sector) {
            continue;
        }
        if (data->commands[i].duration != 0) {
            printf("unexpected non-zero block command duration\n");
            return -1;
        }
        data->commands[i].duration = s->time - data->commands[i].start;
        return 0;
    }
    return -1;
}

static int processBlockBioComplete(struct ProfilerEventGroup *g,
                                   struct sample *s,
                                   struct blockBioComplete *t)
{
    struct ProfilerDataSource *data = &g->p->block;
    struct FsFileInfo *device = g->p->device;
    unsigned i;

    /* Skip writes targeted to other partitions. */
    if (t->sector < device->block_dev_start ||
        t->sector >= device->block_dev_end) {
        return 0;
    }

    for (i = 0; i < data->n_commands; i++) {
        if (data->commands[i].id != t->sector) {
            continue;
        }
        if (data->commands[i].duration != 0) {
            printf("unexpected non-zero block command duration\n");
            return -1;
        }
        data->commands[i].duration = s->time - data->commands[i].start;
        return 0;
    }
    return -1;
}

static int processPerfSample(struct ProfilerEventGroup *g,
                             struct sample *s,
                             u32 *size)
{
    struct tracepoint *t = (void *)(((char *)s) + sizeofSample());

    assert(s->header.type == PERF_RECORD_SAMPLE);
    *size = s->header.size;

    if (t->common_type == g->p->nvme.types[NVME_SETUP_CMD]) {
        return processNvmeSetupCmd(g, s, (void *)t);
    }

    if (t->common_type == g->p->nvme.types[NVME_COMPLETE_RQ]) {
        return processNvmeCompleteRq(g, s, (void *)t);
    }

    if (t->common_type == g->p->block.types[BLOCK_BIO_QUEUE]) {
        return processBlockBioQueue(g, s, (void *)t);
    }

    if (t->common_type == g->p->block.types[BLOCK_RQ_COMPLETE]) {
        return processBlockRqComplete(g, s, (void *)t);
    }

    if (t->common_type == g->p->block.types[BLOCK_BIO_COMPLETE]) {
        return processBlockBioComplete(g, s, (void *)t);
    }

    return -1;
}

struct read_format
{
    u64 nr; /* The number of event file descriptors */
    struct
    {
        u64 value; /* The value of the event (nr of samples) */
    } values[128]; /* TODO: remove hard-wired child limit */
};

static int profilerEventGroupStop(struct ProfilerEventGroup *g)
{
    struct sample *sample;
    struct read_format v;
    unsigned long long n;
    unsigned i;
    int rv;

    rv = ioctl(g->events[0], PERF_EVENT_IOC_DISABLE, 0);
    if (rv != 0) {
        perror("ioctl disable perf");
        return -1;
    }

    rv = (int)read(g->events[0], &v, sizeof v);
    assert(rv >= (int)(sizeof n));

    if (v.nr != g->n_events) {
        printf("mismatch between reported and registered event fds");
        return -1;
    }

    n = 0;
    for (i = 0; i < v.nr; i++) {
        n += v.values[i].value;
    }
    if (n == 0) {
        return 0;
    }

    sample = (void *)((char *)g->data + PAGE_SIZE);

    for (i = 0; i < n; i++) {
        u32 size;
        rv = processPerfSample(g, sample, &size);
        if (rv != 0) {
            return -1;
        }
        sample = (void *)((char *)sample + size);
    }

    return 0;
}

int ProfilerStop(struct Profiler *p)
{
    unsigned i;
    int rv;

    rv = contextSwitchCounterStop(&p->switches);
    if (rv != 0) {
        return rv;
    }

    for (i = 0; i < p->n_traces; i++) {
        rv = profilerTraceFsStop(p->traces[i]);
        if (rv != 0) {
            return rv;
        }
    }

    if (p->device->driver != FS_DRIVER_GENERIC) {
        for (i = 0; i < p->n_groups; i++) {
            rv = profilerEventGroupStop(&p->groups[i]);
            if (rv != 0) {
                return -1;
            }
        }
    }

    return 0;
}
