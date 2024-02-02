#include <string.h>
#include <unistd.h>

#include "assert.h"
#include "heap.h"
#include "uv.h"
#include "uv_os.h"

#define tracef(...) Tracef(uv->tracer, __VA_ARGS__)
#define trace(TYPE, INFO) Trace(uv->tracer, TYPE, INFO)

/* The happy path for UvPrepare is:
 *
 * - If there is an unused open segment available, return its fd and counter
 *   immediately.
 *
 * - Otherwise, wait for the creation of a new open segment to complete,
 *   possibly kicking off the creation logic if no segment is being created
 *   currently.
 *
 * Possible failure modes are:
 *
 * - The create file request fails, in that case we fail all pending prepare
 *   requests and we mark the uv instance as errored.
 *
 * On close:
 *
 * - Cancel all pending prepare requests.
 * - Remove unused prepared open segments.
 * - Wait for any pending internal segment creation and then discard the newly
 *   created segment.
 */

/* Number of open segments that we try to keep ready for writing. */
#define UV__TARGET_POOL_SIZE 2

/* An open segment being prepared or sitting in the pool */
struct uvIdleSegment
{
    struct uv *uv;                     /* Open segment file */
    size_t size;                       /* Segment size */
    struct uv_work_s work;             /* To execute logic in the threadpool */
    int status;                        /* Result of threadpool callback */
    char errmsg[RAFT_ERRMSG_BUF_SIZE]; /* Error of threadpool callback */
    uvCounter counter;                 /* Segment counter */
    char filename[UV__FILENAME_LEN];   /* Filename of the segment */
    uv_file fd;                        /* File descriptor of prepared file */
    queue queue;                       /* Pool */
};

static int uvPrepareCreateSegment(struct uvIdleSegment *segment)
{
    struct uv *uv = segment->uv;
    int rv;

    rv = UvFsAllocateFile(uv->dir, segment->filename, segment->size,
                          &segment->fd, segment->errmsg);
    if (rv != 0) {
        goto err;
    }

    rv = UvFsSyncDir(uv->dir, segment->errmsg);
    if (rv != 0) {
        goto err_after_allocate;
    }

    return 0;

err_after_allocate:
    UvOsClose(segment->fd);
err:
    assert(rv != 0);

    return rv;
}

static void uvPrepareWorkCb(uv_work_t *work)
{
    struct uvIdleSegment *segment = work->data;
    segment->status = uvPrepareCreateSegment(segment);
}

/* Flush all pending requests, invoking their callbacks with the given
 * status. */
static void uvPrepareFinishAllRequests(struct uv *uv, int status)
{
    while (!QUEUE_IS_EMPTY(&uv->prepare_reqs)) {
        queue *head;
        struct uvPrepare *req;
        head = QUEUE_HEAD(&uv->prepare_reqs);
        req = QUEUE_DATA(head, struct uvPrepare, queue);
        QUEUE_REMOVE(&req->queue);
        req->cb(req, status);
    }
}

/* Pop the oldest prepared segment in the pool and return its fd and counter
 * through the given pointers. */
static void uvPrepareConsume(struct uv *uv, uv_file *fd, uvCounter *counter)
{
    queue *head;
    struct uvIdleSegment *segment;
    /* Pop a segment from the pool. */
    head = QUEUE_HEAD(&uv->prepare_pool);
    segment = QUEUE_DATA(head, struct uvIdleSegment, queue);
    assert(segment->fd >= 0);
    QUEUE_REMOVE(&segment->queue);
    *fd = segment->fd;
    *counter = segment->counter;
    RaftHeapFree(segment);
}

/* Finish the oldest pending prepare request using the next available prepared
 * segment. */
static void uvPrepareFinishOldestRequest(struct uv *uv)
{
    queue *head;
    struct uvPrepare *req;

    assert(!uv->closing);
    assert(!QUEUE_IS_EMPTY(&uv->prepare_reqs));
    assert(!QUEUE_IS_EMPTY(&uv->prepare_pool));

    /* Pop the head of the prepare requests queue. */
    head = QUEUE_HEAD(&uv->prepare_reqs);
    req = QUEUE_DATA(head, struct uvPrepare, queue);
    QUEUE_REMOVE(&req->queue);

    /* Finish the request */
    uvPrepareConsume(uv, &req->fd, &req->counter);
    req->cb(req, 0);
}

unsigned UvPrepareCount(struct uv *uv)
{
    queue *head;
    unsigned n;
    n = 0;
    QUEUE_FOREACH (head, &uv->prepare_pool) {
        n++;
    }
    return n;
}

static void uvPrepareAfterWorkCb(uv_work_t *work, int status);

static struct uvIdleSegment *uvIdleSegmentCreate(struct uv *uv)
{
    struct uvIdleSegment *s;

    s = RaftHeapMalloc(sizeof *s);
    if (s == NULL) {
        return NULL;
    }

    memset(s, 0, sizeof *s);
    s->uv = uv;
    s->counter = uv->prepare_next_counter;
    s->work.data = s;
    s->fd = -1;
    s->size = uv->block_size * uvSegmentBlocks(uv);
    sprintf(s->filename, UV__OPEN_TEMPLATE, s->counter);

    return s;
}

/* Start creating a new segment file. */
static int uvPrepareStart(struct uv *uv)
{
    struct uvIdleSegment *segment;
    int rv;

    assert(uv->prepare_inflight == NULL);
    assert(UvPrepareCount(uv) < UV__TARGET_POOL_SIZE);

    segment = uvIdleSegmentCreate(uv);
    if (segment == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    tracef("create open segment %s", segment->filename);
    rv = uv_queue_work(uv->loop, &segment->work, uvPrepareWorkCb,
                       uvPrepareAfterWorkCb);
    if (rv != 0) {
        /* UNTESTED: with the current libuv implementation this can't fail. */
        tracef("can't create segment %s: %s", segment->filename,
               uv_strerror(rv));
        rv = RAFT_IOERR;
        goto err_after_segment_alloc;
    }

    uv->prepare_inflight = segment;
    uv->prepare_next_counter++;

    return 0;

err_after_segment_alloc:
    RaftHeapFree(segment);
err:
    assert(rv != 0);
    return rv;
}

/* Retry segment creation. */
static void uvPrepareRetryTimerCb(uv_timer_t *timer)
{
    struct uvIdleSegment *segment = timer->data;
    struct uv *uv = segment->uv;
    int rv;

    assert(uv->prepare_inflight == segment);

    uv->prepare_retry.data = uv;
    tracef("retry creating segment %s", segment->filename);
    rv = uv_queue_work(uv->loop, &segment->work, uvPrepareWorkCb,
                       uvPrepareAfterWorkCb);
    assert(rv == 0);
}

static void uvPrepareAfterWorkCb(uv_work_t *work, int status)
{
    struct uvIdleSegment *segment = work->data;
    struct uv *uv = segment->uv;
    int rv;
    assert(status == 0);

    /* If we are closing, let's discard the segment. All pending requests have
     * already being fired with RAFT_CANCELED. */
    if (uv->closing) {
        uv->prepare_inflight = NULL;
        assert(QUEUE_IS_EMPTY(&uv->prepare_pool));
        assert(QUEUE_IS_EMPTY(&uv->prepare_reqs));
        if (segment->status == 0) {
            char errmsg[RAFT_ERRMSG_BUF_SIZE];
            UvOsClose(segment->fd);
            UvFsRemoveFile(uv->dir, segment->filename, errmsg);
        }
        tracef("canceled creation of %s", segment->filename);
        RaftHeapFree(segment);
        uvMaybeFireCloseCb(uv);
        return;
    }

    /* If the request has failed, retry again after a while. */
    if (segment->status != 0) {
        assert(uv->prepare_retry.data == uv);
        uv->prepare_retry.data = segment;
        rv = uv_timer_start(&uv->prepare_retry, uvPrepareRetryTimerCb,
                            uv->disk_retry, 0);
        assert(rv == 0);
        return;
    }

    uv->prepare_inflight = NULL; /* Reset the creation in-progress marker. */

    assert(segment->fd >= 0);

    tracef("completed creation of %s", segment->filename);
    QUEUE_PUSH(&uv->prepare_pool, &segment->queue);

    /* Let's process any pending request. */
    if (!QUEUE_IS_EMPTY(&uv->prepare_reqs)) {
        uvPrepareFinishOldestRequest(uv);
    }

    /* If we are already creating a segment, we're done. */
    if (uv->prepare_inflight != NULL) {
        return;
    }

    /* If we have already enough prepared open segments, we're done. There can't
     * be any outstanding prepare requests, since if the request queue was not
     * empty, we would have called uvPrepareFinishOldestRequest() above, thus
     * reducing the pool size and making it smaller than the target size. */
    if (UvPrepareCount(uv) >= UV__TARGET_POOL_SIZE) {
        assert(QUEUE_IS_EMPTY(&uv->prepare_reqs));
        return;
    }

    /* Let's start preparing a new open segment. */
    rv = uvPrepareStart(uv);
    if (rv != 0) {
        uvPrepareFinishAllRequests(uv, rv);
        uv->errored = true;
    }
}

/* Discard a prepared open segment, closing its file descriptor and removing the
 * underlying file. */
static void uvPrepareDiscard(struct uv *uv, uv_file fd, uvCounter counter)
{
    char errmsg[RAFT_ERRMSG_BUF_SIZE];
    char filename[UV__FILENAME_LEN];
    assert(counter > 0);
    assert(fd >= 0);
    sprintf(filename, UV__OPEN_TEMPLATE, counter);
    UvOsClose(fd);
    UvFsRemoveFile(uv->dir, filename, errmsg);
}

int UvPrepare(struct uv *uv,
              uv_file *fd,
              uvCounter *counter,
              struct uvPrepare *req,
              uvPrepareCb cb)
{
    int rv;

    assert(!uv->closing);

    if (!QUEUE_IS_EMPTY(&uv->prepare_pool)) {
        uvPrepareConsume(uv, fd, counter);
        goto maybe_start;
    }

    *fd = -1;
    *counter = 0;
    req->cb = cb;
    QUEUE_PUSH(&uv->prepare_reqs, &req->queue);

maybe_start:
    /* If we are already creating a segment, let's just wait. */
    if (uv->prepare_inflight != NULL) {
        return 0;
    }

    rv = uvPrepareStart(uv);
    if (rv != 0) {
        goto err;
    }

    return 0;

err:
    if (*fd != -1) {
        uvPrepareDiscard(uv, *fd, *counter);
    } else {
        QUEUE_REMOVE(&req->queue);
    }
    assert(rv != 0);
    return rv;
}

void UvPrepareStart(struct uv *uv)
{
    unsigned i;

    for (i = 0; i < UV__TARGET_POOL_SIZE; i++) {
        struct uvIdleSegment *segment = uvIdleSegmentCreate(uv);
        int rv;

        if (segment == NULL) {
            break;
        }

        rv = uvPrepareCreateSegment(segment);
        if (rv != 0) {
            RaftHeapFree(segment);
            break;
        }

        uv->io->capacity += (unsigned short)(segment->size / 1024);

        uv->prepare_next_counter++;
        QUEUE_PUSH(&uv->prepare_pool, &segment->queue);
    }
}

void UvPrepareClose(struct uv *uv)
{
    assert(uv->closing);

    /* Cancel all pending prepare requests. */
    uvPrepareFinishAllRequests(uv, RAFT_CANCELED);

    /* Remove any unused prepared segment. */
    while (!QUEUE_IS_EMPTY(&uv->prepare_pool)) {
        queue *head;
        struct uvIdleSegment *segment;
        head = QUEUE_HEAD(&uv->prepare_pool);
        segment = QUEUE_DATA(head, struct uvIdleSegment, queue);
        QUEUE_REMOVE(&segment->queue);
        uvPrepareDiscard(uv, segment->fd, segment->counter);
        RaftHeapFree(segment);
    }
}

#undef tracef
#undef trace
