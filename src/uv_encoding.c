#include "uv_encoding.h"

#include <limits.h>
#include <string.h>

#include "../include/raft/uv.h"
#include "assert.h"
#include "byte.h"
#include "configuration.h"

/**
 * Size of the request preamble.
 */
#define RAFT_IO_UV__PREAMBLE_SIZE           \
    (sizeof(uint64_t) /* Message type. */ + \
     sizeof(uint64_t) /* Message size. */)

static size_t sizeofRequestVoteV1(void)
{
    return sizeof(uint64_t) + /* Term. */
           sizeof(uint64_t) + /* Candidate ID. */
           sizeof(uint64_t) + /* Last log index. */
           sizeof(uint64_t) /* Last log term. */;
}

static size_t sizeofRequestVote(void)
{
    return sizeofRequestVoteV1() + sizeof(uint64_t) /* Flags. */;
}

static size_t sizeofRequestVoteResultV1(void)
{
    return sizeof(uint64_t) + /* Term. */
           sizeof(uint64_t) /* Vote granted. */;
}

static size_t sizeofRequestVoteResult(void)
{
    return sizeofRequestVoteResultV1() + /* Size of older version 1 message */
           sizeof(uint8_t) +             /* Flags */
           sizeof(uint8_t) +             /* Unused */
           sizeof(uint16_t) +            /* Features */
           sizeof(uint16_t) +            /* Capacity */
           sizeof(uint16_t);             /* Unused */
}

static size_t sizeofAppendEntries(const struct raft_append_entries *p)
{
    return sizeof(uint64_t) +                  /* Leader's term. */
           sizeof(uint64_t) +                  /* Previous log entry index */
           sizeof(uint64_t) +                  /* Previous log entry term */
           sizeof(uint64_t) +                  /* Leader's commit index */
           uvSizeofBatchHeader(p->n_entries) + /* Batch header */
           sizeof(uint64_t);                   /* XXX: currently unused */
}

static size_t sizeofAppendEntriesResultV0(void)
{
    return sizeof(uint64_t) + /* Term. */
           sizeof(uint64_t) + /* Success. */
           sizeof(uint64_t);  /* Last log index. */
}

static size_t sizeofAppendEntriesResult(void)
{
    return sizeofAppendEntriesResultV0() + /* Size of older version 0 message */
           sizeof(uint16_t) +              /* Server features. */
           sizeof(uint16_t) +              /* Capacity. */
           sizeof(uint32_t);               /* Unused */
}

static size_t sizeofInstallSnapshot(const struct raft_install_snapshot *p)
{
    size_t conf_size = configurationEncodedSize(&p->conf);
    return sizeof(uint64_t) + /* Leader's term. */
           sizeof(uint64_t) + /* Snapshot's last index */
           sizeof(uint64_t) + /* Term of last index */
           sizeof(uint64_t) + /* Configuration's index */
           sizeof(uint64_t) + /* Length of configuration */
           conf_size +        /* Configuration data */
           sizeof(uint64_t) + /* Length of snapshot data */
           sizeof(uint64_t);  /* XXX: currently unused */
}

static size_t sizeofTimeoutNow(void)
{
    return sizeof(uint64_t) + /* Term. */
           sizeof(uint64_t) + /* Last log index. */
           sizeof(uint64_t) /* Last log term. */;
}

size_t uvSizeofBatchHeader(size_t n)
{
    return 8 + /* Number of entries in the batch, little endian */
           16 * n /* One header per entry */;
}

static void encodeRequestVote(const struct raft_request_vote *p, void *buf)
{
    uint8_t *cursor = buf;
    uint64_t flags = 0;

    if (p->disrupt_leader) {
        flags |= 1 << 0;
    }
    if (p->pre_vote) {
        flags |= 1 << 1;
    }

    bytePut64(&cursor, p->term);
    bytePut64(&cursor, p->candidate_id);
    bytePut64(&cursor, p->last_log_index);
    bytePut64(&cursor, p->last_log_term);
    bytePut64(&cursor, flags);
}

static void encodeRequestVoteResult(const struct raft_request_vote_result *p,
                                    void *buf)
{
    uint8_t *cursor = buf;
    uint8_t flags = 0;

    if (p->pre_vote) {
        flags |= (1 << 0);
    }

    bytePut64(&cursor, p->term);
    bytePut64(&cursor, p->vote_granted);

    bytePut8(&cursor, flags);
    bytePut8(&cursor, 0);
    bytePut16(&cursor, p->features);

    bytePut16(&cursor, p->capacity);
    bytePut16(&cursor, 0);
}

static void encodeAppendEntries(const struct raft_append_entries *p, void *buf)
{
    uint8_t *cursor;

    cursor = buf;

    bytePut64(&cursor, p->term);           /* Leader's term. */
    bytePut64(&cursor, p->prev_log_index); /* Previous log entry index. */
    bytePut64(&cursor, p->prev_log_term);  /* Previous log entry term. */
    bytePut64(&cursor, p->leader_commit);  /* Leader's commit index. */

    uvEncodeBatchHeader(p->entries, p->n_entries, cursor); /* Batch header */

    cursor = (uint8_t *)cursor + uvSizeofBatchHeader(p->n_entries);
    bytePut64(&cursor, 0); /* XXX: currently unused */
}

static void encodeAppendEntriesResult(
    const struct raft_append_entries_result *p,
    void *buf)
{
    uint8_t *cursor = buf;

    bytePut64(&cursor, p->term);
    bytePut64(&cursor, p->rejected);
    bytePut64(&cursor, p->last_log_index);
    bytePut16(&cursor, p->features);
    bytePut16(&cursor, p->capacity);
    bytePut32(&cursor, 0 /* Unused */);
}

static void encodeInstallSnapshot(const struct raft_install_snapshot *p,
                                  void *buf)
{
    uint8_t *cursor;
    size_t conf_size = configurationEncodedSize(&p->conf);

    cursor = buf;

    bytePut64(&cursor, p->term);       /* Leader's term */
    bytePut64(&cursor, p->last_index); /* Snapshot's last index */
    bytePut64(&cursor, p->last_term);  /* Term of last index */
    bytePut64(&cursor, p->conf_index); /* Configuration's index */
    bytePut64(&cursor, conf_size);     /* Length of configuration */

    configurationEncodeToBuf(&p->conf, cursor,
                             conf_size); /* Configuration data */
    cursor = (uint8_t *)cursor + conf_size;

    bytePut64(&cursor, p->data.len); /* Length of snapshot data */

    bytePut64(&cursor, 0); /* XXX: currently unused */
}

static void encodeTimeoutNow(const struct raft_timeout_now *p, void *buf)
{
    uint8_t *cursor = buf;

    bytePut64(&cursor, p->term);
    bytePut64(&cursor, p->last_log_index);
    bytePut64(&cursor, p->last_log_term);
}

int uvEncodeMessage(const struct raft_message *message,
                    uv_buf_t **bufs,
                    unsigned *n_bufs)
{
    uv_buf_t header;
    uint8_t *cursor;
    int version;

    /* Figure out the length of the header for this request and allocate a
     * buffer for it. */
    header.len = RAFT_IO_UV__PREAMBLE_SIZE;
    switch (message->type) {
        case RAFT_REQUEST_VOTE:
            header.len += sizeofRequestVote();
            version = message->request_vote.version;
            break;
        case RAFT_REQUEST_VOTE_RESULT:
            header.len += sizeofRequestVoteResult();
            version = message->request_vote_result.version;
            break;
        case RAFT_APPEND_ENTRIES:
            header.len += sizeofAppendEntries(&message->append_entries);
            version = message->append_entries.version;
            break;
        case RAFT_APPEND_ENTRIES_RESULT:
            header.len += sizeofAppendEntriesResult();
            version = message->append_entries_result.version;
            break;
        case RAFT_INSTALL_SNAPSHOT:
            header.len += sizeofInstallSnapshot(&message->install_snapshot);
            version = message->install_snapshot.version;
            break;
        case RAFT_TIMEOUT_NOW:
            header.len += sizeofTimeoutNow();
            version = message->timeout_now.version;
            break;
        default:
            return RAFT_MALFORMED;
    };

    header.base = raft_malloc(header.len);
    if (header.base == NULL) {
        goto oom;
    }

    cursor = (uint8_t *)header.base;

    /* Encode the request preamble, with message type, version and size. */
    bytePut8(&cursor, (uint8_t)message->type);
    bytePut8(&cursor, 0);
    bytePut8(&cursor, (uint8_t)version);
    bytePut8(&cursor, 0);
    bytePut32(&cursor, 0);

    bytePut64(&cursor, header.len - RAFT_IO_UV__PREAMBLE_SIZE);

    /* Encode the request header. */
    switch (message->type) {
        case RAFT_REQUEST_VOTE:
            encodeRequestVote(&message->request_vote, cursor);
            break;
        case RAFT_REQUEST_VOTE_RESULT:
            encodeRequestVoteResult(&message->request_vote_result, cursor);
            break;
        case RAFT_APPEND_ENTRIES:
            encodeAppendEntries(&message->append_entries, cursor);
            break;
        case RAFT_APPEND_ENTRIES_RESULT:
            encodeAppendEntriesResult(&message->append_entries_result, cursor);
            break;
        case RAFT_INSTALL_SNAPSHOT:
            encodeInstallSnapshot(&message->install_snapshot, cursor);
            break;
        case RAFT_TIMEOUT_NOW:
            encodeTimeoutNow(&message->timeout_now, cursor);
            break;
    };

    *n_bufs = 1;

    /* For AppendEntries request we also send the entries payload. */
    if (message->type == RAFT_APPEND_ENTRIES) {
        *n_bufs += message->append_entries.n_entries;
    }

    /* For InstallSnapshot request we also send the snapshot payload. */
    if (message->type == RAFT_INSTALL_SNAPSHOT) {
        *n_bufs += 1;
    }

    *bufs = raft_calloc(*n_bufs, sizeof **bufs);
    if (*bufs == NULL) {
        goto oom_after_header_alloc;
    }

    (*bufs)[0] = header;

    if (message->type == RAFT_APPEND_ENTRIES) {
        unsigned i;
        for (i = 0; i < message->append_entries.n_entries; i++) {
            const struct raft_entry *entry =
                &message->append_entries.entries[i];
            (*bufs)[i + 1].base = entry->buf.base;
            (*bufs)[i + 1].len = entry->buf.len;
        }
    }

    if (message->type == RAFT_INSTALL_SNAPSHOT) {
        (*bufs)[1].base = message->install_snapshot.data.base;
        (*bufs)[1].len = message->install_snapshot.data.len;
    }

    return 0;

oom_after_header_alloc:
    raft_free(header.base);

oom:
    return RAFT_NOMEM;
}

void uvEncodeBatchHeader(const struct raft_entry *entries,
                         unsigned n,
                         void *buf)
{
    unsigned i;
    uint8_t *cursor = buf;

    /* Number of entries in the batch, little endian */
    bytePut64(&cursor, n);

    for (i = 0; i < n; i++) {
        const struct raft_entry *entry = &entries[i];

        /* Term in which the entry was created, little endian. */
        bytePut64(&cursor, entry->term);

        /* Message type (Either RAFT_COMMAND or RAFT_CHANGE) */
        bytePut8(&cursor, (uint8_t)entry->type);

        /* Unused */
        bytePut8(&cursor, 0);
        bytePut8(&cursor, 0);
        bytePut8(&cursor, 0);

        /* Size of the log entry data, little endian. */
        bytePut32(&cursor, (uint32_t)entry->buf.len);
    }
}

static void decodeRequestVote(unsigned char version,
                              const uv_buf_t *buf,
                              struct raft_request_vote *p)
{
    const uint8_t *cursor;

    cursor = (void *)buf->base;

    /* If the version is 0 the message was sent by a server that
     * does not encodes the version byte in the preamble. */
    if (version == 0) {
        if (buf->len == sizeofRequestVoteV1()) {
            version = 1;
        } else {
            version = 2;
        }
    }

    p->version = version;
    p->term = byteGet64(&cursor);
    p->candidate_id = byteGet64(&cursor);
    p->last_log_index = byteGet64(&cursor);
    p->last_log_term = byteGet64(&cursor);
    p->disrupt_leader = false;
    p->pre_vote = false;

    if (p->version >= 1) {
        uint64_t flags = byteGet64(&cursor);
        p->disrupt_leader = (bool)(flags & 1 << 0);
        p->pre_vote = (bool)(flags & 1 << 1);
    }
}

static void decodeRequestVoteResult(unsigned char version,
                                    const uv_buf_t *buf,
                                    struct raft_request_vote_result *p)
{
    const uint8_t *cursor;

    cursor = (void *)buf->base;

    /* If the version is 0 the message was sent by a server that
     * does not encodes the version byte in the preamble. */
    if (version == 0) {
        if (buf->len == sizeofRequestVoteResultV1()) {
            version = 1;
        } else {
            version = 2;
        }
    }

    p->version = version;
    p->term = byteGet64(&cursor);
    p->vote_granted = byteGet64(&cursor);
    p->features = 0;
    p->capacity = 0;

    if (p->version >= 2) {
        uint8_t flags = byteGet8(&cursor);
        uint8_t unused = byteGet8(&cursor);
        uint16_t features = byteGet16(&cursor);
        uint16_t capacity = byteGet16(&cursor);
        p->version = 2;
        p->pre_vote = (flags & (1 << 0));
        (void)unused;
        p->features = features;
        p->capacity = capacity;
    }
}

int uvDecodeBatchHeader(const void *batch,
                        struct raft_entry **entries,
                        unsigned *n)
{
    const uint8_t *cursor = batch;
    size_t i;
    int rv;

    *n = (unsigned)byteGet64(&cursor);

    if (*n == 0) {
        *entries = NULL;
        return 0;
    }

    *entries = raft_malloc(*n * sizeof **entries);

    if (*entries == NULL) {
        rv = RAFT_NOMEM;
        goto err;
    }

    for (i = 0; i < *n; i++) {
        struct raft_entry *entry = &(*entries)[i];

        entry->term = byteGet64(&cursor);
        entry->type = byteGet8(&cursor);

        if (entry->type != RAFT_COMMAND && entry->type != RAFT_BARRIER &&
            entry->type != RAFT_CHANGE) {
            rv = RAFT_MALFORMED;
            goto err_after_alloc;
        }

        cursor = (uint8_t *)cursor + 3; /* Unused */

        /* Size of the log entry data, little endian. */
        entry->buf.len = byteGet32(&cursor);
    }

    return 0;

err_after_alloc:
    raft_free(*entries);
    *entries = NULL;

err:
    assert(rv != 0);

    return rv;
}

static int decodeAppendEntries(unsigned char version,
                               const uv_buf_t *buf,
                               struct raft_append_entries *args)
{
    const uint8_t *cursor;
    int rv;

    assert(buf != NULL);
    assert(args != NULL);

    cursor = (void *)buf->base;

    args->version = version;
    args->term = byteGet64(&cursor);
    args->prev_log_index = byteGet64(&cursor);
    args->prev_log_term = byteGet64(&cursor);
    args->leader_commit = byteGet64(&cursor);

    rv = uvDecodeBatchHeader(cursor, &args->entries, &args->n_entries);
    if (rv != 0) {
        return rv;
    }

    return 0;
}

static void decodeAppendEntriesResult(unsigned char version,
                                      const uv_buf_t *buf,
                                      struct raft_append_entries_result *p)
{
    const uint8_t *cursor;

    cursor = (void *)buf->base;

    /* If the version is 0 the message was sent by a server that
     * does not encodes the version byte in the preamble. */
    if (version == 0) {
        if (buf->len > sizeofAppendEntriesResultV0()) {
            version = 2;
        }
    }

    p->version = version;
    p->term = byteGet64(&cursor);
    p->rejected = byteGet64(&cursor);
    p->last_log_index = byteGet64(&cursor);
    p->features = 0;
    p->capacity = 0;
    if (p->version >= 1) {
        p->features = byteGet16(&cursor);
    }
    if (p->version >= 2) {
        p->capacity = byteGet16(&cursor);
    }
}

static int decodeInstallSnapshot(unsigned char version,
                                 const uv_buf_t *buf,
                                 struct raft_install_snapshot *args)
{
    const uint8_t *cursor;
    struct raft_buffer conf;
    int rv;

    assert(buf != NULL);
    assert(args != NULL);

    cursor = (void *)buf->base;

    args->version = version;
    args->term = byteGet64(&cursor);
    args->last_index = byteGet64(&cursor);
    args->last_term = byteGet64(&cursor);
    args->conf_index = byteGet64(&cursor);
    conf.len = (size_t)byteGet64(&cursor);
    conf.base = (void *)cursor;

    rv = configurationDecode(&conf, &args->conf);
    if (rv != 0) {
        return rv;
    }
    cursor = (uint8_t *)cursor + conf.len;
    args->data.len = (size_t)byteGet64(&cursor);

    return 0;
}

static void decodeTimeoutNow(const uv_buf_t *buf, struct raft_timeout_now *p)
{
    const uint8_t *cursor;

    cursor = (void *)buf->base;

    p->version = 0;
    p->term = byteGet64(&cursor);
    p->last_log_index = byteGet64(&cursor);
    p->last_log_term = byteGet64(&cursor);
}

int uvDecodeMessage(uint8_t type,
                    uint8_t version,
                    const uv_buf_t *header,
                    struct raft_message *message,
                    size_t *payload_len)
{
    unsigned i;
    int rv = 0;

    memset(message, 0, sizeof(*message));
    message->type = (unsigned short)type;

    *payload_len = 0;

    /* Decode the header. */
    switch (type) {
        case RAFT_REQUEST_VOTE:
            decodeRequestVote(version, header, &message->request_vote);
            break;
        case RAFT_REQUEST_VOTE_RESULT:
            decodeRequestVoteResult(version, header,
                                    &message->request_vote_result);
            break;
        case RAFT_APPEND_ENTRIES:
            rv = decodeAppendEntries(version, header, &message->append_entries);
            for (i = 0; i < message->append_entries.n_entries; i++) {
                *payload_len += message->append_entries.entries[i].buf.len;
            }
            break;
        case RAFT_APPEND_ENTRIES_RESULT:
            decodeAppendEntriesResult(version, header,
                                      &message->append_entries_result);
            break;
        case RAFT_INSTALL_SNAPSHOT:
            rv = decodeInstallSnapshot(version, header,
                                       &message->install_snapshot);
            *payload_len += message->install_snapshot.data.len;
            break;
        case RAFT_TIMEOUT_NOW:
            decodeTimeoutNow(header, &message->timeout_now);
            break;
        default:
            rv = RAFT_IOERR;
            break;
    };

    return rv;
}

void uvDecodeEntriesBatch(uint8_t *batch,
                          size_t offset,
                          struct raft_entry *entries,
                          unsigned n)
{
    uint8_t *cursor;
    size_t i;

    assert(batch != NULL);

    cursor = batch + offset;

    for (i = 0; i < n; i++) {
        struct raft_entry *entry = &entries[i];
        entry->batch = batch;

        if (entry->buf.len == 0) {
            entry->buf.base = NULL;
            continue;
        }

        entry->buf.base = cursor;

        cursor = cursor + entry->buf.len;
        if (entry->buf.len % 8 != 0) {
            /* Add padding */
            cursor = cursor + 8 - (entry->buf.len % 8);
        }
    }
}
