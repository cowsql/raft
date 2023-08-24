#include <limits.h>

#include "assert.h"
#include "timer.h"

/* Save current time in 'time'. */
static void timerNow(struct timespec *time)
{
    int rv;
    rv = clock_gettime(CLOCK_MONOTONIC, time);
    assert(rv == 0);
}

void TimerStart(struct timer *timer)
{
    timerNow(&timer->start);
}

unsigned long TimerStop(struct timer *timer)
{
    struct timespec now;
    time_t nsecs;
    timerNow(&now);
    if (timer->start.tv_sec == now.tv_sec) {
        nsecs = now.tv_nsec - timer->start.tv_nsec;
    } else {
        nsecs = (now.tv_sec - timer->start.tv_sec) * 1000 * 1000 * 1000 -
                timer->start.tv_nsec + now.tv_nsec;
    }

    return (unsigned long)nsecs;
}
