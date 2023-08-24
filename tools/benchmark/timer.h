/* Utilities for timing operations. */

#ifndef TIMER_H_
#define TIMER_H_

#include <time.h>

struct timer
{
    struct timespec start;
};

/* Start the current timer. */
void TimerStart(struct timer *timer);

/* Calculate how much time has elapsed since the timer started, in
 * nanosecods. */
unsigned long TimerStop(struct timer *timer);

#endif /* TIMER_H_ */
