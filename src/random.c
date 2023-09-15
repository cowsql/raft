#include <stdint.h>

#include "assert.h"
#include "random.h"

#define RANDOM_MULTIPLIER (747796405U)
#define RANDOM_INCREMENT (1729U)

static uint32_t randomAdvance(unsigned *random)
{
    uint32_t state;
    uint32_t n;
    state = *random;
    n = ((state >> ((state >> 28) + 4)) ^ state) * (277803737U);
    n ^= n >> 22;
    *random = state * RANDOM_MULTIPLIER + RANDOM_INCREMENT;
    return n;
}

/* Generate a random non-negative number with at most the given value. */
static uint32_t randomAtMost(unsigned *random, uint32_t max)
{
    /* We want (UINT32_MAX + 1) % max, which in unsigned arithmetic is the same
     * as (UINT32_MAX + 1 - max) % max = -max % max. We compute -max using not
     * to avoid compiler warnings.
     */
    const uint32_t min = (~max + 1U) % max;
    uint32_t x;

    if (max == (~((uint32_t)0U))) {
        return randomAdvance(random);
    }

    max++;

    do {
        x = randomAdvance(random);
    } while (x < min);

    return x % max;
}

unsigned RandomWithinRange(unsigned *state, unsigned min, unsigned max)
{
    uint64_t range = (uint64_t)max - (uint64_t)min;

    assert(min <= max);

    if (range > (~((uint32_t)0U))) {
        range = (~((uint32_t)0U));
    }

    return min + (unsigned)randomAtMost(state, (uint32_t)range);
}
