// Random bytes for SCRAM's client nonce (goose-postgres wants at least 18, from a secure source): getrandom(2).
#include <stdint.h>
#include <sys/random.h>

// Fills buf with n random bytes; returns 1, or 0 on failure.
int32_t secure_random(uint8_t *buf, int64_t n) {
  int64_t got = 0;
  while (got < n) {
    ssize_t r = getrandom(buf + got, (size_t)(n - got), 0);
    if (r <= 0) return 0;
    got += r;
  }
  return 1;
}

