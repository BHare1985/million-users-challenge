// ISA-L gzip for the compression middleware in server.goose: one stateless igzip call at level 0 (its fastest), nothing kept between requests.
#include <stdint.h>
#include <igzip_lib.h>
#include "goose.h"

#define MAX_BODY 131072                       // the most skein sends in one response
static __thread uint8_t out_buf[MAX_BODY + 4096];

// Appends the gzip stream of src to out and returns 1, or returns 0 (out untouched) if it did not fit.
int32_t isal_gzip(gs_rref out, const uint8_t *src, int64_t len) {
  if (len > MAX_BODY) return 0;
  struct isal_zstream s; isal_deflate_stateless_init(&s);
  s.next_in = (uint8_t *)src; s.avail_in = (uint32_t)len; s.next_out = out_buf; s.avail_out = sizeof out_buf;
  s.gzip_flag = IGZIP_GZIP; s.level = 0;
  if (isal_deflate_stateless(&s) != COMP_OK) return 0;
  gs_bld_append(out, out_buf, (int64_t)(sizeof out_buf - s.avail_out));
  return 1;
}
