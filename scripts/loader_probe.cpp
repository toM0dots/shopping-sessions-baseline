// Diagnostic control: compile once without WITH_ARROW and once with the
// PyArrow shared libraries and -DWITH_ARROW to isolate library launch cost.
#include <cstdio>
#include <ctime>
#ifdef WITH_ARROW
#include <arrow/config.h>
#endif
static unsigned long long now() {
    timespec ts{}; clock_gettime(CLOCK_MONOTONIC,&ts);
    return ts.tv_sec*1000000000ULL+ts.tv_nsec;
}
int main() {
    fprintf(stderr,"lifecycle_main_enter_ns=%llu\n",now());
#ifdef WITH_ARROW
    (void)arrow::GetBuildInfo();
#endif
    fprintf(stderr,"{\"elapsed_seconds\":0}\n");
    fprintf(stderr,"lifecycle_main_exit_ns=%llu\n",now());
}
