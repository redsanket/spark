#include<stdio.h>
#include<stdlib.h>
#include<time.h>

int main() {
  int mb_factor = 10;
  size_t alloc_size = 1024 * 1024 * mb_factor;
  size_t no_objects = 1;
  int alloc_num = 0;
  void *buf_ptr;
	sleep(100);
  while (1) {
    alloc_num++;
    buf_ptr = calloc(no_objects, alloc_size);

   if (buf_ptr == NULL) {
     printf ("Allocation failed after %i megabytes\n", alloc_num * mb_factor);
    exit(0);
   }

  printf ("%i\n", alloc_num);
  sleep (1);
  }
}
