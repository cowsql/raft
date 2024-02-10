Quick start
===========

Make sure that `autotools`_, `libtool`_, `pkg-config`_ and `libuv`_ are
installed on your system.

On a Debian (or derivative) systems you can do that with:

.. code-block:: bash

   sudo apt-get install build-essential libtool pkg-config libuv1-dev

Then run:

.. code-block:: bash

   autoreconf -i
   ./configure
   make
   sudo make install

Then create a :file:`main.c` file with this simple test program that just runs a
single raft server and implements a basic state machine for incrementing a
counter:

.. code-block:: C

   #include <raft.h>
   #include <raft/uv.h>

   static raft_id id = 12345;
   static const char *address = "127.0.0.1:8080";
   static const char *dir = "/tmp/raft-quick-start";
   static struct raft_configuration conf;
   static struct uv_loop_s loop;
   static struct uv_raft_s raft;
   static struct uv_timer_s timer;
   static unsigned counter = 0;
   static uint64_t command;

   static void timerCb(uv_timer_t *timer) {
       struct raft_buffer buf;
       command = uv_now(timer->loop) % 10;
       buf.len = sizeof command;
       buf.base = &command;
       uv_raft_submit(&raft, RAFT_COMMAND, &buf);
   }

   static void commitCb(struct uv_raft_s *raft, int type, const struct uv_buf_s *buf) {
       counter += *(uint64_t *)buf->base;
       printf("counter: %u\n", counter);
       return 0;
   }

   int main() {
       mkdir(dir, 0755);
       raft_configuration_init(&conf);
       raft_configuration_add(&conf, id, address, RAFT_VOTER);
       uv_loop_init(&loop);
       uv_raft_init(&loop, &raft, id, address, dir);
       uv_timer_init(&loop, &timer);
       uv_raft_bootstrap(&raft, &conf);
       uv_raft_start(&raft, commitCb, NULL, NULL);
       uv_timer_start(&timer, timerCb, 0, 1000);
       uv_run(&loop, UV_RUN_DEFAULT);
   }

You can compile and run it with:

.. code-block:: bash

   cc main.c -o main -lraft -luv && ./main

.. _autotools: https://en.wikipedia.org/wiki/GNU_Autotools
.. _libtool: https://www.gnu.org/software/libtool/
.. _pkg-config: https://www.freedesktop.org/wiki/Software/pkg-config/
.. _libuv: http://libuv.org

