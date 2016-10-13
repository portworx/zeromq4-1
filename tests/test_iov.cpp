/*
    Copyright (c) 2007-2015 Contributors as noted in the AUTHORS file

    This file is part of libzmq, the ZeroMQ core engine in C++.

    libzmq is free software; you can redistribute it and/or modify it under
    the terms of the GNU Lesser General Public License (LGPL) as published
    by the Free Software Foundation; either version 3 of the License, or
    (at your option) any later version.

    As a special exception, the Contributors give you permission to link
    this library with independent modules to produce an executable,
    regardless of the license terms of these independent modules, and to
    copy and distribute the resulting executable under terms of your choice,
    provided that you also meet, for each linked independent module, the
    terms and conditions of the license of that module. An independent
    module is a module which is not derived from or based on this library.
    If you modify this library, you must extend this exception to your
    version of the library.

    libzmq is distributed in the hope that it will be useful, but WITHOUT
    ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
    FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
    License for more details.

    You should have received a copy of the GNU Lesser General Public License
    along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include "testutil.hpp"

// XSI vector I/O
#if defined ZMQ_HAVE_UIO
#include <sys/uio.h>
#else
struct iovec {
    void *iov_base;
    size_t iov_len;
};
#endif

void do_check(void* sb, void* sc, unsigned int msgsz)
{
    setup_test_environment();
    int rc;
    int sum =0;
    for (int i = 0; i < 10; i++)
    {
        zmq_msg_t msg;
        zmq_msg_init_size(&msg, msgsz);
        void * data = zmq_msg_data(&msg);
        memcpy(data,&i, sizeof(int));
        rc = zmq_msg_send(&msg,sc,i==9 ? 0 :ZMQ_SNDMORE);
        assert (rc == (int)msgsz);
        zmq_msg_close(&msg);
        sum += i;
    }

    struct iovec ibuffer[32] ;
    memset(&ibuffer[0], 0, sizeof(ibuffer));

    size_t count = 10;
    rc = zmq_recviov(sb,&ibuffer[0],&count,0);
    assert (rc == 10);

    int rsum=0;
    for(;count;--count)
    {
        int v;
        memcpy(&v,ibuffer[count-1].iov_base,sizeof(int));
        rsum += v;
        assert(ibuffer[count-1].iov_len == msgsz);
        // free up the memory
        free(ibuffer[count-1].iov_base);
    }

    assert ( sum == rsum );

}

void free_fn(void *, void *)
{
}

int main (void)
{
    void *ctx = zmq_ctx_new ();
    assert (ctx);
    int rc;

    void *sb = zmq_socket (ctx, ZMQ_PULL);
    assert (sb);

    rc = zmq_bind (sb, "inproc://a");
    assert (rc == 0);

    msleep (SETTLE_TIME);
    void *sc = zmq_socket (ctx, ZMQ_PUSH);

    rc = zmq_connect (sc, "inproc://a");
    assert (rc == 0);


    // message bigger than vsm max
    do_check(sb,sc,100);

    // message smaller than vsm max
    do_check(sb,sc,10);

    char buf_1[100];
    char buf_2[100];

    zmq_msg_t req;
    struct iovec iov[2];
    iov[0].iov_len = 10;
    iov[0].iov_base = buf_1;
    iov[1].iov_len = 20;
    iov[1].iov_base = buf_2;
    memset(iov[0].iov_base, 'a', 100);
    memset(iov[1].iov_base, 'b', 100);
    rc = zmq_msg_init_iov(&req, iov, 2, free_fn, iov);
    assert(rc == 0);

    rc = zmq_msg_send(&req, sc, 0);
    assert(rc == 30);

    zmq_msg_t rsp;
    zmq_msg_init(&rsp);
    rc = zmq_msg_recv(&rsp, sb, 0);
    assert(rc == 30);

    assert(zmq_msg_size(&rsp) == 30);
    assert(zmq_msg_iovcnt(&rsp) == 2);
    assert(zmq_msg_iov(&rsp) == iov);

    void *srep = zmq_socket (ctx, ZMQ_REP);
    assert(srep);

    rc = zmq_bind(srep, "tcp://127.0.0.1:5001");
    assert(rc == 0);

    void *sreq = zmq_socket (ctx, ZMQ_REQ);
    assert(sreq);

    rc = zmq_connect(sreq, "tcp://127.0.0.1:5001");
    assert(rc == 0);

    char buf_c[200];
    memset(buf_c, 'c', 200);

    zmq_msg_t req_simple;
    zmq_msg_init_size(&req_simple, 200);
    memset(zmq_msg_data(&req_simple), 'c', 200);

    rc = zmq_msg_send(&req_simple, sreq, 0);
    assert(rc == 200);
    zmq_msg_close(&req_simple);

    zmq_msg_init(&req_simple);
    rc = zmq_msg_recv(&req_simple, srep, 0);
    assert(rc == 200);
    assert(!memcmp(zmq_msg_data(&req_simple), buf_c, 200));

    rc = zmq_msg_send(&req_simple, srep, 0);
    assert(rc == 200);
    zmq_msg_close(&req_simple);

    zmq_msg_init(&req_simple);
    rc = zmq_msg_recv(&req_simple, sreq, 0);
    assert(rc == 200);
    assert(!memcmp(zmq_msg_data(&req_simple), buf_c, 200));
    zmq_msg_close(&req_simple);

    rc = zmq_msg_init_iov(&req, iov, 2, free_fn, iov);
    assert(rc == 0);

    rc = zmq_msg_send(&req, sreq, 0);
    assert(rc == 30);

    zmq_msg_close(&rsp);
    zmq_msg_init(&rsp);
    rc = zmq_msg_recv(&rsp, srep, 0);
    assert(rc == 30);

    assert(!memcmp(zmq_msg_data(&rsp), iov[0].iov_base, iov[0].iov_len));
    assert(!memcmp((char *)zmq_msg_data(&rsp) + iov[0].iov_len, iov[1].iov_base, iov[1].iov_len));

    rc = zmq_msg_send(&rsp, srep, 0);
    assert(rc == 30);
    zmq_msg_close(&rsp);

    zmq_msg_close(&req);

    zmq_msg_init(&req);
    rc = zmq_msg_recv(&req, sreq, 0);
    assert(rc == 30);

    struct iovec *iov_1 = new iovec[100];
    for (int i = 0; i < 100; ++i) {
        iov_1[i].iov_base = new char[100];
        iov_1[i].iov_len = 100;
        memset(iov_1[i].iov_base, i, 100);
    }

    rc = zmq_msg_init_iov(&req, iov_1, 100, free_fn, iov);
    assert(rc == 0);

    rc = zmq_msg_send(&req, sreq, 0);
    assert(rc == 100 * 100);

    zmq_msg_close(&rsp);
    zmq_msg_init(&rsp);
    rc = zmq_msg_recv(&rsp, srep, 0);
    assert(rc == 100 * 100);

    int off = 0;
    char *d = (char *)zmq_msg_data(&rsp);
    for (int i = 0; i < 100; ++i, off += 100) {
        assert(!memcmp(d + off, iov_1[i].iov_base, 100));
    }
    zmq_msg_close(&rsp);

    rc = zmq_close(srep);
    assert (rc == 0);

    rc = zmq_close(sreq);
    assert (rc == 0);

    rc = zmq_close (sc);
    assert (rc == 0);

    rc = zmq_close (sb);
    assert (rc == 0);

    rc = zmq_ctx_term (ctx);
    assert (rc == 0);

    return 0;
}
