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

#ifndef __ZMQ_MSG_HPP_INCLUDE__
#define __ZMQ_MSG_HPP_INCLUDE__

#include <stddef.h>
#include <stdio.h>

#include "config.hpp"
#include "atomic_counter.hpp"
#include "metadata.hpp"
#include <sys/uio.h>
#include "../include/zmq_id.h"
#include "blob.hpp"
#include "err.hpp"

//  Signature for free function to deallocate the message content.
//  Note that it has to be declared as "C" so that it is the same as
//  zmq_free_fn defined in zmq.h.
extern "C"
{
    typedef void (msg_free_fn) (void *data, void *hint);
}

namespace zmq
{

    //  Note that this structure needs to be explicitly constructed
    //  (init functions) and destructed (close function).

    class msg_t
    {
    public:

        //  Message flags.
        enum
        {
            more = 1,           //  Followed by more parts
            command = 2,        //  Command frame (see ZMTP spec)
            credential = 32,
            identity = 64,
            shared = 128
        };

        bool check ();
        int init ();
        int init_size (size_t size_);
        int init_data (void *data_, size_t size_, msg_free_fn *ffn_,
            void *hint_);
        int init_iov(iovec *iov, int iovcnt, size_t size, msg_free_fn *ffn_, void *hint);
        int init_delimiter ();
        int close ();
        int move (msg_t &src_);
        int copy (msg_t &src_);
        void *data ();
        void *buf(int index);
	void *push(size_t size_);
	void *pull(size_t size_);
        iovec *iov();
	int iovcnt();
        int num_bufs();
        size_t buf_size(int index);
        size_t size ();
        unsigned char flags ();
        void set_flags (unsigned char flags_);
        void reset_flags (unsigned char flags_);

            metadata_t *metadata () const;
        void set_metadata (metadata_t *metadata_);
        void reset_metadata ();
        bool is_identity () const;
        bool is_credential () const;
        bool is_delimiter () const;
        bool is_vsm ();
        bool is_cmsg ();

        //  After calling this function you can copy the message in POD-style
        //  refs_ times. No need to call copy.
        void add_refs (int refs_);

        //  Removes references previously added by add_refs. If the number of
        //  references drops to 0, the message is closed and false is returned.
        bool rm_refs (int refs_);

	zmq_id get_id() { return u.base.id; };

	void set_id(const blob_t &blob) {
		size_t sz = blob.size();
		zmq_assert(0 < sz && sz <= 5);
		u.base.id.len = blob.size();
		memcpy(u.base.id.val, blob.data(), u.base.id.len);
	}

	void set_id(size_t len, void *data)
	{
		zmq_assert(0 < len && len <= 5);
		u.base.id.len = len;
		memcpy(u.base.id.val, data, len);
	}
    private:
	void init_vsm();
	void init_lsm();
	iovec *lmsg_iov(int index);

        //  Size in bytes of the largest message that is still copied around
        //  rather than being reference-counted.
        enum { msg_t_size = 64 };
        enum { max_vsm_size = msg_t_size - (sizeof(iovec) + sizeof (metadata_t *) + 2 + 6) };

        //  Shared message buffer. Message data are either allocated in one
        //  continuous block along with this structure - thus avoiding one
        //  malloc/free pair or they are stored in used-supplied memory.
        //  In the latter case, ffn member stores pointer to the function to be
        //  used to deallocate the data. If the buffer is actually shared (there
        //  are at least 2 references to it) refcount member contains number of
        //  references.
        struct content_t
        {
            iovec *data_iov;
            size_t size;
            msg_free_fn *ffn;
            void *hint;
            int iovcnt;
            zmq::atomic_counter_t refcnt;
        };

        //  Different message types.
        enum type_t
        {
            type_min = 101,
            //  VSM messages store the content in the message itself
            type_vsm = 101,
            //  LMSG messages store the content in malloc-ed memory
            type_lmsg = 102,
            //  Delimiter messages are used in envelopes
            type_delimiter = 103,
            //  CMSG messages point to constant data
            type_cmsg = 104,
            type_max = 104
        };

        //  Note that fields shared between different message types are not
        //  moved to the parent class (msg_t). This way we get tighter packing
        //  of the data. Shared fields can be accessed via 'base' member of
        //  the union.
        union {
            struct {
                metadata_t *metadata;
                unsigned char unused [msg_t_size - (sizeof (metadata_t *) + 2 + 6)];
		zmq_id id;
                unsigned char type;
                unsigned char flags;
            } base;
            struct {
                metadata_t *metadata;
                iovec iov;
                unsigned char data [max_vsm_size];
		zmq_id id;
                unsigned char type;
                unsigned char flags;
            } vsm;
            struct {
                metadata_t *metadata;
                content_t *content;
		size_t hdr_size;
                unsigned char hdr [msg_t_size - (sizeof (metadata_t *) +
			sizeof (size_t) + sizeof (content_t*) + 2 + 6)];
		zmq_id id;
                unsigned char type;
                unsigned char flags;
            } lmsg;
            struct {
                metadata_t *metadata;
                iovec iov;
                unsigned char hdr
                    [msg_t_size - (sizeof (metadata_t *) + sizeof (iovec) + 2 + 6)];
		zmq_id id;
                unsigned char type;
                unsigned char flags;
            } cmsg;
            struct {
                metadata_t *metadata;
                unsigned char unused [msg_t_size - (sizeof (metadata_t *) + 2 + 6)];
		zmq_id id;
                unsigned char type;
                unsigned char flags;
            } delimiter;
        } u;
    };

}

#endif
