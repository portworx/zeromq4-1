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
    inline void set_id(zmq_id &id, const void *data, size_t len) {
    	assert(len <= 7);
    	id = len;
    	for (size_t i = 0u; i < len; ++i) {
    		id |= (unsigned long)((unsigned char*)data)[i] << ((i + 1) * 8);
    	}
    }

    struct iovec_buf;

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
	    malloced = 4,	//  Message structure was malloced
	    delimiter = 8,      //  Message is a delimiter
            credential = 32,
            identity = 64,
            shared = 128
        };

        bool check ();
        int init ();
        int init_size (size_t size_);
        int init_data (void *data_, size_t size_, msg_free_fn *ffn_,
            void *hint_);
	void init_content(zmq_content *content_, size_t size_,
		msg_free_fn *ffn_,
		void *hint_);
        int init_iov(iovec *iov, int iovcnt, size_t size, msg_free_fn *ffn_, void *hint);
	int init_iov_content(zmq_content *content, iovec *iov, int iovcnt, size_t size,
		msg_free_fn *ffn_, void *hint);
        int init_delimiter ();
        int close ();
        int move (msg_t &src_);
        int copy (msg_t &src_);
        void *data ();
        void *buf(int index);
	void *push(size_t size_);
	void *pull(size_t size_);
	void add_to_iovec_buf(iovec_buf &buf);

        iovec *iov() const { return u.lmsg.content->data_iov; };

	int iovcnt() const { return u.lmsg.content->iovcnt; }

        int num_bufs();
        size_t buf_size(int index);
        size_t size () { return u.base.size; };
        unsigned char flags () { return u.base.flags; };
        void set_flags (unsigned char flags_);
        void reset_flags (unsigned char flags_);

            metadata_t *metadata () const;
        void set_metadata (metadata_t *metadata_);
        void reset_metadata ();
        bool is_identity () const;
        bool is_credential () const;
        bool is_delimiter () const;
        bool is_empty ();

        //  After calling this function you can copy the message in POD-style
        //  refs_ times. No need to call copy.
        void add_refs (int refs_);

        //  Removes references previously added by add_refs. If the number of
        //  references drops to 0, the message is closed and false is returned.
        bool rm_refs (int refs_);

	zmq_id get_id() { return u.base.id; };

	void set_id(const blob_t &blob)
	{
		zmq::set_id(u.base.id, blob.data(), blob.size());
	}

	void set_id(size_t len, const void *data)
	{
		zmq::set_id(u.base.id, data, len);
	}

	void set_id(zmq_id id)
	{
		u.base.id = id;
	}
    private:
	void init_vsm();
	void init_lsm(size_t size);
	iovec *lmsg_iov(int index);

        //  Size in bytes of the largest message that is still copied around
        //  rather than being reference-counted.
        enum { msg_t_size = 72 };
        enum { max_vsm_size = msg_t_size - (sizeof(iovec) + sizeof (metadata_t *) + 2 + 8) };

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
            //  empty message
            type_empty = 101,
            //  LMSG messages store the content in malloc-ed memory
            type_lmsg = 102,
            type_max = 102
        };

        //  Note that fields shared between different message types are not
        //  moved to the parent class (msg_t). This way we get tighter packing
        //  of the data. Shared fields can be accessed via 'base' member of
        //  the union.
        union {
            struct {
                metadata_t *metadata;
		zmq_id id;
		void *ptr_unused;
		size_t size;
                unsigned char unused [msg_t_size - (sizeof (metadata_t *) + 2 + 24)];
		unsigned char type;
		unsigned char flags;
            } base;
            struct {
                metadata_t *metadata;
		zmq_id id;
                content_t *content;
		size_t size;	// total message size
                unsigned char hdr [msg_t_size - (sizeof (metadata_t *) +
			sizeof (size_t) + sizeof (content_t*) + 2 + 8)];
                unsigned char type;
                unsigned char flags;

                size_t hdr_size() const { return size - content->size; }
            } lmsg;
        } u;
    };

}

#endif
