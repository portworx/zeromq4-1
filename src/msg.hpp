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
#include <mutex>
#include <vector>

#include "config.hpp"
#include "atomic_counter.hpp"
#include "metadata.hpp"
#include "../include/zmq_id.h"
#include "blob.hpp"
#include "err.hpp"
#include "v2_protocol.hpp"

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
	uint64_t val = len;
    	for (size_t i = 0u; i < len; ++i) {
    		val |= (unsigned long)((unsigned char*)data)[i] << ((i + 1) * 8);
    	}
	id = zmq_id(val);
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
            //  (1) Followed by more parts (see ZMTP spec)
            more = zmq::v2_protocol_t::more_flag,
            malloced = 2,	//  Message structure was malloced
            // (4) Command frame (see ZMTP spec)
            command = zmq::v2_protocol_t::command_flag,
	    delimiter = 8,      //  Message is a delimiter
	    pool_alloc = 16,    // memory was allocated from pool
	    retransmit = 32,
            identity = 64,
            shared = 128
        };

        void init ();
        void init_size (size_t size_);
        void init_data (void *data_, size_t size_, msg_free_fn *ffn_,
            void *hint_);
	void init_content(zmq_content *content_, size_t size_,
		msg_free_fn *ffn_,
		void *hint_);
        void init_iov(iovec *iov, int iovcnt, size_t size, msg_free_fn *ffn_, void *hint);
	void init_iov_content(zmq_content *content, iovec *iov, int iovcnt, size_t size,
		msg_free_fn *ffn_, void *hint);
        void init_delimiter ();
        void close ();
        void move (msg_t &src_);
        void copy (msg_t &src_);
        void *data ();

	void *hdr() { return hdr_ + sizeof(hdr_) - hdr_size(); }

        void *push(size_t size);
	void *pull(size_t size);
	void add_to_iovec_buf(iovec_buf &buf);

        iovec *iov() const { return content_->data_iov; };

	int iovcnt() const { return content_->iovcnt; }

	size_t size () const { return size_; };
        unsigned char flags () const { return flags_; };
        void set_flags (unsigned char flags) { flags_ |= flags; }
        void reset_flags (unsigned char flags) { flags_ &= ~flags; };

        bool is_identity () const;
        bool is_delimiter () const;

        zmq_id get_id() const { return id_; };

	void set_id(const blob_t &blob)
	{
		zmq::set_id(id_, blob.data(), blob.size());
	}

	void set_id(size_t len, const void *data)
	{
		zmq::set_id(id_, data, len);
	}

	void set_id(zmq_id id)
	{
		id_ = id;
	}

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

    	size_t hdr_size() const { return size() - content_->size; }

	content_t *content() { return content_; }

    private:
        void init_lsm(size_t size);

        //  Size in bytes of the largest message that is still copied around
        //  rather than being reference-counted.
        enum { msg_t_size = 64 };

        zmq_id id_;
        content_t *content_;
        size_t size_;	// total message size
        unsigned char hdr_ [msg_t_size - (sizeof (size_t) + sizeof (content_t*) + 1 + 8)];
        unsigned char flags_;

        // allocate memory from pool or global malloc depending on size
        void alloc_memory(size_t alloc_size);
    };

    class alignas(64) global_buf_pool {
    public:
        global_buf_pool(size_t struct_size, size_t align = 0);
        ~global_buf_pool();
    private:
        friend class buf_pool;

        std::mutex lock;
        size_t struct_size;
        size_t alignment;
        std::vector<void *> list;
        std::vector<void *> blocks;
    };

    class buf_pool {
    public:
        buf_pool(global_buf_pool &global_list, size_t block_size);

        ~buf_pool();

        void *alloc();

        void free(void *obj);
    private:
        void *alloc_slow();

        std::vector<void *> list;
        size_t block_size;
        global_buf_pool &global_list;
    };

    constexpr size_t buf_pool_max_alloc = 17 * 64;

    namespace detail {

    extern thread_local buf_pool local_pool;

    }
    inline void *alloc_buf()
    {
        return detail::local_pool.alloc();
    }

    inline void free_buf(void *obj)
    {
        detail::local_pool.free(obj);
    }
}

#endif
