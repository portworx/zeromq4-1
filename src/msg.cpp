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

#include "platform.hpp"

#ifdef ZMQ_HAVE_WINDOWS
#include "windows.hpp"
#endif

#include "msg.hpp"
#include "../include/zmq.h"

#include <string.h>
#include <stdlib.h>
#include <new>
#include "stdint.hpp"
#include "likely.hpp"
#include "metadata.hpp"
#include "err.hpp"
#include "i_encoder.hpp"

//  Check whether the sizes of public representation of the message (zmq_msg_t)
//  and private representation of the message (zmq::msg_t) match.

typedef char zmq_msg_size_check
    [2 * ((sizeof (zmq::msg_t) == sizeof (zmq_msg_t)) != 0) - 1];

inline void zmq::msg_t::init_lsm(size_t size)
{
    u.lmsg.flags = 0;
    u.lmsg.id = 0;
    u.lmsg.size = size;
}

void zmq::msg_t::init ()
{
    u.lmsg.size = 0;
    u.lmsg.content = nullptr;
    u.lmsg.flags = 0;
}

void zmq::msg_t::alloc_memory(size_t alloc_size)
{
    if (alloc_size <= buf_pool_max_alloc) {
        u.lmsg.content = static_cast<content_t*>(alloc_buf());
        u.lmsg.flags |= pool_alloc;
    } else {
        u.lmsg.content = static_cast<content_t*>(malloc(alloc_size));
    }
}

void zmq::msg_t::init_size (size_t size_)
{
    init_lsm(size_);

    alloc_memory(sizeof(content_t) + sizeof(iovec) + size_);

    u.lmsg.content->data_iov = (iovec *) (u.lmsg.content + 1);
    u.lmsg.content->iovcnt = 1;
    u.lmsg.content->data_iov->iov_base = (char *) (u.lmsg.content + 1) + sizeof(iovec);
    u.lmsg.content->data_iov->iov_len = size_;
    u.lmsg.content->size = size_;
    u.lmsg.content->ffn = NULL;
    u.lmsg.content->hint = NULL;
    new(&u.lmsg.content->refcnt) zmq::atomic_counter_t();
}

namespace {

void do_nothing(void *, void *) {}

}

void zmq::msg_t::init_data (void *data_, size_t size_, msg_free_fn *ffn_,
    void *hint_)
{
    if (ffn_ == nullptr) {
        ffn_ = do_nothing;
    }

    init_lsm(size_);

    alloc_memory(sizeof(content_t) + sizeof(iovec));

    u.lmsg.content->data_iov = (iovec *) (u.lmsg.content + 1);
    u.lmsg.content->iovcnt = 1;
    u.lmsg.content->data_iov->iov_base = data_;
    u.lmsg.content->data_iov->iov_len = size_;
    u.lmsg.content->size = size_;
    u.lmsg.content->ffn = ffn_;
    u.lmsg.content->hint = hint_;
    new(&u.lmsg.content->refcnt) zmq::atomic_counter_t();
}

void zmq::msg_t::init_content(zmq_content *data_, size_t size_,
	msg_free_fn *ffn_, void *hint_)
{
	init_lsm(size_);
	u.lmsg.content = reinterpret_cast<content_t *>(data_);
	u.lmsg.content->data_iov = (iovec *)(u.lmsg.content + 1);
	u.lmsg.content->iovcnt = 1;
	u.lmsg.content->data_iov->iov_base = data_ + 1;
	u.lmsg.content->data_iov->iov_len = size_;
	u.lmsg.content->size = size_;
	u.lmsg.content->ffn = ffn_;
	u.lmsg.content->hint = hint_;
	u.lmsg.flags |= malloced;
	new (&u.lmsg.content->refcnt) zmq::atomic_counter_t ();
}

void zmq::msg_t::init_iov(iovec *iov, int iovcnt, size_t size, msg_free_fn *ffn_, void *hint_)
{
	zmq_assert(iov != NULL && iovcnt > 0 && ffn_ != NULL);
	init_lsm(size);

	alloc_memory(sizeof (content_t));

	u.lmsg.content->data_iov = iov;
	u.lmsg.content->iovcnt = iovcnt;
	u.lmsg.content->size = size;
	u.lmsg.content->ffn = ffn_;
	u.lmsg.content->hint = hint_;
	new (&u.lmsg.content->refcnt) zmq::atomic_counter_t ();
}

void zmq::msg_t::init_iov_content(zmq_content *content, iovec *iov, int iovcnt, size_t size,
	msg_free_fn *ffn_, void *hint_)
{
	init_lsm(size);
	u.lmsg.content = (content_t*)content;
	u.lmsg.content->data_iov = iov;
	u.lmsg.content->iovcnt = iovcnt;
	u.lmsg.content->size = size;
	u.lmsg.content->ffn = ffn_;
	u.lmsg.content->hint = hint_;
	u.lmsg.flags |= malloced;
	new (&u.lmsg.content->refcnt) zmq::atomic_counter_t ();
}

int zmq::msg_t::init_delimiter ()
{
    init();
    u.base.flags = delimiter;
    return 0;
}

void zmq::msg_t::close()
{
    // Save a copy of fields since message can be destroyed once free function is called.
    auto flags = u.lmsg.flags;
    auto content = u.lmsg.content;

    if (content == nullptr)
        return;

    //  Make the message invalid.
    u.lmsg.content = nullptr;

    //  If the content is not shared, or if it is shared and the reference
    //  count has dropped to zero, deallocate it.
    if (!(flags & msg_t::shared) || !content->refcnt.sub(1)) {
        if (content->ffn)
            content->ffn(content->data_iov->iov_base, content->hint);

        switch (flags & (malloced | pool_alloc)) {
        case pool_alloc:
            free_buf(content);
            break;
        case 0:
            ::free(content);
            break;
        default:
            // message will be freed by caller
            break;
        }
    }
}

void zmq::msg_t::move (msg_t &src_)
{
    close ();

    *this = src_;

    src_.init ();
}

void zmq::msg_t::copy (msg_t &src_)
{
    close ();

    if (src_.u.lmsg.content == nullptr)
        return;

    //  One reference is added to shared messages. Non-shared messages
    //  are turned into shared messages and reference count is set to 2.
    if (src_.u.lmsg.flags & msg_t::shared)
        src_.u.lmsg.content->refcnt.add(1);
    else {
        src_.u.lmsg.flags |= msg_t::shared;
        src_.u.lmsg.content->refcnt.set(2);
    }

    *this = src_;
}

void *zmq::msg_t::data ()
{
    return u.lmsg.size > u.lmsg.content->size ?
           u.lmsg.hdr + sizeof(u.lmsg.hdr) - u.lmsg.hdr_size() :
           u.lmsg.content->data_iov[0].iov_base;
}

void *zmq::msg_t::buf(int index)
{
    size_t hdr_size = u.lmsg.hdr_size();
    int off = hdr_size ? 1 : 0;
    zmq_assert(index - off < u.lmsg.content->iovcnt);
    if (!index) {
        return hdr_size ? u.lmsg.hdr + sizeof(u.lmsg.hdr) - hdr_size :
               u.lmsg.content->data_iov[0].iov_base;
    } else {
        return u.lmsg.content->data_iov[index - off].iov_base;
    }
}

size_t zmq::msg_t::buf_size(int index)
{
    size_t hdr_size = u.lmsg.hdr_size();
    int off = hdr_size ? 1 : 0;
    zmq_assert(index - off < u.lmsg.content->iovcnt);
    if (!index) {
        return hdr_size ? hdr_size :
               u.lmsg.content->data_iov[0].iov_len;
    } else {
        return u.lmsg.content->data_iov[index - off].iov_len;
    }
}

int zmq::msg_t::num_bufs()
{
     return (u.lmsg.hdr_size() > 0 ? 1 : 0) + u.lmsg.content->iovcnt;
}

void zmq::msg_t::set_flags (unsigned char flags_)
{
    u.base.flags |= flags_;
}

void zmq::msg_t::reset_flags (unsigned char flags_)
{
    u.base.flags &= ~flags_;
}

bool zmq::msg_t::is_identity () const
{
    return (u.base.flags & identity) == identity;
}

bool zmq::msg_t::is_delimiter () const
{
    return (u.base.flags & delimiter) != 0;
}

void *zmq::msg_t::push(size_t size_)
{
    size_t hdr_size = u.lmsg.hdr_size();
    zmq_assert(size_ + hdr_size <= sizeof(u.lmsg.hdr));
    u.lmsg.size += size_;
    hdr_size += size_;
    return hdr_size ?
           u.lmsg.hdr + sizeof(u.lmsg.hdr) - hdr_size :
           u.lmsg.content->data_iov[0].iov_base;
}

void *zmq::msg_t::pull(size_t size_)
{
    size_t hdr_size = u.lmsg.hdr_size();
    assert(hdr_size >= size_);
    u.lmsg.size -= size_;
    hdr_size -= size_;
    return hdr_size > 0 ?
           u.lmsg.hdr + sizeof(u.lmsg.hdr) - hdr_size :
           u.lmsg.content->data_iov[0].iov_base;
}

void zmq::msg_t::add_to_iovec_buf(zmq::iovec_buf &buf)
{
    size_t hdr_size = u.lmsg.hdr_size();
    if (hdr_size) {
        iovec i = {u.lmsg.hdr + sizeof(u.lmsg.hdr) - hdr_size, hdr_size};
        buf.iov.push_back(i);
    }
    std::copy(u.lmsg.content->data_iov,
              u.lmsg.content->data_iov + u.lmsg.content->iovcnt,
              std::back_inserter(buf.iov));
    buf.size += u.lmsg.size;
}

zmq::global_buf_pool::global_buf_pool(size_t struct_size, size_t align) :
	struct_size(struct_size), alignment(align)
{
}

zmq::global_buf_pool::~global_buf_pool()
{
    for (auto &v : blocks)
        free(v);
}

zmq::buf_pool::buf_pool(zmq::global_buf_pool &global_list, size_t block_size) :
    block_size(block_size), global_list(global_list)
{
}

zmq::buf_pool::~buf_pool()
{
    std::lock_guard<std::mutex> lock(global_list.lock);
    global_list.list.insert(global_list.list.end(), list.begin(), list.end());
}

void *zmq::buf_pool::alloc_slow()
{
    std::unique_lock<std::mutex> lock(global_list.lock);
    if (global_list.list.size() < block_size) {
        lock.unlock();
        auto alloc_size = global_list.struct_size * block_size;
        auto mem = global_list.alignment == 0 ?
                   malloc(alloc_size) :
                   aligned_alloc(global_list.alignment, alloc_size);
        lock.lock();
        global_list.blocks.push_back(mem);
        lock.unlock();
        for (auto i = 0ul; i < block_size; ++i)
            list.push_back(static_cast<char*>(mem) + i * global_list.struct_size);
    } else {
        auto end = global_list.list.end();
        auto beg = end - block_size;
        list.insert(list.end(), beg, end);
        global_list.list.erase(beg, end);
        lock.unlock();
    }
    return alloc();
}

void *zmq::buf_pool::alloc()
{
#if !defined(__SANITIZE_ADDRESS__)
    if (!list.empty()) {
        auto val = list.back();
        list.pop_back();
        return val;
    } else {
        return alloc_slow();
    }
#else
    return global_list.alignment == 0 ?
                   malloc(global_list.struct_size) :
                   aligned_alloc(global_list.alignment, global_list.struct_size);
#endif
}

void zmq::buf_pool::free(void *obj)
{
#if !defined(__SANITIZE_ADDRESS__)
    list.push_back(obj);
    if (list.size() == block_size * 2) {
        std::lock_guard<std::mutex> lock(global_list.lock);
        auto end = list.end();
        auto beg = end - block_size;
        global_list.list.insert(global_list.list.end(), beg, end);
        list.erase(beg, end);
    }
#else
    ::free(obj);
#endif
}

namespace {

zmq::global_buf_pool global_free_list(zmq::buf_pool_max_alloc);

}

thread_local zmq::buf_pool zmq::detail::local_pool(global_free_list, 1024);
