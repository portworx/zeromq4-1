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
    flags_ = 0;
    id_ = zmq_id(0);
    size_ = size;
}

void zmq::msg_t::init ()
{
    size_ = 0;
    content_ = nullptr;
    flags_ = 0;
}

void zmq::msg_t::alloc_memory(size_t alloc_size)
{
    if (alloc_size <= buf_pool_max_alloc) {
        content_ = static_cast<content_t*>(alloc_buf());
        flags_ |= pool_alloc;
    } else {
        content_ = static_cast<content_t*>(malloc(alloc_size));
    }
}

void zmq::msg_t::init_size (size_t size_)
{
    init_lsm(size_);

    alloc_memory(sizeof(content_t) + sizeof(iovec) + size_);

    content_->data_iov = (iovec *) (content_ + 1);
    content_->iovcnt = 1;
    content_->data_iov->iov_base = (char *) (content_ + 1) + sizeof(iovec);
    content_->data_iov->iov_len = size_;
    content_->size = size_;
    content_->ffn = NULL;
    content_->hint = NULL;
    new(&content_->refcnt) zmq::atomic_counter_t();
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

    content_->data_iov = (iovec *) (content_ + 1);
    content_->iovcnt = 1;
    content_->data_iov->iov_base = data_;
    content_->data_iov->iov_len = size_;
    content_->size = size_;
    content_->ffn = ffn_;
    content_->hint = hint_;
    new(&content_->refcnt) zmq::atomic_counter_t();
}

void zmq::msg_t::init_content(zmq_content *data_, size_t size_,
	msg_free_fn *ffn_, void *hint_)
{
	init_lsm(size_);
	content_ = reinterpret_cast<content_t *>(data_);
	content_->data_iov = (iovec *)(content_ + 1);
	content_->iovcnt = 1;
	content_->data_iov->iov_base = data_ + 1;
	content_->data_iov->iov_len = size_;
	content_->size = size_;
	content_->ffn = ffn_;
	content_->hint = hint_;
	flags_ |= malloced;
	new (&content_->refcnt) zmq::atomic_counter_t ();
}

void zmq::msg_t::init_iov(iovec *iov, int iovcnt, size_t size, msg_free_fn *ffn_, void *hint_)
{
	zmq_assert(iov != NULL && iovcnt > 0 && ffn_ != NULL);
	init_lsm(size);

	alloc_memory(sizeof (content_t));

	content_->data_iov = iov;
	content_->iovcnt = iovcnt;
	content_->size = size;
	content_->ffn = ffn_;
	content_->hint = hint_;
	new (&content_->refcnt) zmq::atomic_counter_t ();
}

void zmq::msg_t::init_iov_content(zmq_content *content, iovec *iov, int iovcnt, size_t size,
	msg_free_fn *ffn_, void *hint_)
{
	init_lsm(size);
	content_ = (content_t*)content;
	content_->data_iov = iov;
	content_->iovcnt = iovcnt;
	content_->size = size;
	content_->ffn = ffn_;
	content_->hint = hint_;
	flags_ |= malloced;
	new (&content_->refcnt) zmq::atomic_counter_t ();
}

void zmq::msg_t::init_delimiter ()
{
    init();
    flags_ = delimiter;
}

void zmq::msg_t::close()
{
    // Save a copy of fields since message can be destroyed once free function is called.
    auto flags = flags_;
    auto content = content_;

    if (content == nullptr)
        return;

    //  Make the message invalid.
    content_ = nullptr;

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

    if (src_.content_ == nullptr)
        return;

    //  One reference is added to shared messages. Non-shared messages
    //  are turned into shared messages and reference count is set to 2.
    if (src_.flags_ & msg_t::shared)
        src_.content_->refcnt.add(1);
    else {
        src_.flags_ |= msg_t::shared;
        src_.content_->refcnt.set(2);
    }

    *this = src_;
}

void *zmq::msg_t::data ()
{
    return size_ > content_->size ?
           hdr_ + sizeof(hdr_) - hdr_size() :
           content_->data_iov[0].iov_base;
}

bool zmq::msg_t::is_identity () const
{
    return (flags_ & identity) == identity;
}

bool zmq::msg_t::is_delimiter () const
{
    return (flags_ & delimiter) != 0;
}

void *zmq::msg_t::push(size_t size)
{
    size_t hdr_size = this->hdr_size();
    zmq_assert(size + hdr_size <= sizeof(hdr_));
    size_ += size;
    hdr_size += size;
    return hdr_size ?
           hdr_ + sizeof(hdr_) - hdr_size :
           content_->data_iov[0].iov_base;
}

void *zmq::msg_t::pull(size_t size)
{
    size_t hdr_size = this->hdr_size();
    assert(hdr_size >= size);
    size_ -= size;
    hdr_size -= size_;
    return hdr_size > 0 ?
           hdr_ + sizeof(hdr_) - hdr_size :
           content_->data_iov[0].iov_base;
}

void zmq::msg_t::add_to_iovec_buf(zmq::iovec_buf &buf)
{
    size_t hdr_size = this->hdr_size();
    if (hdr_size) {
        iovec i = {hdr_ + sizeof(hdr_) - hdr_size, hdr_size};
        buf.iov.push_back(i);
    }
    std::copy(content_->data_iov,
              content_->data_iov + content_->iovcnt,
              std::back_inserter(buf.iov));
    buf.size += size();
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
