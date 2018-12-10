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

bool zmq::msg_t::check ()
{
     return u.base.type >= type_min && u.base.type <= type_max;
}

inline void zmq::msg_t::init_vsm()
{
    u.vsm.metadata = NULL;
    u.vsm.type = type_vsm;
    u.vsm.flags = 0;
    u.vsm.iov.iov_len = 0;
    u.vsm.id = 0;
}

inline void zmq::msg_t::init_lsm()
{
    u.lmsg.metadata = NULL;
    u.lmsg.type = type_lmsg;
    u.lmsg.flags = 0;
    u.lmsg.id = 0;
    u.lmsg.hdr_size = 0;
}

int zmq::msg_t::init ()
{
    init_vsm();
    return 0;
}

int zmq::msg_t::init_size (size_t size_)
{
	init_lsm();
	u.lmsg.content =
		(content_t*) malloc (sizeof (content_t) + sizeof(iovec) + size_);
	if (unlikely (!u.lmsg.content)) {
		errno = ENOMEM;
		return -1;
	}

	u.lmsg.content->data_iov = (iovec *)(u.lmsg.content + 1);
	u.lmsg.content->iovcnt = 1;
	u.lmsg.content->data_iov->iov_base = (char *)(u.lmsg.content + 1) + sizeof(iovec);
	u.lmsg.content->data_iov->iov_len = size_;
	u.lmsg.content->size = size_;
	u.lmsg.content->ffn = NULL;
	u.lmsg.content->hint = NULL;
	new (&u.lmsg.content->refcnt) zmq::atomic_counter_t ();
	return 0;
}

int zmq::msg_t::init_data (void *data_, size_t size_, msg_free_fn *ffn_,
    void *hint_)
{
    //  If data is NULL and size is not 0, a segfault
    //  would occur once the data is accessed
    zmq_assert (data_ != NULL || size_ == 0);

    //  Initialize constant message if there's no need to deallocate
    if (ffn_ == NULL) {
        u.cmsg.metadata = NULL;
        u.cmsg.type = type_cmsg;
        u.cmsg.flags = 0;
	u.cmsg.hdr_size = 0;
        u.cmsg.iov.iov_base = data_;
        u.cmsg.iov.iov_len = size_;
        u.cmsg.id = 0;
    }
    else {
        init_lsm();
        u.lmsg.content = (content_t*) malloc (sizeof (content_t) + sizeof(iovec));
        if (!u.lmsg.content) {
            errno = ENOMEM;
            return -1;
        }

        u.lmsg.content->data_iov = (iovec *)(u.lmsg.content + 1);
        u.lmsg.content->iovcnt = 1;
        u.lmsg.content->data_iov->iov_base = data_;
        u.lmsg.content->data_iov->iov_len = size_;
        u.lmsg.content->size = size_;
        u.lmsg.content->ffn = ffn_;
        u.lmsg.content->hint = hint_;
        new (&u.lmsg.content->refcnt) zmq::atomic_counter_t ();
    }
    return 0;

}

void zmq::msg_t::init_content(zmq_content *data_, size_t size_,
	msg_free_fn *ffn_, void *hint_)
{
	init_lsm();
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

int zmq::msg_t::init_iov(iovec *iov, int iovcnt, size_t size, msg_free_fn *ffn_, void *hint_)
{
	zmq_assert(iov != NULL && iovcnt > 0 && ffn_ != NULL);
	init_lsm();
	u.lmsg.content = (content_t*) malloc (sizeof (content_t));
	if (!u.lmsg.content) {
		errno = ENOMEM;
		return -1;
	}

	u.lmsg.content->data_iov = iov;
	u.lmsg.content->iovcnt = iovcnt;
	u.lmsg.content->size = size;
	u.lmsg.content->ffn = ffn_;
	u.lmsg.content->hint = hint_;
	new (&u.lmsg.content->refcnt) zmq::atomic_counter_t ();

	return 0;
}

int zmq::msg_t::init_iov_content(zmq_content *content, iovec *iov, int iovcnt, size_t size,
	msg_free_fn *ffn_, void *hint_)
{
	init_lsm();
	u.lmsg.content = (content_t*)content;
	u.lmsg.content->data_iov = iov;
	u.lmsg.content->iovcnt = iovcnt;
	u.lmsg.content->size = size;
	u.lmsg.content->ffn = ffn_;
	u.lmsg.content->hint = hint_;
	u.lmsg.flags |= malloced;
	new (&u.lmsg.content->refcnt) zmq::atomic_counter_t ();

	return 0;
}

int zmq::msg_t::init_delimiter ()
{
    u.delimiter.metadata = NULL;
    u.delimiter.type = type_delimiter;
    u.delimiter.flags = 0;
    u.delimiter.id = 0;
    return 0;
}

int zmq::msg_t::close()
{
	//  Check the validity of the message.
	if (unlikely (!check())) {
		errno = EFAULT;
		return -1;
	}

	char type = u.base.type;

	//  Make the message invalid.
	u.base.type = 0;

	if (u.base.metadata != NULL)
		if (u.base.metadata->drop_ref())
			delete u.base.metadata;
	if (type == type_lmsg) {

		//  If the content is not shared, or if it is shared and the reference
		//  count has dropped to zero, deallocate it.
		if (!(u.lmsg.flags & msg_t::shared) ||
		    !u.lmsg.content->refcnt.sub(1)) {

			//  We used "placement new" operator to initialize the reference
			//  counter so we call the destructor explicitly now.
			u.lmsg.content->refcnt.~atomic_counter_t();
			bool free_content = !(u.lmsg.flags & malloced);
			if (u.lmsg.content->ffn)
				u.lmsg.content
				 ->ffn(u.lmsg.content->data_iov->iov_base,
				       u.lmsg.content->hint);
			if (free_content)
				free(u.lmsg.content);
		}
	}

	return 0;
}

int zmq::msg_t::move (msg_t &src_)
{
    //  Check the validity of the source.
    if (unlikely (!src_.check ())) {
        errno = EFAULT;
        return -1;
    }

    int rc = close ();
    if (unlikely (rc < 0))
        return rc;

    *this = src_;

    rc = src_.init ();
    if (unlikely (rc < 0))
        return rc;

    return 0;
}

int zmq::msg_t::copy (msg_t &src_)
{
    //  Check the validity of the source.
    if (unlikely (!src_.check ())) {
        errno = EFAULT;
        return -1;
    }

    int rc = close ();
    if (unlikely (rc < 0))
        return rc;

    if (src_.u.base.type == type_lmsg) {

        //  One reference is added to shared messages. Non-shared messages
        //  are turned into shared messages and reference count is set to 2.
        if (src_.u.lmsg.flags & msg_t::shared)
            src_.u.lmsg.content->refcnt.add (1);
        else {
            src_.u.lmsg.flags |= msg_t::shared;
            src_.u.lmsg.content->refcnt.set (2);
        }
    }

    if (src_.u.base.metadata != NULL)
        src_.u.base.metadata->add_ref ();

    // if message is vsm, pointer to data in iov will be incorrect after copy,
    // however it is reset when iov is returned.
    *this = src_;

    return 0;

}

void *zmq::msg_t::data ()
{
    //  Check the validity of the message.
    zmq_assert (check ());

    switch (u.base.type) {
    case type_vsm:
        return u.vsm.data + max_vsm_size - u.vsm.iov.iov_len;
    case type_lmsg:
        return u.lmsg.hdr_size ?
	       u.lmsg.hdr + sizeof(u.lmsg.hdr) - u.lmsg.hdr_size :
	       u.lmsg.content->data_iov[0].iov_base;
    case type_cmsg:
        return u.cmsg.iov.iov_base;
    default:
        zmq_assert (false);
        return NULL;
    }
}

void *zmq::msg_t::buf(int index)
{
    //  Check the validity of the message.
    zmq_assert (check ());

    switch (u.base.type) {
        case type_vsm:
            zmq_assert(index == 0);
	    return u.vsm.data + max_vsm_size - u.vsm.iov.iov_len;
        case type_lmsg:
        {
  	    size_t hdr_size = u.lmsg.hdr_size;
            int off = hdr_size ? 1 : 0;
            zmq_assert(index - off < u.lmsg.content->iovcnt);
            if (!index) {
                return hdr_size ? u.lmsg.hdr + sizeof(u.lmsg.hdr) - hdr_size :
                       u.lmsg.content->data_iov[0].iov_base;
            } else {
                return u.lmsg.content->data_iov[index - off].iov_base;
            }
        }
        case type_cmsg:
	{
		size_t hdr_size = u.cmsg.hdr_size;
		zmq_assert(index == 0 || index == 1);
		if (!index) {
			return hdr_size ? u.cmsg.hdr + sizeof(u.cmsg.hdr) -
				  hdr_size : u.cmsg.iov.iov_base;
		} else {
			return u.cmsg.iov.iov_base;
		}
	}
        default:
            zmq_assert (false);
            return NULL;
    }
}

size_t zmq::msg_t::buf_size(int index)
{
    //  Check the validity of the message.
    zmq_assert (check ());

    switch (u.base.type) {
        case type_vsm:
            zmq_assert(index == 0);
            return u.vsm.iov.iov_len;
        case type_lmsg:
	{
		size_t hdr_size = u.lmsg.hdr_size;
		int off = hdr_size ? 1 : 0;
		zmq_assert(index - off < u.lmsg.content->iovcnt);
		if (!index) {
			return hdr_size ? u.lmsg.hdr_size :
			       u.lmsg.content->data_iov[0].iov_len;
		} else {
			return u.lmsg.content->data_iov[index - off].iov_len;
		}
	}
        case type_cmsg:
	{
		size_t hdr_size = u.cmsg.hdr_size;
		int off = hdr_size ? 1 : 0;
		zmq_assert(index - off < 1);
		if (!index) {
			return hdr_size ? u.cmsg.hdr_size : u.cmsg.iov.iov_len;
		} else {
			return u.cmsg.iov.iov_len;
		}
	}
        default:
            zmq_assert (false);
            return 0;
    }
}

int zmq::msg_t::num_bufs()
{
    //  Check the validity of the message.
    zmq_assert (check ());

    switch (u.base.type) {
        case type_vsm:
            return 1;
        case type_lmsg:
            return (u.lmsg.hdr_size ? 1 : 0) + u.lmsg.content->iovcnt;
        case type_cmsg:
            return 1;
        default:
            zmq_assert (false);
            return 0;
    }
}

int zmq::msg_t::iovcnt()
{
	if (u.base.type == type_lmsg)
	    return u.lmsg.content->iovcnt;
        else if (u.base.type == type_vsm)
            return 0;
	else if (u.base.type == type_cmsg) {
	     return 1;
	} else {
	     return 0;
	}
}

iovec *zmq::msg_t::iov()
{
    switch (u.base.type) {
    case type_vsm:
        u.vsm.iov.iov_base = u.vsm.data + max_vsm_size - u.vsm.iov.iov_len;
        return &u.vsm.iov;
    case type_lmsg:
        return u.lmsg.content->data_iov;
    case type_cmsg:
        return &u.cmsg.iov;
    default:
	zmq_assert (false);
	return NULL;
    }
}

size_t zmq::msg_t::size ()
{
    //  Check the validity of the message.
    zmq_assert (check ());

    switch (u.base.type) {
    case type_vsm:
        return u.vsm.iov.iov_len;
    case type_lmsg:
        return u.lmsg.content->size + u.lmsg.hdr_size;
    case type_cmsg:
        return u.cmsg.iov.iov_len;
    default:
        zmq_assert (false);
        return 0;
    }
}

void zmq::msg_t::set_flags (unsigned char flags_)
{
    u.base.flags |= flags_;
}

void zmq::msg_t::reset_flags (unsigned char flags_)
{
    u.base.flags &= ~flags_;
}

zmq::metadata_t *zmq::msg_t::metadata () const
{
    return u.base.metadata;
}

void zmq::msg_t::set_metadata (zmq::metadata_t *metadata_)
{
    assert (metadata_ != NULL);
    assert (u.base.metadata == NULL);
    metadata_->add_ref ();
    u.base.metadata = metadata_;
}

void zmq::msg_t::reset_metadata ()
{
    if (u.base.metadata) {
        if (u.base.metadata->drop_ref ())
            delete u.base.metadata;
        u.base.metadata = NULL;
    }
}

bool zmq::msg_t::is_identity () const
{
    return (u.base.flags & identity) == identity;
}

bool zmq::msg_t::is_credential () const
{
    return (u.base.flags & credential) == credential;
}

bool zmq::msg_t::is_delimiter () const
{
    return u.base.type == type_delimiter;
}

bool zmq::msg_t::is_vsm ()
{
    return u.base.type == type_vsm;
}

bool zmq::msg_t::is_cmsg ()
{
    return u.base.type == type_cmsg;
}

void zmq::msg_t::add_refs (int refs_)
{
    zmq_assert (refs_ >= 0);

    //  Operation not supported for messages with metadata.
    zmq_assert (u.base.metadata == NULL);

    //  No copies required.
    if (!refs_)
        return;

    //  VSMs, CMSGS and delimiters can be copied straight away. The only
    //  message type that needs special care are long messages.
    if (u.base.type == type_lmsg) {
        if (u.lmsg.flags & msg_t::shared)
            u.lmsg.content->refcnt.add (refs_);
        else {
            u.lmsg.content->refcnt.set (refs_ + 1);
            u.lmsg.flags |= msg_t::shared;
        }
    }
}

bool zmq::msg_t::rm_refs (int refs_)
{
    zmq_assert (refs_ >= 0);

    //  Operation not supported for messages with metadata.
    zmq_assert (u.base.metadata == NULL);

    //  No copies required.
    if (!refs_)
        return true;

    //  If there's only one reference close the message.
    if (u.base.type != type_lmsg || !(u.lmsg.flags & msg_t::shared)) {
        close ();
        return false;
    }

    //  The only message type that needs special care are long messages.
    if (!u.lmsg.content->refcnt.sub (refs_)) {
        //  We used "placement new" operator to initialize the reference
        //  counter so we call the destructor explicitly now.
        u.lmsg.content->refcnt.~atomic_counter_t ();

        if (u.lmsg.content->ffn)
            u.lmsg.content->ffn (u.lmsg.content->data_iov->iov_base, u.lmsg.content->hint);
        free (u.lmsg.content);

        return false;
    }

    return true;
}

void *zmq::msg_t::push(size_t size_)
{
    if (u.base.type == type_vsm) {
        zmq_assert(size_ + u.vsm.iov.iov_len <= max_vsm_size);
        u.vsm.iov.iov_len += size_;
	return u.vsm.data + max_vsm_size - u.vsm.iov.iov_len;
    } else if (u.base.type == type_lmsg) {
        zmq_assert(size_ + u.lmsg.hdr_size <= sizeof(u.lmsg.hdr));
        u.lmsg.hdr_size += size_;
	return u.lmsg.hdr_size ?
	   u.lmsg.hdr + sizeof(u.lmsg.hdr) - u.lmsg.hdr_size :
	   u.lmsg.content->data_iov[0].iov_base;
    } else if (u.base.type == type_cmsg) {
	    zmq_assert(size_ + u.cmsg.hdr_size <= sizeof(u.lmsg.hdr));
	    u.cmsg.hdr_size += size_;
	    return u.cmsg.hdr + sizeof(u.cmsg.hdr) - u.cmsg.hdr_size;
    } else {
        zmq_assert(0);
        return NULL;
    }
}

void *zmq::msg_t::pull(size_t size_)
{
    if (u.base.type == type_vsm) {
        zmq_assert(size_ <= u.vsm.iov.iov_len);
        u.vsm.iov.iov_len -= size_;
	return u.vsm.data + max_vsm_size - u.vsm.iov.iov_len;
    } else if (u.base.type == type_lmsg) {
	assert(u.lmsg.hdr_size >= size_);
	size_t v = std::min(u.lmsg.hdr_size, size_);
	u.lmsg.hdr_size -= v;
	size_ -= v;
	return u.lmsg.hdr_size ?
	       u.lmsg.hdr + sizeof(u.lmsg.hdr) - u.lmsg.hdr_size :
	       u.lmsg.content->data_iov[0].iov_base;
    } else {
        zmq_assert(0);
        return NULL;
    }
}

void zmq::msg_t::add_to_iovec_buf(zmq::iovec_buf &buf)
{
	if (u.base.type == type_lmsg) {
		if (u.lmsg.hdr_size) {
			iovec i = { u.lmsg.hdr + sizeof(u.lmsg.hdr) -
					    u.lmsg.hdr_size, u.lmsg.hdr_size };
			buf.iov.push_back(i);
		}
		std::copy(u.lmsg.content->data_iov,
			  u.lmsg.content->data_iov + u.lmsg.content->iovcnt,
				std::back_inserter(buf.iov));
		buf.size += u.lmsg.content->size + u.lmsg.hdr_size;
	} else if (u.base.type == type_vsm) {
		iovec i = { u.vsm.data + sizeof(u.vsm.data) - u.vsm.iov.iov_len,
			    u.vsm.iov.iov_len};
		buf.iov.push_back(i);
		buf.size += u.vsm.iov.iov_len;
	} else if (u.base.type == type_cmsg) {
		if (u.cmsg.hdr_size) {
			iovec i = { u.cmsg.hdr + sizeof(u.cmsg.hdr) -
					u.cmsg.hdr_size, u.cmsg.hdr_size };
			buf.iov.push_back(i);
		}
		buf.iov.push_back(u.cmsg.iov);
		buf.size += u.cmsg.hdr_size + u.cmsg.iov.iov_len;
	} else {
		assert(0);
	}
}

