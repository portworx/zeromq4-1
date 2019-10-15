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

#ifndef __ZMQ_I_ENCODER_HPP_INCLUDED__
#define __ZMQ_I_ENCODER_HPP_INCLUDED__

#include "stdint.hpp"
#include "sys/uio.h"
#include <vector>

namespace zmq
{

    //  Forward declaration
    class msg_t;

struct iovec_buf {
        iovec_buf();
	~iovec_buf();

        std::vector<iovec> iov;
        int curr;
	bool msg_allocated;
        size_t size;

        std::vector<zmq::msg_t> msgs;

        size_t next_msg = -1;    // index of next message to free

        size_t next_msg_remain = 0; // number of bytes remaining unsent in next message

	static const size_t max_msgs = 100;

        void reset();
        void pull(size_t num_bytes);
        void pull_iov(size_t num_bytes);
        int count() { return iov.size() - curr; };
};

//  Interface to be implemented by message encoder.

    struct i_encoder
    {
        virtual ~i_encoder () {}

        virtual void encode(iovec_buf &buf) = 0;

        //  Load a new message into encoder.
        virtual void load_msg (msg_t *msg_) = 0;

    };

}

#endif
