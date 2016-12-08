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

#include "v2_protocol.hpp"
#include "v2_encoder.hpp"
#include "likely.hpp"
#include "wire.hpp"

zmq::v2_encoder_t::v2_encoder_t () :
        msg(NULL)
{
}

zmq::v2_encoder_t::~v2_encoder_t ()
{
}

void zmq::v2_encoder_t::load_msg(zmq::msg_t *msg_)
{
    msg = msg_;
}

void zmq::v2_encoder_t::encode(iovec_buf &buf)
{
    size_t msg_size = msg->size();
    size_t hdr_len = msg_size > 255 ? 9 : 2;
    unsigned char *flags_len = reinterpret_cast<unsigned char *>(msg->push(hdr_len));
    unsigned char &protocol_flags = *flags_len;
    protocol_flags = 0;
    unsigned char msg_flags = msg->flags();
    if (msg_flags & msg_t::more)
        protocol_flags |= v2_protocol_t::more_flag;
    if (msg_size > 255) {
        protocol_flags |= v2_protocol_t::large_flag;
        put_uint64(flags_len + 1, msg_size);
    } else {
        flags_len[1] = msg_size;
    }
    if (msg_flags & msg_t::command)
        protocol_flags |= v2_protocol_t::command_flag;
    msg->add_to_iovec_buf(buf);
}
