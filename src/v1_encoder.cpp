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

#include "v1_encoder.hpp"
#include "likely.hpp"
#include "wire.hpp"

void zmq::v1_encoder_t::encode(zmq::iovec_buf &buf)
{
    size_t msg_size = msg->size();
    size_t msg_flags_size = msg_size + 1;
    size_t hdr_len = msg_flags_size < 255 ? 2 : 10;
    unsigned char *flags_len = reinterpret_cast<unsigned char *>(msg->push(hdr_len));
    if (msg_flags_size < 255) {
        flags_len[0] = msg_flags_size;
        flags_len[1] = (msg->flags () & msg_t::more);
    } else {
        flags_len[0] = 0xff;
        put_uint64(&flags_len[1], msg_flags_size);
        flags_len[9] = (msg->flags () & msg_t::more);
    }
    msg->add_to_iovec_buf(buf);
}
