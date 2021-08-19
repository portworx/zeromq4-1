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
#if defined ZMQ_HAVE_WINDOWS
#include "windows.hpp"
#else
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#include <netdb.h>
#include <fcntl.h>
#if defined ZMQ_HAVE_OPENBSD
#define ucred sockpeercred
#endif
#endif

#include <string.h>
#include <new>
#include <sstream>
#include <iostream>

#include "stream_engine.hpp"
#include "io_thread.hpp"
#include "session_base.hpp"
#include "v2_encoder.hpp"
#include "v2_decoder.hpp"
#include "null_mechanism.hpp"
#include "plain_client.hpp"
#include "plain_server.hpp"
#include "gssapi_client.hpp"
#include "gssapi_server.hpp"
#include "curve_client.hpp"
#include "curve_server.hpp"
#include "raw_decoder.hpp"
#include "raw_encoder.hpp"
#include "config.hpp"
#include "err.hpp"
#include "ip.hpp"
#include "tcp.hpp"
#include "likely.hpp"
#include "wire.hpp"

zmq::stream_engine_t::stream_engine_t (fd_t fd_, const options_t &options_,
                                       const std::string &endpoint_) :
    s (fd_),
    inpos (NULL),
    insize (0),
    decoder (NULL),
    decoder_ctx (NULL),
    encoder (NULL),
    metadata (NULL),
    handshaking (true),
    greeting_size (v2_greeting_size),
    greeting_bytes_read (0),
    session (NULL),
    options (options_),
    endpoint (endpoint_),
    plugged (false),
    next_msg (&stream_engine_t::identity_msg),
    process_msg (&stream_engine_t::process_identity_msg),
    io_error (false),
    subscription_required (false),
    mechanism (NULL),
    input_stopped (false),
    output_stopped (false),
    has_handshake_timer (false),
    socket (NULL)
{
    memset(&id_, 0, sizeof(id_));
    tx_msg.init ();

    //  Put the socket into non-blocking mode.
    unblock_socket (s);

    int family = get_peer_ip_address (s, peer_address);
    if (family == 0)
        peer_address.clear();
#if defined ZMQ_HAVE_SO_PEERCRED
    else
    if (family == PF_UNIX) {
        struct ucred cred;
        socklen_t size = sizeof (cred);
        if (!getsockopt (s, SOL_SOCKET, SO_PEERCRED, &cred, &size)) {
            std::ostringstream buf;
            buf << ":" << cred.uid << ":" << cred.gid << ":" << cred.pid;
            peer_address += buf.str ();
        }
    }
#elif defined ZMQ_HAVE_LOCAL_PEERCRED
    else
    if (family == PF_UNIX) {
        struct xucred cred;
        socklen_t size = sizeof (cred);
        if (!getsockopt (s, 0, LOCAL_PEERCRED, &cred, &size)
                && cred.cr_version == XUCRED_VERSION) {
            std::ostringstream buf;
            buf << ":" << cred.cr_uid << ":";
            if (cred.cr_ngroups > 0)
                buf << cred.cr_groups[0];
            buf << ":";
            peer_address += buf.str ();
        }
    }
#endif

#ifdef SO_NOSIGPIPE
    //  Make sure that SIGPIPE signal is not generated when writing to a
    //  connection that was already closed by the peer.
    int set = 1;
    rc = setsockopt (s, SOL_SOCKET, SO_NOSIGPIPE, &set, sizeof (int));
    errno_assert (rc == 0);
#endif
}

zmq::stream_engine_t::~stream_engine_t ()
{
    zmq_assert (!plugged);

    if (s != retired_fd) {
#ifdef ZMQ_HAVE_WINDOWS
        int rc = closesocket (s);
        wsa_assert (rc != SOCKET_ERROR);
#else
        int rc = close (s);
        errno_assert (rc == 0);
#endif
        s = retired_fd;
    }

    tx_msg.close ();

    //  Drop reference to metadata and destroy it if we are
    //  the only user.
    if (metadata != NULL)
        if (metadata->drop_ref ())
            delete metadata;

    delete encoder;
    if (!options.has_decoder_ops) {
	    delete decoder;
    } else {
	    options.dec_ops.destroy(decoder_ctx);
    }
    delete mechanism;
}

void zmq::stream_engine_t::plug (io_thread_t *io_thread_,
    session_base_t *session_)
{
    zmq_assert (!plugged);
    plugged = true;

    //  Connect to session object.
    zmq_assert (!session);
    zmq_assert (session_);
    session = session_;
    socket = session-> get_socket ();

    //  Connect to I/O threads poller object.
    io_object_t::plug (io_thread_);
    handle = add_fd (s);
    io_error = false;

    if (options.raw_sock) {
        // no handshaking for raw sock, instantiate raw encoder and decoders
        encoder = new(std::nothrow) raw_encoder_t();
        alloc_assert (encoder);

        decoder = new (std::nothrow) raw_decoder_t (in_batch_size);
        alloc_assert (decoder);

        // disable handshaking for raw socket
        handshaking = false;

        next_msg = &stream_engine_t::pull_msg_from_session;
        process_msg = &stream_engine_t::push_raw_msg_to_session;

        if (!peer_address.empty()) {
            //  Compile metadata.
            typedef metadata_t::dict_t properties_t;
            properties_t properties;
            properties.insert(std::make_pair("Peer-Address", peer_address));
            zmq_assert (metadata == NULL);
            metadata = new (std::nothrow) metadata_t (properties);
        }

        //  For raw sockets, send an initial 0-length message to the
        // application so that it knows a peer has connected.
        msg_t connector;
        connector.init();
        push_raw_msg_to_session (&connector);
        connector.close();
        session->flush ();
    }
    else {
        // start optional timer, to prevent handshake hanging on no input
        set_handshake_timer ();

        //  Send the 'length' and 'flags' fields of the identity message.
        //  The 'length' field is encoded in the long format.
        iovec iov = { greeting_send, 10 };
	outbuf.iov.push_back(iov);
	outbuf.size += 10;
	greeting_send[0] = 0xff;
        put_uint64 (&greeting_send[1], options.identity_size + 1);
	greeting_send[9] = 0x7f;
    }

    set_pollin (handle);
    set_pollout (handle);
    //  Flush all the data that may have been already received downstream.
    in_event ();
}

void zmq::stream_engine_t::unplug ()
{
    zmq_assert (plugged);
    plugged = false;

    //  Cancel all timers.
    if (has_handshake_timer) {
        cancel_timer (handshake_timer_id);
        has_handshake_timer = false;
    }

    //  Cancel all fd subscriptions.
    if (!io_error)
        rm_fd (handle);

    //  Disconnect from I/O threads poller object.
    io_object_t::unplug ();

    session = NULL;
}

void zmq::stream_engine_t::terminate ()
{
    unplug ();
    delete this;
}

void zmq::stream_engine_t::in_event ()
{
    zmq_assert (!io_error);

    //  If still handshaking, receive and process the greeting message.
    if (unlikely (handshaking))
        if (!handshake ())
            return;

    //  If there has been an I/O error, stop polling.
    if (input_stopped) {
        rm_fd (handle);
        io_error = true;
        return;
    }

    if (options.has_decoder_ops) {
        while (true) {
            decoder_ops::read_result res;
            options.dec_ops.read(decoder_ctx, s, &res);
            if (res.msg != nullptr) {
                if ((this->*process_msg)(res.msg) != 0) {
                    if (errno != EAGAIN) {
                        error(protocol_error);
                        return;
                    }
                    input_stopped = true;
                    reset_pollin (handle);
                    break;
                }
                if (res.last)
                    break;
            } else if (res.error == no_error) {
                // no more messages available
                break;
            } else {
                error(res.error);
                return;
            }
        }
    } else {
        //  If there's no data to process in the buffer...
        if (!insize) {

            //  Retrieve the buffer and read as much data as possible.
            //  Note that buffer can be arbitrarily large. However, we assume
            //  the underlying TCP layer has fixed buffer size and thus the
            //  number of bytes read will be always limited.
            size_t bufsize = 0;
            decoder->get_buffer (&inpos, &bufsize);

            const int rc = tcp_read (s, inpos, bufsize);
            if (rc == 0) {
                error (connection_error);
                return;
            }
            if (rc == -1) {
                if (errno != EAGAIN)
                    error (connection_error);
                return;
            }

            //  Adjust input size
            insize = static_cast <size_t> (rc);
        }

        int rc = 0;
        size_t processed = 0;

        while (insize > 0) {
            rc = decoder->decode (inpos, insize, processed);
            zmq_assert (processed <= insize);
            inpos += processed;
            insize -= processed;
            if (rc == 0 || rc == -1)
                break;
            msg_t *msg = decoder->msg();
            rc = (this->*process_msg) (msg);
            if (rc == -1)
                break;
        }

        //  Tear down the connection if we have failed to decode input data
        //  or the session has rejected the message.
        if (rc == -1) {
            if (errno != EAGAIN) {
                error (protocol_error);
                return;
            }
            input_stopped = true;
            reset_pollin (handle);
        }
    }

    session->flush ();
}

void zmq::stream_engine_t::out_event ()
{
    zmq_assert (!io_error);

    bool nothing_pending = false;

    //  If write buffer is empty, try to read new data from the encoder.
    if (!outbuf.size) {

        //  Even when we stop polling as soon as there is no
        //  data to send, the poller may invoke out_event one
        //  more time due to 'speculative write' optimisation.
        if (unlikely (encoder == NULL)) {
            zmq_assert (handshaking);
            return;
        }

	outbuf.reset();

        while (outbuf.size < out_batch_size &&
                (outbuf.msgs.size() < outbuf.msgs.capacity() ||
                        outbuf.msg_allocated)) {
	    if (!outbuf.msg_allocated) {
		outbuf.msgs.resize(outbuf.msgs.size() + 1);
		outbuf.msgs.back().init();
		outbuf.msg_allocated = true;
	    }

            if ((this->*next_msg) (&outbuf.msgs.back()) == -1) {
                nothing_pending = true;
                break;
            }
	    outbuf.msg_allocated = false;
            encoder->load_msg (&outbuf.msgs.back());
	    encoder->encode(outbuf);
        }

        //  If there is no data to send, stop polling for output.
        if (outbuf.size == 0) {
            output_stopped = true;
            poller->reset_pollout_state (handle);
            return;
        }
    }

    //  If there are any data to write in write buffer, write as much as
    //  possible to the socket. Note that amount of data to write can be
    //  arbitrarily large. However, we assume that underlying TCP layer has
    //  limited transmission buffer and thus the actual number of bytes
    //  written should be reasonably modest.

    ssize_t nbytes;
    while (1) {
         nbytes = writev(s, &outbuf.iov[outbuf.curr], outbuf.count());

        //  IO error has occurred. We stop waiting for output events.
        //  The engine is not terminated until we detect input error;
        //  this is necessary to prevent losing incoming messages.
        if (nbytes == -1) {
            if (errno == EINTR)
                continue;
            if (errno != EAGAIN) {
                poller->reset_pollout_state (handle);
            }
            return;
        }
        break;
    }

    //  If we are still handshaking and there are no data
    //  to send, stop polling for output.
    if (unlikely (handshaking)) {
        outbuf.pull_iov(nbytes);
        if (outbuf.size == 0)
            poller->reset_pollout_state (handle);
    } else {
        outbuf.pull(nbytes);
        if (outbuf.size == 0 && nothing_pending) {
            output_stopped = true;
            poller->reset_pollout_state(handle);
        }
    }
}

void zmq::stream_engine_t::restart_output ()
{
    if (unlikely (io_error))
        return;

    if (likely (output_stopped)) {
        poller->set_pollout_state(handle);
        output_stopped = false;
    }

    //  Speculative write: The assumption is that at the moment new message
    //  was sent by the user the socket is probably available for writing.
    //  Thus we try to write the data to socket avoiding polling for POLLOUT.
    //  Consequently, the latency should be better in request/reply scenarios.
    out_event ();

    poller->sync_pollout_state(handle);
}

void zmq::stream_engine_t::restart_input ()
{
    zmq_assert (input_stopped);
    zmq_assert (session != NULL);
    zmq_assert (decoder != NULL);

    if (options.has_decoder_ops) {
        while (true) {
            decoder_ops::read_result res;
            options.dec_ops.read(decoder_ctx, s, &res);
            if (res.msg != nullptr) {
                if ((this->*process_msg)(res.msg) != 0) {
                    error(protocol_error);
                    return;
                }
            } else if (res.error == no_error) {
                input_stopped = false;
                set_pollin (handle);
                session->flush ();

                //  Speculative read.
                in_event ();
                break;
            } else {
                error(res.error);
            }
        }
    } else {
        int rc = (this->*process_msg) (decoder->msg ());
        if (rc == -1) {
            if (errno == EAGAIN)
                session->flush ();
            else
                error (protocol_error);
            return;
        }

        while (insize > 0) {
            size_t processed = 0;
            rc = decoder->decode (inpos, insize, processed);
            zmq_assert (processed <= insize);
            inpos += processed;
            insize -= processed;
            if (rc == 0 || rc == -1)
                break;
            msg_t *msg = decoder->msg();
            rc = (this->*process_msg) (msg);
            if (rc == -1)
                break;
        }

        if (rc == -1 && errno == EAGAIN)
            session->flush ();
        else if (io_error)
            error (connection_error);
        else if (rc == -1)
            error (protocol_error);
        else {
            input_stopped = false;
            set_pollin (handle);
            session->flush ();

            //  Speculative read.
            in_event ();
        }
    }
}

bool zmq::stream_engine_t::handshake ()
{
    zmq_assert (handshaking);
    zmq_assert (greeting_bytes_read < greeting_size);
    //  Receive the greeting.
    while (greeting_bytes_read < greeting_size) {
        const int n = tcp_read (s, greeting_recv + greeting_bytes_read,
                                greeting_size - greeting_bytes_read);
        if (n == 0) {
            error (connection_error);
            return false;
        }
        if (n == -1) {
            if (errno != EAGAIN)
                error (connection_error);
            return false;
        }

        greeting_bytes_read += n;

        //  We have received at least one byte from the peer.
        //  If the first byte is not 0xff, we know that the
        //  peer is using unversioned protocol.
        if (greeting_recv [0] != 0xff)
            break;

        if (greeting_bytes_read < signature_size)
            continue;

        //  Inspect the right-most bit of the 10th byte (which coincides
        //  with the 'flags' field if a regular message was sent).
        //  Zero indicates this is a header of identity message
        //  (i.e. the peer is using the unversioned protocol).
        if (!(greeting_recv [9] & 0x01))
            break;

        //  The peer is using versioned protocol.
        //  Send the major version number.
	if (outbuf.size == 0 && outbuf.curr == 1) {
		outbuf.curr = 0;
		outbuf.iov[0].iov_base = (unsigned char *)outbuf.iov[0].iov_base +
			outbuf.iov[0].iov_len;
		outbuf.iov[0].iov_len = 0;
	}
	iovec &iov = outbuf.iov[outbuf.curr];
        if ((unsigned char *)iov.iov_base + iov.iov_len == greeting_send + signature_size) {
            if (outbuf.size == 0)
                set_pollout (handle);
	    ((unsigned char *)iov.iov_base)[iov.iov_len++] = 3; //  Major version number
	    ++outbuf.size;
        }

        if (greeting_bytes_read > signature_size) {
            if ((unsigned char *)iov.iov_base + iov.iov_len == greeting_send + signature_size + 1) {
                if (outbuf.size == 0)
                    set_pollout (handle);

                //  Use ZMTP/2.0 to talk to older peers.
                if (greeting_recv [10] == ZMTP_1_0
                ||  greeting_recv [10] == ZMTP_2_0) {
			((unsigned char *)iov.iov_base)[iov.iov_len++] = options.type;
			++outbuf.size;
		}
                else {
		    ((unsigned char *)iov.iov_base)[iov.iov_len++] = 0; //  Minor version number
		    ++outbuf.size;
                    memset ((unsigned char *)iov.iov_base + iov.iov_len, 0, 20);

                    zmq_assert (options.mechanism == ZMQ_NULL
                            ||  options.mechanism == ZMQ_PLAIN
                            ||  options.mechanism == ZMQ_CURVE
                            ||  options.mechanism == ZMQ_GSSAPI);

		    unsigned char *p = (unsigned char *)iov.iov_base + iov.iov_len;
                    if (options.mechanism == ZMQ_NULL)
                        memcpy (p, "NULL", 4);
                    else
                    if (options.mechanism == ZMQ_PLAIN)
                        memcpy (p, "PLAIN", 5);
                    else
                    if (options.mechanism == ZMQ_GSSAPI)
                        memcpy (p, "GSSAPI", 6);
                    else
                    if (options.mechanism == ZMQ_CURVE)
                        memcpy (p, "CURVE", 5);
		    iov.iov_len += 20;
		    outbuf.size += 20;
                    memset ((unsigned char *)iov.iov_base + iov.iov_len, 0, 32);
		    iov.iov_len += 32;
		    outbuf.size += 32;
                    greeting_size = v3_greeting_size;
                }
            }
        }
    }

    //  Position of the revision field in the greeting.
    const size_t revision_pos = 10;

    //  Is the peer using ZMTP/1.0 with no revision number?
    //  If so, we send and receive rest of identity message
    if (greeting_recv [0] != 0xff || !(greeting_recv [9] & 0x01)) {
        // reject ZMTP 1.0 connections
        error (protocol_error);
        return false;
    }
    else
    if (greeting_recv [revision_pos] == ZMTP_1_0) {
        // reject ZMTP 1.0 connections
        error (protocol_error);
        return false;
    }
    else
    if (greeting_recv [revision_pos] == ZMTP_2_0) {
        if (session->zap_enabled ()) {
           // reject ZMTP 2.0 connections if ZAP is enabled
           error (protocol_error);
           return false;
        }

        encoder = new (std::nothrow) v2_encoder_t ();
        alloc_assert (encoder);

	if (!options.has_decoder_ops) {
		decoder = new (std::nothrow) v2_decoder_t (
			in_batch_size, options.maxmsgsize);
		alloc_assert (decoder);
	} else {
		decoder_ctx = options.dec_ops.create(in_batch_size);
		alloc_assert(decoder_ctx);
	}
    }
    else {
        encoder = new (std::nothrow) v2_encoder_t ();
        alloc_assert (encoder);

	if (!options.has_decoder_ops) {
	    decoder = new (std::nothrow) v2_decoder_t (
			    in_batch_size, options.maxmsgsize);
	    alloc_assert (decoder);
	} else {
	    decoder_ctx = options.dec_ops.create(in_batch_size);
	    alloc_assert(decoder_ctx);
	}

        if (options.mechanism == ZMQ_NULL
        &&  memcmp (greeting_recv + 12, "NULL\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 20) == 0) {
            mechanism = new (std::nothrow)
                null_mechanism_t (session, peer_address, options);
            alloc_assert (mechanism);
        }
        else
        if (options.mechanism == ZMQ_PLAIN
        &&  memcmp (greeting_recv + 12, "PLAIN\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 20) == 0) {
            if (options.as_server)
                mechanism = new (std::nothrow)
                    plain_server_t (session, peer_address, options);
            else
                mechanism = new (std::nothrow)
                    plain_client_t (options);
            alloc_assert (mechanism);
        }
#ifdef ZMQ_HAVE_CURVE
        else
        if (options.mechanism == ZMQ_CURVE
        &&  memcmp (greeting_recv + 12, "CURVE\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 20) == 0) {
            if (options.as_server)
                mechanism = new (std::nothrow)
                    curve_server_t (session, peer_address, options);
            else
                mechanism = new (std::nothrow) curve_client_t (options);
            alloc_assert (mechanism);
        }
#endif
#ifdef HAVE_LIBGSSAPI_KRB5
        else
        if (options.mechanism == ZMQ_GSSAPI
        &&  memcmp (greeting_recv + 12, "GSSAPI\0\0\0\0\0\0\0\0\0\0\0\0\0\0", 20) == 0) {
            if (options.as_server)
                mechanism = new (std::nothrow)
                    gssapi_server_t (session, peer_address, options);
            else
                mechanism = new (std::nothrow) gssapi_client_t (options);
            alloc_assert (mechanism);
        }
#endif
        else {
            error (protocol_error);
            return false;
        }
        next_msg = &stream_engine_t::next_handshake_command;
        process_msg = &stream_engine_t::process_handshake_command;
    }

    // Start polling for output if necessary.
    if (outbuf.size == 0)
        set_pollout (handle);

    // add a dummy message covering the remained of data in the out buffer so
    // freeing the messages on transmit complete works
    outbuf.msgs.resize(outbuf.msgs.size() + 1);
    outbuf.msgs.back().init_size(outbuf.iov.back().iov_len);

    //  Handshaking was successful.
    //  Switch into the normal message flow.
    handshaking = false;

    if (has_handshake_timer) {
        cancel_timer (handshake_timer_id);
        has_handshake_timer = false;
    }

    return true;
}

int zmq::stream_engine_t::identity_msg (msg_t *msg_)
{
    msg_->init_size (options.identity_size);
    if (options.identity_size > 0)
        memcpy (msg_->data (), options.identity, options.identity_size);
    next_msg = &stream_engine_t::pull_msg_from_session;
    return 0;
}

int zmq::stream_engine_t::process_identity_msg (msg_t *msg_)
{
    if (sizeof(id_) - 1 >= msg_->size()) {
        set_id(id_, msg_->data(), msg_->size());
    }

    if (options.recv_identity) {
        msg_->set_flags (msg_t::identity);
        int rc = session->push_msg (msg_);
        errno_assert (rc == 0);
    }
    else {
        msg_->close ();
        msg_->init ();
    }

    if (subscription_required)
        process_msg = &stream_engine_t::write_subscription_msg;
    else
        process_msg = &stream_engine_t::push_msg_to_session;

    return 0;
}

int zmq::stream_engine_t::next_handshake_command (msg_t *msg_)
{
    zmq_assert (mechanism != NULL);

    if (mechanism->status () == mechanism_t::ready) {
        mechanism_ready ();
        return pull_and_encode (msg_);
    }
    else
    if (mechanism->status () == mechanism_t::error) {
        errno = EPROTO;
        return -1;
    }
    else {
        const int rc = mechanism->next_handshake_command (msg_);
        if (rc == 0)
            msg_->set_flags (msg_t::command);
        return rc;
    }
}

int zmq::stream_engine_t::process_handshake_command (msg_t *msg_)
{
    zmq_assert (mechanism != NULL);
    const int rc = mechanism->process_handshake_command (msg_);
    if (rc == 0) {
	    msg_->close();
	    if (!options.has_decoder_ops) {
		    msg_->init();
	    }
        if (mechanism->status () == mechanism_t::ready)
            mechanism_ready ();
	else
        if (mechanism->status () == mechanism_t::error) {
            errno = EPROTO;
            return -1;
        }
        if (output_stopped)
            restart_output ();
    }

    return rc;
}

void zmq::stream_engine_t::zap_msg_available ()
{
    zmq_assert (mechanism != NULL);

    const int rc = mechanism->zap_msg_available ();
    if (rc == -1) {
        error (protocol_error);
        return;
    }
    if (input_stopped)
        restart_input ();
    if (output_stopped)
        restart_output ();
}

void zmq::stream_engine_t::mechanism_ready ()
{
    if (options.recv_identity) {
        msg_t identity;
        mechanism->peer_identity (&identity);
        if (sizeof(id_) - 1 >= identity.size()) {
            set_id(id_, identity.data(), identity.size());
	    if (id_ != zmq_id(0) && options.zmq_callback.accept_callback)
                options.zmq_callback.accept_callback(options.zmq_callback.ctx, id_, peer_address.c_str());
        }

        const int rc = session->push_msg (&identity);
        identity.close();
        if (rc == -1 && errno == EAGAIN) {
            // If the write is failing at this stage with
            // an EAGAIN the pipe must be being shut down,
            // so we can just bail out of the identity set.
            return;
        }
        errno_assert (rc == 0);
        session->flush ();
    }

    next_msg = &stream_engine_t::pull_and_encode;
    process_msg = &stream_engine_t::decode_and_push;

    //  Compile metadata.
    typedef metadata_t::dict_t properties_t;
    properties_t properties;
    properties_t::const_iterator it;

    //  If we have a peer_address, add it to metadata
    if (!peer_address.empty()) {
        properties.insert(std::make_pair("Peer-Address", peer_address));
    }

    //  Add ZAP properties.
    const properties_t& zap_properties = mechanism->get_zap_properties ();
    properties.insert(zap_properties.begin (), zap_properties.end ());

    //  Add ZMTP properties.
    const properties_t& zmtp_properties = mechanism->get_zmtp_properties ();
    properties.insert(zmtp_properties.begin (), zmtp_properties.end ());

    zmq_assert (metadata == NULL);
    if (!properties.empty ())
        metadata = new (std::nothrow) metadata_t (properties);
}

int zmq::stream_engine_t::pull_msg_from_session (msg_t *msg_)
{
    return session->pull_msg (msg_);
}

int zmq::stream_engine_t::push_msg_to_session (msg_t *msg_)
{
    return session->push_msg (msg_);
}

int zmq::stream_engine_t::push_raw_msg_to_session (msg_t *msg_) {
    return push_msg_to_session(msg_);
}

int zmq::stream_engine_t::pull_and_encode (msg_t *msg_)
{
    zmq_assert (mechanism != NULL);

    if (session->pull_msg (msg_) == -1)
        return -1;
    if (mechanism->encode (msg_) == -1)
        return -1;
    return 0;
}

int zmq::stream_engine_t::decode_and_push (msg_t *msg_)
{
    zmq_assert (mechanism != NULL);

    if (mechanism->decode (msg_) == -1)
        return -1;
    if (options.zmq_callback.recv_callback && id_ != zmq_id(0)) {
        if (!(msg_->flags() & msg_t::more)) {
            msg_->set_id(id_);
            options.zmq_callback.recv_callback(options.zmq_callback.ctx, msg_);
            return 0;
        } else {
            // disable callbacks if sender uses multi-frame messages
            options.zmq_callback.recv_callback = NULL;
        }
    }
    if (session->push_msg (msg_) == -1) {
        if (errno == EAGAIN)
            process_msg = &stream_engine_t::push_one_then_decode_and_push;
        return -1;
    }
    return 0;
}

int zmq::stream_engine_t::push_one_then_decode_and_push (msg_t *msg_)
{
    const int rc = session->push_msg (msg_);
    if (rc == 0)
        process_msg = &stream_engine_t::decode_and_push;
    return rc;
}

int zmq::stream_engine_t::write_subscription_msg (msg_t *msg_)
{
    msg_t subscription;

    //  Inject the subscription message, so that also
    //  ZMQ 2.x peers receive published messages.
    subscription.init_size (1);
    *(unsigned char*) subscription.data () = 1;
    int rc = session->push_msg (&subscription);
    if (rc == -1)
       return -1;

    process_msg = &stream_engine_t::push_msg_to_session;
    return push_msg_to_session (msg_);
}

void zmq::stream_engine_t::error (error_reason_t reason)
{
    if (options.raw_sock) {
        //  For raw sockets, send a final 0-length message to the application
        //  so that it knows the peer has been disconnected.
        msg_t terminator;
        terminator.init();
        (this->*process_msg) (&terminator);
        terminator.close();
    }
    zmq_assert (session);
    socket->event_disconnected (endpoint, s);
    session->flush ();
    session->engine_error (reason);
    unplug ();

    if (options.zmq_callback.disconnect_callback) {
        options.zmq_callback.disconnect_callback(options.zmq_callback.ctx, id_);
    }

    delete this;
}

void zmq::stream_engine_t::set_handshake_timer ()
{
    zmq_assert (!has_handshake_timer);

    if (!options.raw_sock && options.handshake_ivl > 0) {
        add_timer (options.handshake_ivl, handshake_timer_id);
        has_handshake_timer = true;
    }
}

void zmq::stream_engine_t::timer_event (int id_)
{
    zmq_assert (id_ == handshake_timer_id);
    has_handshake_timer = false;

    //  handshake timer expired before handshake completed, so engine fails
    error (timeout_error);
}

void zmq::iovec_buf::pull(size_t num_bytes)
{
    if (num_bytes == 0)
        return;

    zmq_assert(num_bytes <= size);
    size -= num_bytes;

    if (next_msg_remain == 0) {
        next_msg = next_msg == (size_t)-1 ? 0 : next_msg + 1;
        assert(next_msg < msgs.size());
        next_msg_remain = msgs[next_msg].size();
    }

    size_t remain = num_bytes;
    while (remain) {
        iovec &iov_curr = iov[curr];
        auto this_bytes = std::min(remain, iov_curr.iov_len);
        next_msg_remain -= this_bytes;
        if (next_msg_remain == 0) {
            // complete message transmitted, can close it so memory can be freed
            msgs[next_msg].close();
            // do not reset the index if out of messages, the iteration will continue
            // when new messages are added in the end of the vector
            if (++next_msg < msgs.size()) {
                next_msg_remain = msgs[next_msg].size();
            } else {
                next_msg_remain = 0;
            }
        }
        if (remain < iov_curr.iov_len) {
            iov_curr.iov_len -= remain;
            iov_curr.iov_base = (char *) iov_curr.iov_base + remain;
            return;
        } else {
            zmq_assert(remain >= iov_curr.iov_len);
            remain -= iov_curr.iov_len;
            ++curr;
        }
    }
}

void zmq::iovec_buf::pull_iov(size_t num_bytes)
{
    if (num_bytes == 0)
        return;

    zmq_assert(num_bytes <= size);
    size -= num_bytes;

    assert(msgs.empty());

    size_t remain = num_bytes;
    while (remain) {
        iovec &iov_curr = iov[curr];
        if (remain < iov_curr.iov_len) {
            iov_curr.iov_len -= remain;
            iov_curr.iov_base = (char *) iov_curr.iov_base + remain;
            return;
        } else {
            zmq_assert(remain >= iov_curr.iov_len);
            remain -= iov_curr.iov_len;
            ++curr;
        }
    }
}

zmq::iovec_buf::iovec_buf() : curr(0), msg_allocated(false), size(0)
{
    msgs.reserve(max_msgs);
}

zmq::iovec_buf::~iovec_buf()
{
    reset();
}

void zmq::iovec_buf::reset()
{
	iov.clear();
	if (next_msg == -1)
		next_msg = 0;
	for (;next_msg < msgs.size(); ++next_msg)
		msgs[next_msg].close();
	msgs.clear();
	curr = 0;
	size = 0;
	next_msg_remain = 0;
	next_msg = (size_t)-1;
	msg_allocated = false;
}
