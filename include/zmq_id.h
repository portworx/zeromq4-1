#ifndef ZMQ_ID_H
#define ZMQ_ID_H

#include <cstdint>
#include <cassert>

enum class zmq_id : uint64_t {};

inline unsigned char zmq_id_len(zmq_id val) {
	return static_cast<uint64_t>(val) & 0xff;
}

inline unsigned long zmq_id_val(zmq_id val) {
	return static_cast<uint64_t>(val) >> 8;
}

inline size_t zmq_id_unpack(zmq_id id, unsigned char *val) {
	for (unsigned i = 0u; i < zmq_id_len(id); ++i) {
		val[i] = (zmq_id_val(id) >> (i * 8)) & 0xff;
	}
	return zmq_id_len(id);
}

inline zmq_id zmq_id_pack(const uint8_t *val, size_t len) {
	assert(len <= 7);
	uint64_t id = len;
	for (auto i = 0u; i < len; ++i) {
		id |= static_cast<uint64_t>(val[i]) << ((i + 1) * 8);
	}
	return zmq_id(id);
}

#endif //ZMQ_ID_H
