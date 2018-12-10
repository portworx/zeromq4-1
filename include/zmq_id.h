#ifndef ZMQ_ID_H
#define ZMQ_ID_H

typedef unsigned long zmq_id;

inline unsigned char zmq_id_len(zmq_id val) {
	return val & 0xff;
}

inline unsigned long zmq_id_val(zmq_id val) {
	return val >> 8;
}

inline size_t zmq_id_unpack(zmq_id id, unsigned char *val) {
	for (unsigned i = 0u; i < zmq_id_len(id); ++i) {
		val[i] = (zmq_id_val(id) >> (i * 8)) & 0xff;
	}
	return zmq_id_len(id);
}

#endif //ZMQ_ID_H
