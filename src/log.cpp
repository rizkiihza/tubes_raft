#include <log.hpp>

namespace raft {
	Log::Log(){
		operation = Operation::NOTHING;
		payload = 0;
		term = 0;

	}
}