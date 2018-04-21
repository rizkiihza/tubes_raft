#ifndef RAFT_LOG_HPP
#define RAFT_LOG_HPP

namespace raft {
	enum class Operation {
		MULTIPLY,
		ADD,
		SUBTRACT,
		REPLACE,
		NOTHING
	};

	struct Log {
		Log();
		Operation operation;
		int payload;
		int term;
	};
}
#endif