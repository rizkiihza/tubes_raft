#ifndef MAIL_HPP
#define MAIL_HPP

#include <log.hpp>
#include <ostream>
#include <vector>

namespace raft {
	struct RequestVoteRPC {
		int term;
		int candidate_id;
		int last_log_index;
		int last_log_term;
	};

	struct RequestVoteReply {
		// id 
		int from_id;
		RequestVoteRPC request;

		// payload
		int term;
		bool vote_granted;
	};

	// please send 1 at a time
	struct AppendEntriesRPC {
		int term;
		int leader_id;
		int prev_log_index;
		int prev_log_term;
		std::vector<Log> logs;
		int leader_commit_index;
	};

	struct AppendEntriesReply {
		// id
		int from_id;
		AppendEntriesRPC request;

		// payload
		int term;
		bool success;
	};


	enum class MailType {
		RVRPC,
		RVREPLY,
		AERPC,
		AEREPLY
	};

	struct Mail {
		int to_id;
		int from_id;
		MailType type;

		RequestVoteRPC rvrpc;
		RequestVoteReply rvreply;
		AppendEntriesRPC aerpc;
		AppendEntriesReply aereply;

		friend std::ostream & operator<<(std::ostream &os, const Mail& m);

	};

	class Mailer {
	public:
		virtual void Send(int sender_id, int to, RequestVoteRPC payload) = 0;
		virtual void Send(int sender_id, int to, AppendEntriesRPC payload) = 0;
		virtual void Send(int sender_id, int to, RequestVoteReply payload) = 0;
		virtual void Send(int sender_id, int to, AppendEntriesReply payload) = 0;
	};
}

#endif