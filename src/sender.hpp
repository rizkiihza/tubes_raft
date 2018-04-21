#ifndef SENDER_HPP
#define SENDER_HPP
#include <vector>
#include <mail.hpp>
#include <log.hpp>

namespace raft {
	class Sender {
	public:
		Sender();
		Sender(Mailer* s, int id);

		void Send(int to, RequestVoteRPC payload);
		void Send(int to, AppendEntriesRPC payload);
		void Send(int to, RequestVoteReply payload);
		void Send(int to, AppendEntriesReply payload);
	private:
		int sender_id;
		Mailer* simulation;
	};
}
#endif