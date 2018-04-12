#ifndef SERVER_HPP
#define SERVER_HPP

#include <vector>
#include <ostream>
#include <log.hpp>
#include <mail.hpp>
#include <sender.hpp>
#include <algorithm>

namespace raft {
	enum class State {
		LEADER,
		FOLLOWER,
		CANDIDATE
	};

	class Server {
	public:
		Server();
		Server(int cluster_size, int si, Sender sender);
		void Crash();
		void ApplyLog();
		void Timestep();
		void SetTimeout(int);

		void Receive(AppendEntriesReply);
		void Receive(RequestVoteReply);
		void Receive(AppendEntriesRPC);
		void Receive(RequestVoteRPC);
		void Receive(Log);

		//fungsi helper tambahan
		void sendAppendEntriesReply(AppendEntriesRPC rpc, bool success);
		void sendRequestVoteReply(RequestVoteRPC rpc, bool voted);

		friend std::ostream & operator<<(std::ostream &os, const Server& s);
		std::string GetLeaderStateString();
	private:
		// persistent state
		int current_term;
		int voted_for;
		std::vector<Log> logs;
		int cluster_size;
		int server_index;
		int data;

		// volatile state
		int commit_index;
		int last_applied;

		// destroyed when crash
		State state;
		int time_to_timeout;

		// volatile state leader
		std::vector<int> next_index;
		std::vector<int> match_index;
		Sender sender;

		// volatile state candidate
		std::vector<bool> vote_granted;
		int vote_count;
	};
}

#endif
