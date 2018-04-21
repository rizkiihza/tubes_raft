#ifndef RAFT_SIMULATION_HPP
#define RAFT_SIMULATION_HPP

#include <mail.hpp>
#include <sender.hpp>
#include <server.hpp>
#include <log.hpp>
#include <vector>
#include <map>
#include <vector>
#include <iostream>

namespace raft {
	class RaftSimulation : public Mailer {
	public:
		RaftSimulation(int cluster_size);
		void Send(int sender_id, int to, RequestVoteRPC payload);
		void Send(int sender_id, int to, AppendEntriesRPC payload);
		void Send(int sender_id, int to, RequestVoteReply payload);
		void Send(int sender_id, int to, AppendEntriesReply payload);
		void Connect(int a, int b);
		void Disconnect(int a, int b);
		void Crash(int server_index);
		void Start(int server_index);
		void Timestep();
		void SetTimeout(int server_index, int ttt);
		
		void FlushOutbox();
		void PerfectShuffleOutbox();
		void StoreOutbox(std::string key);
		void DeleteOutbox();
		void RetrieveOutbox(std::string key);
		void ClientSendRequest(int server, Log log);

		void PrintServerState(int);
		void PrintServerLeaderState(int);
		void PrintOutbox();
		void PrintServerRole(int);
		void PrintServerLog(int);
		void PrintServerData(int);
		void PrintServerCommitIndex(int);
	private:
		int cluster_size;
		std::vector<Server> servers;
		std::map< std::pair<int,int>, bool > connection_matrix;
		std::vector< bool > server_online;

		std::vector<Mail> mailbox;
		std::map< std::string, std::vector<Mail> > stored_mailbox;
		int mailbox_index;
	};
}

#endif